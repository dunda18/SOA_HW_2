from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable
from uuid import UUID

from fastapi.responses import JSONResponse
from psycopg.errors import UniqueViolation

from app.config import load_settings
from app.context import CurrentUser, get_current_user
from app.db import get_connection
from app.errors import AppError
from app.generated.models import (
    LoginRequest,
    OrderCreateRequest,
    OrderItemResponse,
    OrderResponse,
    OrderStatus,
    OrderUpdateRequest,
    ProductCreate,
    ProductPageResponse,
    ProductResponse,
    ProductStatus,
    ProductUpdate,
    PromoCodeCreateRequest,
    PromoCodeResponse,
    RefreshRequest,
    RegisterRequest,
    Role,
    TokenPairResponse,
    UserResponse,
)
from app.security import (
    hash_password,
    issue_token_pair,
    validate_and_rotate_refresh_token,
    verify_password,
)

SETTINGS = load_settings()
MONEY_QUANT = Decimal("0.01")


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _json_201(model) -> JSONResponse:
    return JSONResponse(status_code=201, content=model.model_dump(mode="json"))


def _require_user() -> CurrentUser:
    user = get_current_user()
    if user is None:
        raise AppError(401, "TOKEN_INVALID", "Access token is invalid")
    return user


def _require_any_role(roles: Iterable[Role]) -> CurrentUser:
    user = _require_user()
    allowed = set(roles)
    if user.role not in allowed:
        raise AppError(403, "ACCESS_DENIED", "Access denied")
    return user


def _to_user_response(row: dict[str, Any]) -> UserResponse:
    return UserResponse(
        id=row["id"],
        email=row["email"],
        role=Role(row["role"]),
        created_at=row["created_at"],
    )


def _to_token_response(user_row: dict[str, Any], tokens: dict[str, Any]) -> TokenPairResponse:
    return TokenPairResponse(
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        token_type="Bearer",
        expires_in=tokens["expires_in"],
        user=_to_user_response(user_row),
    )


def _to_product_response(row: dict[str, Any]) -> ProductResponse:
    return ProductResponse(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        price=row["price"],
        stock=row["stock"],
        category=row["category"],
        status=ProductStatus(row["status"]),
        seller_id=row["seller_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _to_promo_response(row: dict[str, Any]) -> PromoCodeResponse:
    return PromoCodeResponse(
        id=row["id"],
        code=row["code"],
        discount_type=row["discount_type"],
        discount_value=row["discount_value"],
        min_order_amount=row["min_order_amount"],
        max_uses=row["max_uses"],
        current_uses=row["current_uses"],
        valid_from=row["valid_from"],
        valid_until=row["valid_until"],
        active=row["active"],
    )


def _to_order_item_response(row: dict[str, Any]) -> OrderItemResponse:
    return OrderItemResponse(
        id=row["id"],
        product_id=row["product_id"],
        quantity=row["quantity"],
        price_at_order=row["price_at_order"],
    )


def _to_order_response(order_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> OrderResponse:
    return OrderResponse(
        id=order_row["id"],
        user_id=order_row["user_id"],
        status=OrderStatus(order_row["status"]),
        promo_code_id=order_row["promo_code_id"],
        total_amount=order_row["total_amount"],
        discount_amount=order_row["discount_amount"],
        created_at=order_row["created_at"],
        updated_at=order_row["updated_at"],
        items=[_to_order_item_response(item_row) for item_row in item_rows],
    )


def _aggregate_items(items) -> dict[UUID, int]:
    aggregated: dict[UUID, int] = {}
    for item in items:
        current = aggregated.get(item.product_id, 0)
        aggregated[item.product_id] = current + int(item.quantity)
    return aggregated


def _check_order_rate_limit(connection, user_id: UUID, operation_type: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT created_at
            FROM user_operations
            WHERE user_id = %s AND operation_type = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id, operation_type),
        )
        row = cursor.fetchone()

    if row is None:
        return

    elapsed = _now_utc() - row["created_at"]
    if elapsed < timedelta(minutes=SETTINGS.order_rate_limit_minutes):
        raise AppError(
            429,
            "ORDER_LIMIT_EXCEEDED",
            "Order operation limit exceeded",
            details={"retry_after_minutes": SETTINGS.order_rate_limit_minutes},
        )


def _insert_user_operation(connection, user_id: UUID, operation_type: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO user_operations (user_id, operation_type)
            VALUES (%s, %s)
            """,
            (user_id, operation_type),
        )


def _fetch_order(connection, order_id: UUID, for_update: bool = False) -> dict[str, Any] | None:
    sql = """
        SELECT id, user_id, status, promo_code_id, total_amount, discount_amount, created_at, updated_at
        FROM orders
        WHERE id = %s
    """
    if for_update:
        sql += " FOR UPDATE"

    with connection.cursor() as cursor:
        cursor.execute(sql, (order_id,))
        return cursor.fetchone()


def _fetch_order_items(connection, order_id: UUID, for_update: bool = False) -> list[dict[str, Any]]:
    sql = """
        SELECT id, order_id, product_id, quantity, price_at_order
        FROM order_items
        WHERE order_id = %s
        ORDER BY id
    """
    if for_update:
        sql += " FOR UPDATE"

    with connection.cursor() as cursor:
        cursor.execute(sql, (order_id,))
        rows = cursor.fetchall()

    return rows


def _ensure_order_access(order_user_id: UUID, user: CurrentUser) -> None:
    if user.role == Role.ADMIN:
        return
    if user.role == Role.SELLER:
        raise AppError(403, "ACCESS_DENIED", "Access denied")
    if user.user_id != order_user_id:
        raise AppError(403, "ORDER_OWNERSHIP_VIOLATION", "Order belongs to another user")


def _build_product_map(connection, product_ids: list[UUID]) -> dict[UUID, dict[str, Any]]:
    if not product_ids:
        return {}

    placeholders = ",".join(["%s"] * len(product_ids))
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT id, name, price, stock, status
            FROM products
            WHERE id IN ({placeholders})
            FOR UPDATE
            """,
            tuple(product_ids),
        )
        rows = cursor.fetchall()

    return {row["id"]: row for row in rows}


def _validate_products_for_order(products: dict[UUID, dict[str, Any]], requested: dict[UUID, int]) -> None:
    missing = [product_id for product_id in requested.keys() if product_id not in products]
    if missing:
        raise AppError(
            404,
            "PRODUCT_NOT_FOUND",
            "Product not found",
            details={"product_id": str(missing[0])},
        )

    inactive = [
        product_id
        for product_id, product in products.items()
        if product["status"] != ProductStatus.ACTIVE.value
    ]
    if inactive:
        raise AppError(
            409,
            "PRODUCT_INACTIVE",
            "Product is inactive",
            details={"product_id": str(inactive[0])},
        )


def _validate_stock(products: dict[UUID, dict[str, Any]], requested: dict[UUID, int]) -> None:
    insufficient = []
    for product_id, quantity in requested.items():
        available = int(products[product_id]["stock"])
        if available < quantity:
            insufficient.append(
                {
                    "product_id": str(product_id),
                    "requested": quantity,
                    "available": available,
                }
            )

    if insufficient:
        raise AppError(
            409,
            "INSUFFICIENT_STOCK",
            "Insufficient stock",
            details={"items": insufficient},
        )


def _reserve_stock(connection, requested: dict[UUID, int]) -> None:
    with connection.cursor() as cursor:
        for product_id, quantity in requested.items():
            cursor.execute(
                """
                UPDATE products
                SET stock = stock - %s
                WHERE id = %s
                """,
                (quantity, product_id),
            )


def _restore_stock_from_items(connection, item_rows: list[dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        for item_row in item_rows:
            cursor.execute(
                """
                UPDATE products
                SET stock = stock + %s
                WHERE id = %s
                """,
                (item_row["quantity"], item_row["product_id"]),
            )


def _calc_subtotal(products: dict[UUID, dict[str, Any]], requested: dict[UUID, int]) -> Decimal:
    subtotal = Decimal("0.00")
    for product_id, quantity in requested.items():
        price = Decimal(products[product_id]["price"])
        subtotal += price * quantity
    return _money(subtotal)


def _fetch_promo_by_code(connection, code: str) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, code, discount_type, discount_value, min_order_amount, max_uses,
                   current_uses, valid_from, valid_until, active
            FROM promo_codes
            WHERE code = %s
            FOR UPDATE
            """,
            (code,),
        )
        return cursor.fetchone()


def _fetch_promo_by_id(connection, promo_id: UUID) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, code, discount_type, discount_value, min_order_amount, max_uses,
                   current_uses, valid_from, valid_until, active
            FROM promo_codes
            WHERE id = %s
            FOR UPDATE
            """,
            (promo_id,),
        )
        return cursor.fetchone()


def _validate_promo(promo: dict[str, Any], strict_max_uses: bool) -> None:
    now = _now_utc()
    if not promo["active"]:
        raise AppError(422, "PROMO_CODE_INVALID", "Promo code is invalid")

    current_uses = int(promo["current_uses"])
    max_uses = int(promo["max_uses"])
    if strict_max_uses:
        if current_uses >= max_uses:
            raise AppError(422, "PROMO_CODE_INVALID", "Promo code is invalid")
    else:
        if current_uses > max_uses:
            raise AppError(422, "PROMO_CODE_INVALID", "Promo code is invalid")

    if now < promo["valid_from"] or now > promo["valid_until"]:
        raise AppError(422, "PROMO_CODE_INVALID", "Promo code is invalid")


def _calculate_discount(subtotal: Decimal, promo: dict[str, Any]) -> Decimal:
    discount_type = promo["discount_type"]
    discount_value = Decimal(promo["discount_value"])

    if discount_type == "PERCENTAGE":
        discount = _money(subtotal * discount_value / Decimal("100"))
        max_discount = _money(subtotal * Decimal("0.70"))
        if discount > max_discount:
            discount = max_discount
        return discount

    return _money(min(discount_value, subtotal))


def _insert_order_items(
    connection,
    order_id: UUID,
    requested: dict[UUID, int],
    products: dict[UUID, dict[str, Any]],
) -> list[dict[str, Any]]:
    inserted: list[dict[str, Any]] = []
    with connection.cursor() as cursor:
        for product_id, quantity in requested.items():
            cursor.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, price_at_order)
                VALUES (%s, %s, %s, %s)
                RETURNING id, order_id, product_id, quantity, price_at_order
                """,
                (order_id, product_id, quantity, products[product_id]["price"]),
            )
            inserted.append(cursor.fetchone())
    return inserted


def register(body: RegisterRequest):
    role = body.role or Role.USER

    with get_connection() as connection:
        with connection.transaction():
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO users (email, password_hash, role)
                        VALUES (%s, %s, %s)
                        RETURNING id, email, role, created_at
                        """,
                        (body.email, hash_password(body.password), role.value),
                    )
                    user_row = cursor.fetchone()
            except UniqueViolation as exc:
                raise AppError(409, "VALIDATION_ERROR", "Email is already registered") from exc

            user_ctx = CurrentUser(
                user_id=user_row["id"],
                role=Role(user_row["role"]),
                email=user_row["email"],
            )
            tokens = issue_token_pair(connection, user_ctx, SETTINGS)

    return _json_201(_to_token_response(user_row, tokens))


def login(body: LoginRequest):
    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, email, password_hash, role, created_at
                    FROM users
                    WHERE email = %s
                    """,
                    (body.email,),
                )
                user_row = cursor.fetchone()

            if user_row is None or not verify_password(body.password, user_row["password_hash"]):
                raise AppError(401, "TOKEN_INVALID", "Invalid email or password")

            user_ctx = CurrentUser(
                user_id=user_row["id"],
                role=Role(user_row["role"]),
                email=user_row["email"],
            )
            tokens = issue_token_pair(connection, user_ctx, SETTINGS)

    return _to_token_response(user_row, tokens)


def refresh_token(body: RefreshRequest):
    with get_connection() as connection:
        with connection.transaction():
            token_user = validate_and_rotate_refresh_token(connection, body.refresh_token, SETTINGS)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, email, role, created_at
                    FROM users
                    WHERE id = %s
                    """,
                    (token_user.user_id,),
                )
                user_row = cursor.fetchone()

            if user_row is None:
                raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid")

            user_ctx = CurrentUser(
                user_id=user_row["id"],
                role=Role(user_row["role"]),
                email=user_row["email"],
            )
            tokens = issue_token_pair(connection, user_ctx, SETTINGS)

    return _to_token_response(user_row, tokens)


def list_products(page=0, size=20, status=None, category=None):
    _require_any_role({Role.USER, Role.SELLER, Role.ADMIN})

    page = 0 if page is None else int(page)
    size = 20 if size is None else int(size)
    filters = []
    params: list[Any] = []

    if status is not None:
        filters.append("status = %s")
        params.append(status.value)
    if category is not None:
        filters.append("category = %s")
        params.append(category)

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) AS total FROM products{where_clause}", tuple(params))
            total = int(cursor.fetchone()["total"])

            cursor.execute(
                f"""
                SELECT id, name, description, price, stock, category, status, seller_id, created_at, updated_at
                FROM products
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [size, page * size]),
            )
            rows = cursor.fetchall()

    return ProductPageResponse(
        content=[_to_product_response(row) for row in rows],
        totalElements=total,
        page=page,
        size=size,
    )


def create_product(body: ProductCreate):
    user = _require_any_role({Role.SELLER, Role.ADMIN})

    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO products (name, description, price, stock, category, status, seller_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, name, description, price, stock, category, status, seller_id, created_at, updated_at
                    """,
                    (
                        body.name,
                        body.description,
                        body.price,
                        body.stock,
                        body.category,
                        body.status.value,
                        user.user_id,
                    ),
                )
                row = cursor.fetchone()

    return _json_201(_to_product_response(row))


def get_product_by_id(id: UUID):
    _require_any_role({Role.USER, Role.SELLER, Role.ADMIN})

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, description, price, stock, category, status, seller_id, created_at, updated_at
                FROM products
                WHERE id = %s
                """,
                (id,),
            )
            row = cursor.fetchone()

    if row is None:
        raise AppError(404, "PRODUCT_NOT_FOUND", "Product not found")

    return _to_product_response(row)


def update_product(id: UUID, body: ProductUpdate):
    user = _require_any_role({Role.SELLER, Role.ADMIN})

    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, seller_id
                    FROM products
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (id,),
                )
                existing = cursor.fetchone()
                if existing is None:
                    raise AppError(404, "PRODUCT_NOT_FOUND", "Product not found")

                if user.role == Role.SELLER and existing["seller_id"] != user.user_id:
                    raise AppError(403, "ACCESS_DENIED", "Access denied")

                cursor.execute(
                    """
                    UPDATE products
                    SET name = %s,
                        description = %s,
                        price = %s,
                        stock = %s,
                        category = %s,
                        status = %s
                    WHERE id = %s
                    RETURNING id, name, description, price, stock, category, status, seller_id, created_at, updated_at
                    """,
                    (
                        body.name,
                        body.description,
                        body.price,
                        body.stock,
                        body.category,
                        body.status.value,
                        id,
                    ),
                )
                updated = cursor.fetchone()

    return _to_product_response(updated)


def delete_product(id: UUID):
    user = _require_any_role({Role.SELLER, Role.ADMIN})

    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, seller_id
                    FROM products
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (id,),
                )
                existing = cursor.fetchone()
                if existing is None:
                    raise AppError(404, "PRODUCT_NOT_FOUND", "Product not found")

                if user.role == Role.SELLER and existing["seller_id"] != user.user_id:
                    raise AppError(403, "ACCESS_DENIED", "Access denied")

                cursor.execute(
                    """
                    UPDATE products
                    SET status = %s
                    WHERE id = %s
                    RETURNING id, name, description, price, stock, category, status, seller_id, created_at, updated_at
                    """,
                    (ProductStatus.ARCHIVED.value, id),
                )
                archived = cursor.fetchone()

    return _to_product_response(archived)


def create_promo_code(body: PromoCodeCreateRequest):
    _require_any_role({Role.SELLER, Role.ADMIN})

    if body.valid_until < body.valid_from:
        raise AppError(
            400,
            "VALIDATION_ERROR",
            "Validation failed",
            details={
                "errors": [
                    {
                        "field": "valid_until",
                        "message": "must be greater or equal to valid_from",
                    }
                ]
            },
        )

    with get_connection() as connection:
        with connection.transaction():
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO promo_codes (
                            code, discount_type, discount_value, min_order_amount,
                            max_uses, valid_from, valid_until, active
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id, code, discount_type, discount_value, min_order_amount,
                                  max_uses, current_uses, valid_from, valid_until, active
                        """,
                        (
                            body.code,
                            body.discount_type.value,
                            body.discount_value,
                            body.min_order_amount,
                            body.max_uses,
                            body.valid_from,
                            body.valid_until,
                            True if body.active is None else bool(body.active),
                        ),
                    )
                    row = cursor.fetchone()
            except UniqueViolation as exc:
                raise AppError(422, "PROMO_CODE_INVALID", "Promo code already exists") from exc

    return _json_201(_to_promo_response(row))


def create_order(body: OrderCreateRequest):
    user = _require_user()
    if user.role == Role.SELLER:
        raise AppError(403, "ACCESS_DENIED", "Access denied")

    requested = _aggregate_items(body.items)
    product_ids = list(requested.keys())

    with get_connection() as connection:
        with connection.transaction():
            _check_order_rate_limit(connection, user.user_id, "CREATE_ORDER")

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id
                    FROM orders
                    WHERE user_id = %s AND status IN ('CREATED', 'PAYMENT_PENDING')
                    LIMIT 1
                    """,
                    (user.user_id,),
                )
                has_active = cursor.fetchone() is not None

            if has_active:
                raise AppError(409, "ORDER_HAS_ACTIVE", "User already has an active order")

            products = _build_product_map(connection, product_ids)
            _validate_products_for_order(products, requested)
            _validate_stock(products, requested)
            _reserve_stock(connection, requested)

            subtotal = _calc_subtotal(products, requested)
            discount = Decimal("0.00")
            total = subtotal
            promo_id: UUID | None = None

            if body.promo_code is not None:
                promo = _fetch_promo_by_code(connection, body.promo_code)
                if promo is None:
                    raise AppError(422, "PROMO_CODE_INVALID", "Promo code is invalid")

                _validate_promo(promo, strict_max_uses=True)
                if subtotal < promo["min_order_amount"]:
                    raise AppError(422, "PROMO_CODE_MIN_AMOUNT", "Order amount is below promo minimum")

                discount = _calculate_discount(subtotal, promo)
                total = _money(subtotal - discount)
                promo_id = promo["id"]

                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE promo_codes
                        SET current_uses = current_uses + 1
                        WHERE id = %s
                        """,
                        (promo_id,),
                    )

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO orders (user_id, status, promo_code_id, total_amount, discount_amount)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id, user_id, status, promo_code_id, total_amount, discount_amount, created_at, updated_at
                    """,
                    (user.user_id, OrderStatus.CREATED.value, promo_id, total, discount),
                )
                order_row = cursor.fetchone()

            item_rows = _insert_order_items(connection, order_row["id"], requested, products)
            _insert_user_operation(connection, user.user_id, "CREATE_ORDER")

    return _json_201(_to_order_response(order_row, item_rows))


def get_order_by_id(id: UUID):
    user = _require_user()

    with get_connection() as connection:
        order_row = _fetch_order(connection, id)
        if order_row is None:
            raise AppError(404, "ORDER_NOT_FOUND", "Order not found")

        _ensure_order_access(order_row["user_id"], user)
        item_rows = _fetch_order_items(connection, id)

    return _to_order_response(order_row, item_rows)


def update_order(id: UUID, body: OrderUpdateRequest):
    user = _require_user()
    requested = _aggregate_items(body.items)
    product_ids = list(requested.keys())

    with get_connection() as connection:
        with connection.transaction():
            order_row = _fetch_order(connection, id, for_update=True)
            if order_row is None:
                raise AppError(404, "ORDER_NOT_FOUND", "Order not found")

            _ensure_order_access(order_row["user_id"], user)
            if order_row["status"] != OrderStatus.CREATED.value:
                raise AppError(409, "INVALID_STATE_TRANSITION", "Invalid order state transition")

            _check_order_rate_limit(connection, user.user_id, "UPDATE_ORDER")

            old_item_rows = _fetch_order_items(connection, id, for_update=True)
            _restore_stock_from_items(connection, old_item_rows)

            products = _build_product_map(connection, product_ids)
            _validate_products_for_order(products, requested)
            _validate_stock(products, requested)
            _reserve_stock(connection, requested)

            subtotal = _calc_subtotal(products, requested)
            discount = Decimal("0.00")
            total = subtotal
            promo_id = order_row["promo_code_id"]

            if promo_id is not None:
                promo = _fetch_promo_by_id(connection, promo_id)
                if promo is None:
                    raise AppError(422, "PROMO_CODE_INVALID", "Promo code is invalid")

                _validate_promo(promo, strict_max_uses=False)

                if subtotal < promo["min_order_amount"]:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE promo_codes
                            SET current_uses = GREATEST(current_uses - 1, 0)
                            WHERE id = %s
                            """,
                            (promo_id,),
                        )
                    promo_id = None
                    discount = Decimal("0.00")
                    total = subtotal
                else:
                    discount = _calculate_discount(subtotal, promo)
                    total = _money(subtotal - discount)

            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM order_items WHERE order_id = %s", (id,))

            item_rows = _insert_order_items(connection, id, requested, products)

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE orders
                    SET promo_code_id = %s,
                        total_amount = %s,
                        discount_amount = %s
                    WHERE id = %s
                    RETURNING id, user_id, status, promo_code_id, total_amount, discount_amount, created_at, updated_at
                    """,
                    (promo_id, total, discount, id),
                )
                updated_order = cursor.fetchone()

            _insert_user_operation(connection, user.user_id, "UPDATE_ORDER")

    return _to_order_response(updated_order, item_rows)


def cancel_order(id: UUID):
    user = _require_user()

    with get_connection() as connection:
        with connection.transaction():
            order_row = _fetch_order(connection, id, for_update=True)
            if order_row is None:
                raise AppError(404, "ORDER_NOT_FOUND", "Order not found")

            _ensure_order_access(order_row["user_id"], user)
            if order_row["status"] not in {
                OrderStatus.CREATED.value,
                OrderStatus.PAYMENT_PENDING.value,
            }:
                raise AppError(409, "INVALID_STATE_TRANSITION", "Invalid order state transition")

            item_rows = _fetch_order_items(connection, id, for_update=True)
            _restore_stock_from_items(connection, item_rows)

            if order_row["promo_code_id"] is not None:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE promo_codes
                        SET current_uses = GREATEST(current_uses - 1, 0)
                        WHERE id = %s
                        """,
                        (order_row["promo_code_id"],),
                    )

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE orders
                    SET status = %s
                    WHERE id = %s
                    RETURNING id, user_id, status, promo_code_id, total_amount, discount_amount, created_at, updated_at
                    """,
                    (OrderStatus.CANCELED.value, id),
                )
                canceled_order = cursor.fetchone()

    return _to_order_response(canceled_order, item_rows)
