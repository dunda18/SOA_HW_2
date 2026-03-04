#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
SPEC = ROOT / "openapi" / "openapi.yaml"
OUT = ROOT / "app" / "generated"

def run_codegen() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    try:
        from fastapi_code_generator.__main__ import generate_code

        generate_code(
            input_name=str(SPEC),
            input_text=SPEC.read_text(encoding="utf-8"),
            encoding="utf-8",
            output_dir=OUT,
            template_dir=None,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"OpenAPI code generation failed: {exc}") from exc


def patch_models() -> None:
    models_path = OUT / "models.py"
    text = models_path.read_text(encoding="utf-8")
    text = text.replace("promo_code_id: UUID", "promo_code_id: Optional[UUID] = None")
    text = text.replace("constr(regex=", "constr(pattern=")
    models_path.write_text(text, encoding="utf-8")


def patch_main() -> None:
    main_path = OUT / "main.py"
    text = main_path.read_text(encoding="utf-8")
    tree = ast.parse(text)

    pass_replacements: dict[int, str] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            args = [arg.arg for arg in node.args.args]
            for statement in node.body:
                if isinstance(statement, ast.Pass):
                    call_args = ", ".join(args)
                    pass_replacements[statement.lineno] = (
                        f"    return handlers.{node.name}({call_args})"
                    )

    lines = text.splitlines()
    out_lines: list[str] = []
    for idx, line in enumerate(lines, start=1):
        replacement = pass_replacements.get(idx)
        if replacement is not None:
            out_lines.append(replacement)
            continue
        out_lines.append(line)

    patched = "\n".join(out_lines) + "\n"

    if "from app import handlers" not in patched:
        marker = "from .models import ("
        marker_idx = patched.find(marker)
        if marker_idx == -1:
            raise RuntimeError("Failed to patch generated main.py: import marker not found")

        close_idx = patched.find(")\n\n", marker_idx)
        if close_idx == -1:
            raise RuntimeError("Failed to patch generated main.py: models import closing not found")

        patched = patched[: close_idx + 3] + "from app import handlers\n\n" + patched[close_idx + 3 :]

    main_path.write_text(patched, encoding="utf-8")


def ensure_init() -> None:
    init_path = OUT / "__init__.py"
    if not init_path.exists():
        init_path.write_text("", encoding="utf-8")


def main() -> None:
    run_codegen()
    patch_models()
    patch_main()
    ensure_init()
    print(f"Generated FastAPI code from {SPEC} -> {OUT}")


if __name__ == "__main__":
    main()
