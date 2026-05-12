"""Emit the JSON Schema for `PipelineConfig`.

Usage:
    python -m pipelines.shared.schema                  # stdout
    python -m pipelines.shared.schema -o schema.json   # to file

The generated schema is what every pipeline YAML must validate against.
Editors can wire it up via `yaml.schemas` (VSCode) or the
`# yaml-language-server: $schema=...` header for inline validation.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipelines.shared.schema.pipeline import PipelineConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--output", type=Path, help="Write to file instead of stdout.")
    parser.add_argument("--indent", type=int, default=2)
    args = parser.parse_args(argv)

    schema = PipelineConfig.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://datafabrik.dev/schemas/pipeline_config.schema.json"

    rendered = json.dumps(schema, indent=args.indent, sort_keys=False) + "\n"

    if args.output:
        args.output.write_text(rendered)
        print(f"Wrote schema to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
