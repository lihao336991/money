from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def main() -> None:
    out_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("xtquant-probe.json")
    result = {
        "executable": sys.executable,
        "version": sys.version,
        "xtquant": importlib.util.find_spec("xtquant") is not None,
        "xtdata": False,
        "xtdata_file": None,
        "error": None,
    }
    try:
        from xtquant import xtdata  # type: ignore

        result["xtdata"] = True
        result["xtdata_file"] = getattr(xtdata, "__file__", None)
    except Exception as exc:
        result["error"] = repr(exc)

    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
