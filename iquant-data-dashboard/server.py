from __future__ import annotations

import argparse
import csv
import io
import json
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from scanner import IQuantScanner


ROOT = Path(__file__).resolve().parent
STATIC_ROOT = ROOT / "static"


class DashboardState:
    def __init__(self) -> None:
        self.scanner = IQuantScanner()
        self.cache: dict[str, object] = {}

    def scan(self, universe: str | None, data_type: str | None, start_date: str | None, end_date: str | None) -> dict:
        key = f"{universe or '*'}:{data_type or '*'}:{start_date or ''}:{end_date or ''}"
        if key not in self.cache:
            self.cache[key] = self.scanner.scan(
                universe_key=universe,
                data_type_key=data_type,
                start_date=start_date,
                end_date=end_date,
            )
        return self.cache[key]  # type: ignore[return-value]

    def refresh(self, universe: str | None, data_type: str | None, start_date: str | None, end_date: str | None) -> dict:
        key = f"{universe or '*'}:{data_type or '*'}:{start_date or ''}:{end_date or ''}"
        self.cache[key] = self.scanner.scan(
            universe_key=universe,
            data_type_key=data_type,
            start_date=start_date,
            end_date=end_date,
        )
        return self.cache[key]  # type: ignore[return-value]


STATE = DashboardState()


class Handler(BaseHTTPRequestHandler):
    server_version = "IQuantDashboard/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_file(STATIC_ROOT / "index.html")
            return
        if parsed.path == "/api/config":
            self._send_json(STATE.scanner.config)
            return
        if parsed.path == "/api/scan":
            query = parse_qs(parsed.query)
            universe = query.get("universe", [""])[0] or query.get("pool", [""])[0] or None
            data_type = query.get("data_type", [""])[0] or query.get("period", [""])[0] or None
            start_date = query.get("start_date", [""])[0] or None
            end_date = query.get("end_date", [""])[0] or None
            refresh = query.get("refresh", ["0"])[0] == "1"
            payload = (
                STATE.refresh(universe, data_type, start_date, end_date)
                if refresh
                else STATE.scan(universe, data_type, start_date, end_date)
            )
            self._send_json(payload)
            return
        if parsed.path == "/api/export.csv":
            query = parse_qs(parsed.query)
            universe = query.get("universe", [""])[0] or None
            data_type = query.get("data_type", [""])[0] or None
            start_date = query.get("start_date", [""])[0] or None
            end_date = query.get("end_date", [""])[0] or None
            payload = STATE.scan(universe, data_type, start_date, end_date)
            self._send_csv(payload)
            return

        static_path = (STATIC_ROOT / parsed.path.lstrip("/")).resolve()
        if STATIC_ROOT in static_path.parents and static_path.exists():
            self._send_file(static_path)
            return
        self.send_error(404)

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[dashboard] {self.address_string()} - {fmt % args}")

    def _send_json(self, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_csv(self, payload: dict) -> None:
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "universe_name",
                "data_type_name",
                "code",
                "status",
                "first_date",
                "latest_date",
                "coverage",
                "missing_count",
                "note",
            ],
        )
        writer.writeheader()
        for row in payload.get("rows", []):
            writer.writerow({key: row.get(key, "") for key in writer.fieldnames})
        body = output.getvalue().encode("utf-8-sig")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", "attachment; filename=iquant-data-health.csv")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path) -> None:
        body = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        if path.suffix in {".html", ".css", ".js"}:
            content_type += "; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local iQuant data dashboard.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"iQuant 数据大盘已启动: {url}")
    print("按 Ctrl+C 退出")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
