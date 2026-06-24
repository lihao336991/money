from __future__ import annotations

import datetime as dt
import json
import random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path(__file__).with_name("dashboard_config.json")


@dataclass
class InstrumentCompleteness:
    universe_key: str
    universe_name: str
    data_type: str
    data_type_name: str
    code: str
    status: str
    first_date: str | None
    latest_date: str | None
    expected_count: int
    actual_count: int
    missing_count: int
    coverage: float
    missing_dates: list[str]
    note: str = ""


class IQuantScanner:
    def __init__(self, config_path: Path | str = DEFAULT_CONFIG_PATH):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.xtdata = self._load_xtdata()
        self.source = "xtquant" if self.xtdata else "demo"

    def scan(
        self,
        universe_key: str | None = None,
        data_type_key: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        now = dt.datetime.now()
        universe = self._get_config_item("universes", universe_key or self.config.get("default_universe"))
        data_type = self._get_config_item("data_types", data_type_key or self.config.get("default_data_type"))
        start, end = self._resolve_date_range(start_date, end_date)

        expected_dates = self._expected_dates(start, end, data_type)
        codes, warnings = self._resolve_universe_codes(universe)
        actual_dates_by_code = self._actual_dates(codes, data_type, expected_dates)

        rows = [
            self._build_row(
                universe=universe,
                data_type=data_type,
                code=code,
                expected_dates=expected_dates,
                actual_dates=actual_dates_by_code.get(code, []),
            )
            for code in codes
        ]

        if data_type.get("category") == "financial" and self.source == "xtquant":
            warnings.append("财务数据接口因券商环境差异较大；当前仅在发现可用 xtdata 财务方法时读取，否则会回退到模拟覆盖率。")

        return self._build_payload(
            rows=rows,
            universe=universe,
            data_type=data_type,
            expected_dates=expected_dates,
            warnings=warnings,
            scanned_at=now,
            start_date=start,
            end_date=end,
        )

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as fp:
            return json.load(fp)

    def _load_xtdata(self) -> Any | None:
        try:
            from xtquant import xtdata  # type: ignore

            return xtdata
        except Exception:
            return None

    def _get_config_item(self, section: str, key: str | None) -> dict[str, Any]:
        items = self.config.get(section, [])
        if key:
            for item in items:
                if item.get("key") == key:
                    return item
        if not items:
            raise ValueError(f"配置缺少 {section}")
        return items[0]

    def _resolve_date_range(self, start_date: str | None, end_date: str | None) -> tuple[str, str]:
        end = self._parse_date(end_date) or dt.date.today()
        start = self._parse_date(start_date)
        if not start:
            default_days = int(self.config.get("default_days", 60))
            start = end - dt.timedelta(days=max(default_days * 2, default_days + 20))
        if start > end:
            start, end = end, start
        return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    def _parse_date(self, value: str | None) -> dt.date | None:
        if not value:
            return None
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) < 8:
            return None
        try:
            return dt.datetime.strptime(digits[:8], "%Y%m%d").date()
        except ValueError:
            return None

    def _resolve_universe_codes(self, universe: dict[str, Any]) -> tuple[list[str], list[str]]:
        warnings: list[str] = []
        if universe.get("type") == "codes":
            return list(dict.fromkeys(universe.get("codes", []))), warnings

        codes: list[str] = []
        if self.xtdata and hasattr(self.xtdata, "get_stock_list_in_sector"):
            for sector in universe.get("sector_names", []):
                try:
                    codes.extend(self.xtdata.get_stock_list_in_sector(sector) or [])
                except Exception as exc:
                    warnings.append(f"{universe.get('name')} 获取板块 {sector} 失败: {exc}")

        if not codes:
            codes = list(universe.get("fallback_codes", []))
            if codes:
                warnings.append(f"{universe.get('name')} 暂用 fallback_codes；在 iQuant 环境启动后可读取完整板块。")

        return list(dict.fromkeys(codes)), warnings

    def _expected_dates(self, start_date: str, end_date: str, data_type: dict[str, Any]) -> list[str]:
        if data_type.get("category") == "financial":
            return self._report_dates_between(start_date, end_date, data_type.get("report_dates", []))
        return self._trade_dates_between(start_date, end_date, data_type.get("period", "1d"))

    def _trade_dates_between(self, start_date: str, end_date: str, period: str) -> list[str]:
        if self.xtdata and hasattr(self.xtdata, "get_trading_dates"):
            calls = [
                (("SH", start_date, end_date, 0, period), {}),
                (("SH", start_date, end_date, 0), {"period": period}),
                (("SH",), {"start_date": start_date, "end_date": end_date, "period": period}),
            ]
            for args, kwargs in calls:
                try:
                    dates = self.xtdata.get_trading_dates(*args, **kwargs) or []
                    normalized = [self._to_yyyymmdd(item) for item in dates]
                    normalized = [item for item in normalized if start_date <= item <= end_date]
                    if normalized:
                        return sorted(dict.fromkeys(normalized))
                except Exception:
                    continue
        return self._weekday_dates_between(start_date, end_date)

    def _report_dates_between(self, start_date: str, end_date: str, suffixes: list[str]) -> list[str]:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        dates: list[str] = []
        for year in range(start_year, end_year + 1):
            for suffix in suffixes:
                value = f"{year}{suffix}"
                if start_date <= value <= end_date:
                    dates.append(value)
        return dates

    def _actual_dates(
        self,
        codes: list[str],
        data_type: dict[str, Any],
        expected_dates: list[str],
    ) -> dict[str, list[str]]:
        if not codes or not expected_dates:
            return {code: [] for code in codes}
        if data_type.get("category") == "financial":
            return self._financial_dates(codes, data_type, expected_dates)
        return self._market_dates(codes, data_type, expected_dates)

    def _market_dates(
        self,
        codes: list[str],
        data_type: dict[str, Any],
        expected_dates: list[str],
    ) -> dict[str, list[str]]:
        if self.xtdata and hasattr(self.xtdata, "get_market_data_ex"):
            try:
                data = self.xtdata.get_market_data_ex(
                    data_type.get("fields", ["close"]),
                    codes,
                    period=data_type.get("period", "1d"),
                    start_time=expected_dates[0],
                    end_time=expected_dates[-1],
                    count=-1,
                    dividend_type="follow",
                    fill_data=False,
                    subscribe=False,
                )
                return {code: self._extract_dates(data.get(code)) for code in codes if code in data}
            except Exception:
                pass
        return self._demo_dates(codes, expected_dates, data_type.get("key", "daily_1d"))

    def _financial_dates(
        self,
        codes: list[str],
        data_type: dict[str, Any],
        expected_dates: list[str],
    ) -> dict[str, list[str]]:
        if self.xtdata:
            for method_name in ("get_financial_data", "get_raw_financial_data"):
                method = getattr(self.xtdata, method_name, None)
                if not callable(method):
                    continue
                try:
                    data = method(codes, expected_dates[0], expected_dates[-1])
                    parsed = self._extract_financial_result(codes, data)
                    if parsed:
                        return parsed
                except Exception:
                    continue
        return self._demo_dates(codes, expected_dates, data_type.get("key", "financial"))

    def _build_row(
        self,
        universe: dict[str, Any],
        data_type: dict[str, Any],
        code: str,
        expected_dates: list[str],
        actual_dates: list[str],
    ) -> InstrumentCompleteness:
        expected_set = set(expected_dates)
        actual = sorted({day for day in actual_dates if day in expected_set})
        missing_dates = [day for day in expected_dates if day not in set(actual)]
        coverage = len(actual) / len(expected_dates) if expected_dates else 0

        if not actual:
            status = "missing"
            note = "所选时间段完全没有数据"
        elif coverage == 1:
            status = "ok"
            note = "全覆盖"
        elif actual and max(actual) < expected_dates[-1]:
            status = "stale"
            note = "尾部日期缺失"
        else:
            status = "partial"
            note = "区间内存在缺口"

        return InstrumentCompleteness(
            universe_key=str(universe.get("key", "")),
            universe_name=str(universe.get("name", "")),
            data_type=str(data_type.get("key", "")),
            data_type_name=str(data_type.get("name", "")),
            code=code,
            status=status,
            first_date=min(actual) if actual else None,
            latest_date=max(actual) if actual else None,
            expected_count=len(expected_dates),
            actual_count=len(actual),
            missing_count=len(missing_dates),
            coverage=round(coverage, 4),
            missing_dates=missing_dates,
            note=note,
        )

    def _build_payload(
        self,
        rows: list[InstrumentCompleteness],
        universe: dict[str, Any],
        data_type: dict[str, Any],
        expected_dates: list[str],
        warnings: list[str],
        scanned_at: dt.datetime,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        total = len(rows)
        ok_count = sum(1 for row in rows if row.status == "ok")
        missing_count = sum(1 for row in rows if row.status == "missing")
        avg_coverage = round(sum(row.coverage for row in rows) / total, 4) if total else 0

        date_summary = []
        for day in expected_dates:
            covered = sum(1 for row in rows if day not in row.missing_dates)
            coverage = round(covered / total, 4) if total else 0
            date_summary.append(
                {
                    "date": day,
                    "covered": covered,
                    "missing": max(0, total - covered),
                    "total": total,
                    "coverage": coverage,
                    "ratio": round(1 - coverage, 4),
                }
            )

        worst_dates = sorted(date_summary, key=lambda item: item["coverage"])[:10]

        return {
            "title": self.config.get("title", "iQuant 数据完整度大盘"),
            "source": self.source,
            "scanned_at": scanned_at.strftime("%Y-%m-%d %H:%M:%S"),
            "config": {
                "universes": self.config.get("universes", []),
                "data_types": self.config.get("data_types", []),
                "default_universe": self.config.get("default_universe"),
                "default_data_type": self.config.get("default_data_type"),
            },
            "query": {
                "universe": universe.get("key"),
                "universe_name": universe.get("name"),
                "data_type": data_type.get("key"),
                "data_type_name": data_type.get("name"),
                "start_date": start_date,
                "end_date": end_date,
            },
            "overview": {
                "total": total,
                "ok": ok_count,
                "problem": total - ok_count,
                "missing": missing_count,
                "avg_coverage": avg_coverage,
                "expected_count": len(expected_dates),
            },
            "date_summary": date_summary,
            "worst_dates": worst_dates,
            "rows": [asdict(row) for row in rows],
            "warnings": warnings,
        }

    def _extract_dates(self, frame: Any) -> list[str]:
        if frame is None:
            return []
        try:
            index = frame.index
        except Exception:
            return []
        dates = [self._to_yyyymmdd(item) for item in index]
        return sorted({item for item in dates if item})

    def _extract_financial_result(self, codes: list[str], data: Any) -> dict[str, list[str]]:
        if not data:
            return {}
        result: dict[str, list[str]] = {}
        if isinstance(data, dict):
            for code in codes:
                item = data.get(code)
                if isinstance(item, dict):
                    dates = []
                    for value in item.values():
                        if isinstance(value, dict):
                            dates.extend(self._to_yyyymmdd(key) for key in value.keys())
                    result[code] = sorted({day for day in dates if day})
        return {code: dates for code, dates in result.items() if dates}

    def _demo_dates(self, codes: list[str], expected_dates: list[str], salt: str) -> dict[str, list[str]]:
        rng = random.Random(f"20260624:{salt}")
        data: dict[str, list[str]] = {}
        for idx, code in enumerate(codes):
            dates = list(expected_dates)
            if idx % 11 == 0:
                dates = []
            elif idx % 9 == 0:
                dates = dates[:-rng.randint(1, min(6, max(1, len(dates))))]
            elif idx % 7 == 0 and len(dates) > 8:
                for remove_idx in sorted({rng.randrange(0, len(dates)) for _ in range(3)}, reverse=True):
                    dates.pop(remove_idx)
            elif idx % 5 == 0 and len(dates) > 20:
                start = rng.randrange(2, len(dates) - 8)
                del dates[start : start + 4]
            data[code] = dates
        return data

    def _weekday_dates_between(self, start_date: str, end_date: str) -> list[str]:
        start = dt.datetime.strptime(start_date, "%Y%m%d").date()
        end = dt.datetime.strptime(end_date, "%Y%m%d").date()
        dates: list[str] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current.strftime("%Y%m%d"))
            current += dt.timedelta(days=1)
        return dates

    def _to_yyyymmdd(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, dt.datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, dt.date):
            return value.strftime("%Y%m%d")
        text = str(value)
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]
        return ""
