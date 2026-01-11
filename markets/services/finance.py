# apps/markets/services/finance.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
import json
import os
import random
import time
import re

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from django.conf import settings


DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class TrendResult:
    data: List[Dict[str, Any]]
    total_count: Optional[int] = None
    total_pages: Optional[int] = None
    current_page: Optional[int] = None
    page_size: Optional[int] = None


# ----------------------------
# Helpers
# ----------------------------
def _to_float_maybe(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _parse_pct(s: Any) -> Optional[float]:
    """
    "-0.10%" or "0.13%" -> -0.10 / 0.13 (float)
    """
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    t = t.replace("%", "").strip()
    return _to_float_maybe(t)


def _parse_price_change_cell(s: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    SlickCharts 'Chg' 셀 예:
      "-0.18(-0.10%)"
      "0.33(0.13%)"
    또는 (최근 변경으로)
      "-0.18"
      "0.33"
    반환: (chg_value, pct_value)
    """
    if s is None:
        return (None, None)
    t = str(s).strip()
    if not t:
        return (None, None)

    m = re.match(r"^\s*([+-]?[0-9\.,]+)\s*\(\s*([+-]?[0-9\.,]+)\s*%\s*\)\s*$", t)
    if not m:
        # 괄호가 있는 변형 케이스
        if "(" in t and ")" in t:
            left = t.split("(", 1)[0].strip()
            inside = t.split("(", 1)[1].split(")", 1)[0].strip()
            inside = inside.replace("%", "").strip()
            return (_to_float_maybe(left), _to_float_maybe(inside))

        # 괄호/퍼센트가 없으면 chg만 존재
        return (_to_float_maybe(t), None)

    chg = _to_float_maybe(m.group(1))
    pct = _to_float_maybe(m.group(2))
    return (chg, pct)


def _parse_market_cap_to_int(s: Any) -> Optional[int]:
    """
    SlickCharts Market Cap 표기 예:
      "4.49T", "494.45B", "980.12M"
    반환: USD 절대값(int)
    """
    if s is None:
        return None
    t = str(s).strip().replace(",", "")
    if not t:
        return None

    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([TtBbMmKk])?\s*$", t)
    if not m:
        return None

    val = float(m.group(1))
    unit = (m.group(2) or "").upper()
    mult = {
        "T": 1_000_000_000_000,
        "B": 1_000_000_000,
        "M": 1_000_000,
        "K": 1_000,
        "": 1,
    }.get(unit, 1)

    return int(round(val * mult))


def format_market_cap(n: Optional[Union[int, float]]) -> str:
    """
    화면용: 4490000000000 -> "4.49T"
    """
    if n is None:
        return "-"

    n = float(n)
    abs_n = abs(n)

    if abs_n >= 1_000_000_000_000:
        v, suf = n / 1_000_000_000_000, "T"
    elif abs_n >= 1_000_000_000:
        v, suf = n / 1_000_000_000, "B"
    elif abs_n >= 1_000_000:
        v, suf = n / 1_000_000, "M"
    elif abs_n >= 1_000:
        v, suf = n / 1_000, "K"
    else:
        v, suf = n, ""

    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return f"{s}{suf}"


def _ensure_pct_signed_for_fall(change_type: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Daum FALL은 응답 changeRate가 양수로 올 때가 있어(또는 혼합) 서버에서 일관화.
    - RISE: changeRate는 양수 유지
    - FALL: changeRate는 음수로 강제
    """
    out: List[Dict[str, Any]] = []
    for r in rows:
        cr = r.get("changeRate")
        try:
            crf = float(cr) if cr is not None else None
        except Exception:
            crf = None

        if crf is not None:
            if change_type == "FALL":
                crf = -abs(crf)
            else:
                crf = abs(crf)
            r = dict(r)
            r["changeRate"] = crf
        out.append(r)
    return out


# ----------------------------
# 1) Daum (KR)
# ----------------------------
class DaumFinanceClient:
    BASE = "https://finance.daum.net"

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.session = requests.Session()

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _get_json(self, path: str, params: Dict[str, Any], referer: str) -> Dict[str, Any]:
        url = f"{self.BASE}{path}"
        headers = {
            "User-Agent": DEFAULT_UA,
            "Referer": referer,
            "Accept": "application/json, text/plain, */*",
        }
        resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_market_cap(self, market: str, page: int = 1, per_page: int = 200) -> TrendResult:
        path = "/api/trend/market_capitalization"
        params = {
            "page": page,
            "perPage": per_page,
            "fieldName": "marketCap",
            "order": "desc",
            "market": market,
            "pagination": "true",
        }
        referer = f"{self.BASE}/domestic/market_cap?market={market}"
        raw = self._get_json(path, params, referer=referer)
        return TrendResult(
            data=raw.get("data", []) or [],
            total_count=raw.get("totalCount"),
            total_pages=raw.get("totalPages"),
            current_page=raw.get("currentPage"),
            page_size=raw.get("pageSize"),
        )

    def get_price_performance(
        self, market: str, change_type: str, page: int = 1, per_page: int = 200
    ) -> TrendResult:
        """
        ✅ 핵심 수정:
        - RISE: order=desc (큰 상승률이 1위)
        - FALL: order=asc  (가장 음수인 하락률이 1위)
        - FALL changeRate가 양수로 올 때가 있어 서버에서 음수로 강제(-abs)
        """
        if change_type not in ("RISE", "FALL"):
            raise ValueError("change_type must be 'RISE' or 'FALL'")

        path = "/api/trend/price_performance"

        # ✅ Daum 'fall_stocks' 화면과 동일하게: FALL은 오름차순
        order = "desc" if change_type == "RISE" else "asc"

        params = {
            "page": page,
            "perPage": per_page,
            "intervalType": "TODAY",
            "market": market,
            "changeType": change_type,
            "pagination": "true",
            "order": order,
        }
        referer = (
            f"{self.BASE}/domestic/{'rise_stocks' if change_type == 'RISE' else 'fall_stocks'}?market={market}"
        )
        raw = self._get_json(path, params, referer=referer)

        rows = raw.get("data", []) or []
        # ✅ changeRate 부호 일관화 (FALL은 음수)
        rows = _ensure_pct_signed_for_fall(change_type, rows)

        return TrendResult(
            data=rows,
            total_count=raw.get("totalCount"),
            total_pages=raw.get("totalPages"),
            current_page=raw.get("currentPage"),
            page_size=raw.get("pageSize"),
        )


# ----------------------------
# 2) NASDAQ-100 from SlickCharts (FULL: market cap + price/chg/%chg/weight)
# ----------------------------
SLICKCHARTS_NASDAQ100_URL = "https://www.slickcharts.com/nasdaq100"
SLICKCHARTS_NASDAQ100_ANALYSIS_URL = "https://www.slickcharts.com/nasdaq100/analysis"


class SlickChartsTemporaryError(RuntimeError):
    """SlickCharts 네트워크/파싱 일시 오류."""


def _slick_cache_path() -> str:
    """
    NASDAQ-100 머지 데이터 캐시 파일 경로.
    - settings.BASE_DIR가 있으면 프로젝트 루트에 파일 생성
    - 없으면 /tmp 사용
    """
    base = getattr(settings, "BASE_DIR", None)
    if base:
        return os.path.join(str(base), ".cache_slickcharts_nasdaq100.json")
    return "/tmp/.cache_slickcharts_nasdaq100.json"


def _read_cached_slick(ttl_seconds: int) -> Optional[Dict[str, Any]]:
    path = _slick_cache_path()
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)

        fetched_at = datetime.fromisoformat(obj["fetched_at"])
        if datetime.utcnow() - fetched_at > timedelta(seconds=ttl_seconds):
            return None

        data = obj.get("data")
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return data
    except Exception:
        return None
    return None


def _write_cached_slick(data: Dict[str, Any]) -> None:
    path = _slick_cache_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"fetched_at": datetime.utcnow().isoformat(), "data": data},
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception:
        # 캐시 실패는 치명적이지 않게 무시
        pass


class SlickChartsNasdaq100Client:
    """
    - /nasdaq100 에서 Company/Symbol/Weight/Price/Chg/(%Chg optional) 추출
    - /nasdaq100/analysis 에서 Market Cap 추출
    Symbol로 merge하여:
      - MARKET_CAP(시총) 정렬
      - RISE(%chg desc) 정렬
      - FALL(%chg asc) 정렬
    """

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.session = requests.Session()

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

        # 캐시 TTL (기본 5분)
        self.ttl_seconds = int(getattr(settings, "SLICK_NASDAQ_TTL_SECONDS", 5 * 60))

    def _get_html(self, url: str, referer: str) -> str:
        headers = {
            "User-Agent": DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": referer,
        }
        resp = self.session.get(url, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def _parse_table_rows(self, html: str) -> List[List[str]]:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            raise SlickChartsTemporaryError("SlickCharts table not found")

        rows: List[List[str]] = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            rows.append([c.get_text(" ", strip=True) for c in cells])

        if len(rows) < 2:
            raise SlickChartsTemporaryError("SlickCharts table rows not found")
        return rows

    def _fetch_components(self) -> Dict[str, Dict[str, Any]]:
        """
        /nasdaq100 에서 Company/Symbol/Weight/Price/Chg/%Chg 추출 (가능하면)
        %Chg가 없거나 Chg 셀에서 %를 못 뽑으면, price/chg로 %를 계산해 changeRate를 채움.
        return: {SYMBOL: {...}}
        """
        html = self._get_html(SLICKCHARTS_NASDAQ100_URL, referer="https://www.slickcharts.com/")
        rows = self._parse_table_rows(html)

        header = rows[0]

        def idx_of(name: str) -> int:
            try:
                return header.index(name)
            except ValueError:
                for i, h in enumerate(header):
                    if h.replace(" ", "").lower() == name.replace(" ", "").lower():
                        return i
                raise

        company_i = idx_of("Company")
        symbol_i = idx_of("Symbol")
        weight_i = idx_of("Weight")
        price_i = idx_of("Price")
        chg_i = idx_of("Chg")

        # %Chg 컬럼이 존재할 수도 있으므로: 내구성 있게 탐지
        pct_i: Optional[int] = None
        for cand in ("% Chg", "%Chg", "Pct Chg", "Change %", "% Change", "Chg %"):
            try:
                pct_i = idx_of(cand)
                break
            except Exception:
                continue

        if pct_i is None:
            for i, h in enumerate(header):
                hh = h.replace(" ", "").lower()
                if "%" in h and ("chg" in hh or "change" in hh):
                    pct_i = i
                    break

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows[1:]:
            if len(r) <= max(company_i, symbol_i, weight_i, price_i, chg_i):
                continue

            company = r[company_i].strip()
            symbol = r[symbol_i].strip().upper()
            weight_s = r[weight_i].strip()
            price_s = r[price_i].strip()
            chg_s = r[chg_i].strip()

            if not symbol:
                continue

            weight = _parse_pct(weight_s)
            price = _to_float_maybe(price_s)
            chg_val, pct_val = _parse_price_change_cell(chg_s)

            # %Chg 컬럼이 있으면 우선 적용
            if pct_i is not None and pct_i < len(r):
                pct_col = _parse_pct(r[pct_i])
                if pct_col is not None:
                    pct_val = pct_col

            # Fallback: pct_val이 없으면 price/chg로 % 계산
            if pct_val is None and chg_val is not None and price is not None:
                prev = price - chg_val
                if prev and abs(prev) > 1e-12:
                    pct_val = (chg_val / prev) * 100.0

            out[symbol] = {
                "symbol": symbol,
                "name": company or symbol,
                "weight_pct": weight,
                "tradePrice": price,
                "change": chg_val,
                "changeRate": pct_val,  # percent (%)
            }

        if len(out) < 80:
            raise SlickChartsTemporaryError(f"Too few rows parsed from /nasdaq100: {len(out)}")
        return out

    def _fetch_market_caps(self) -> Dict[str, Dict[str, Any]]:
        """
        /nasdaq100/analysis 에서 market cap 추출
        return: {SYMBOL: {"marketCap": int, "marketCapText": str}}
        """
        html = self._get_html(SLICKCHARTS_NASDAQ100_ANALYSIS_URL, referer=SLICKCHARTS_NASDAQ100_URL)
        rows = self._parse_table_rows(html)

        header = rows[0]

        def idx_of(name: str) -> int:
            try:
                return header.index(name)
            except ValueError:
                for i, h in enumerate(header):
                    if h.replace(" ", "").lower() == name.replace(" ", "").lower():
                        return i
                raise

        symbol_i = idx_of("Symbol")
        company_i = idx_of("Company")
        mcap_i = idx_of("Market Cap")

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows[1:]:
            if len(r) <= max(symbol_i, company_i, mcap_i):
                continue

            symbol = str(r[symbol_i]).strip().upper()
            company = str(r[company_i]).strip()
            mcap_text = str(r[mcap_i]).strip()

            if not symbol:
                continue

            mcap_val = _parse_market_cap_to_int(mcap_text)
            out[symbol] = {
                "symbol": symbol,
                "name": company or symbol,
                "marketCap": mcap_val,
                "marketCapText": mcap_text,
            }

        if len(out) < 80:
            raise SlickChartsTemporaryError(f"Too few rows parsed from /nasdaq100/analysis: {len(out)}")
        return out

    def fetch_merged_once(self, force: bool = False) -> Dict[str, Any]:
        """
        캐시 포함: 머지된 전체 row dict 반환
        {
          "asof": "...",
          "rows": [ {...}, ... ]
        }
        """
        if not force:
            cached = _read_cached_slick(self.ttl_seconds)
            if cached:
                return cached

        time.sleep(random.uniform(0.2, 0.8))

        comps = self._fetch_components()
        time.sleep(random.uniform(0.2, 0.8))
        caps = self._fetch_market_caps()

        rows: List[Dict[str, Any]] = []
        for sym, c in comps.items():
            cap_row = caps.get(sym, {})
            mcap = cap_row.get("marketCap")
            mcap_text = cap_row.get("marketCapText")

            rows.append(
                {
                    "symbolCode": sym,
                    "name": c.get("name") or cap_row.get("name") or sym,
                    "tradePrice": c.get("tradePrice"),
                    "change": c.get("change"),
                    "changeRate": c.get("changeRate"),
                    "weight": c.get("weight_pct"),
                    "marketCap": mcap,
                    "marketCapText": mcap_text,
                    "marketCapDisplay": mcap_text or format_market_cap(mcap),
                    "source": "slickcharts",
                }
            )

        data = {"asof": datetime.utcnow().isoformat(), "rows": rows}
        _write_cached_slick(data)
        return data

    def _rank(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [{"rank": i, **r} for i, r in enumerate(rows, start=1)]

    def get_nasdaq_market_cap(self, *, per_page: int = 100) -> TrendResult:
        merged = self.fetch_merged_once()
        rows = merged["rows"]

        rows_sorted = sorted(
            rows,
            key=lambda x: (x.get("marketCap") is None, -(x.get("marketCap") or 0)),
        )[: max(1, int(per_page))]

        return TrendResult(
            data=self._rank(rows_sorted),
            total_count=len(rows_sorted),
            total_pages=1,
            current_page=1,
            page_size=per_page,
        )

    def get_nasdaq_rise(self, *, per_page: int = 100) -> TrendResult:
        merged = self.fetch_merged_once()
        rows = merged["rows"]

        # ✅ RISE는 +%만
        only_pos = []
        for r in rows:
            cr = r.get("changeRate")
            try:
                crf = float(cr) if cr is not None else None
            except Exception:
                crf = None
            if crf is None:
                continue
            if crf > 0:
                only_pos.append(r)

        rows_sorted = sorted(
            only_pos,
            key=lambda x: -(x.get("changeRate") or 0.0),
        )[: max(1, int(per_page))]

        return TrendResult(
            data=self._rank(rows_sorted),
            total_count=len(rows_sorted),
            total_pages=1,
            current_page=1,
            page_size=per_page,
        )

    def get_nasdaq_fall(self, *, per_page: int = 100) -> TrendResult:
        merged = self.fetch_merged_once()
        rows = merged["rows"]

        # ✅ FALL은 -%만
        only_neg = []
        for r in rows:
            cr = r.get("changeRate")
            try:
                crf = float(cr) if cr is not None else None
            except Exception:
                crf = None
            if crf is None:
                continue
            if crf < 0:
                only_neg.append(r)

        rows_sorted = sorted(
            only_neg,
            key=lambda x: (x.get("changeRate") or 0.0),  # 더 작은(더 음수) 값이 먼저
        )[: max(1, int(per_page))]

        return TrendResult(
            data=self._rank(rows_sorted),
            total_count=len(rows_sorted),
            total_pages=1,
            current_page=1,
            page_size=per_page,
        )


# ----------------------------
# 3) Unified facade (optional)
# ----------------------------
class FinanceFacade:
    def __init__(self):
        self.daum = DaumFinanceClient()
        self.slick = SlickChartsNasdaq100Client()

    def get_kr_today(self, *, market: str, per_page: int = 200) -> Dict[str, Any]:
        top_market_cap = self.daum.get_market_cap(market=market, per_page=per_page).data
        top_gainers = self.daum.get_price_performance(market=market, change_type="RISE", per_page=per_page).data
        top_drawdown = self.daum.get_price_performance(market=market, change_type="FALL", per_page=per_page).data

        return {
            "market": "KR",
            "exchange": market,
            "asof": datetime.utcnow().isoformat(),
            "top_market_cap": top_market_cap,
            "top_gainers": top_gainers,
            "top_drawdown": top_drawdown,
        }

    def get_us_today(self, *, per_page: int = 100) -> Dict[str, Any]:
        top_market_cap = self.slick.get_nasdaq_market_cap(per_page=per_page).data
        top_gainers = self.slick.get_nasdaq_rise(per_page=per_page).data
        top_drawdown = self.slick.get_nasdaq_fall(per_page=per_page).data

        return {
            "market": "US",
            "exchange": "NASDAQ100",
            "asof": datetime.utcnow().isoformat(),
            "top_market_cap": top_market_cap,
            "top_gainers": top_gainers,
            "top_drawdown": top_drawdown,
        }
