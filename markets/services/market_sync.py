from __future__ import annotations

import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import date as _date
from decimal import Decimal
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from django.db import transaction
from django.utils import timezone
from zoneinfo import ZoneInfo

from ..models import (
    Market,
    DailyStockSnapshot,
    Stock,
)

logger = logging.getLogger(__name__)

# =========================================================
# Local constants (avoid missing model deps)
# =========================================================
class Exchange:
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"
    NASDAQ = "NASDAQ"


# =========================================================
# Config
# =========================================================
UNIVERSE_N = 100
HTTP_TIMEOUT = 10
REQUEST_SLEEP = 0.12
INTRADAY_SLEEP = 0.05

NAVER_BASE = "https://finance.naver.com"

SLICK_BASE = "https://www.slickcharts.com"
SLICK_NDX_COMPANIES_PATH = "/nasdaq100"
SLICK_NDX_ANALYSIS_PATH = "/nasdaq100/analysis"

NY_TZ = ZoneInfo("America/New_York")

_CODE_RE = re.compile(r"code=(\d{6})")

SLICK_DEBUG = True


# =========================================================
# Result
# =========================================================
@dataclass
class SyncResult:
    market: str
    asof: _date
    stocks_upserted: int = 0
    indicators_upserted: int = 0  # indicators disabled


# =========================================================
# Utils
# =========================================================
def _sleep(sec: float) -> None:
    if sec and sec > 0:
        time.sleep(sec)


def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper()


def _to_decimal(x) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None

        neg = False
        if s.startswith("(") and s.endswith(")"):
            neg = True
            s = s[1:-1].strip()

        s = s.replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() == "nan":
            return None

        d = Decimal(s)
        return -d if neg else d
    except Exception:
        return None


def _safe_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, float) and math.isnan(x):
            return None
        s = str(x).replace(",", "").strip()
        if s == "" or s.lower() == "nan":
            return None
        return int(float(s))
    except Exception:
        return None


def _pct_from_prev(prev: Optional[Decimal], cur: Optional[Decimal]) -> Optional[Decimal]:
    if prev is None or cur is None or prev == 0:
        return None
    return (cur - prev) / prev * Decimal("100")


def _normalize_col(s: Any) -> str:
    return re.sub(r"\s+", "", str(s or "").strip())


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm_map = {_normalize_col(str(c)): str(c) for c in df.columns}
    for cand in candidates:
        key = _normalize_col(cand)
        if key in norm_map:
            return norm_map[key]
    for c in df.columns:
        cc = _normalize_col(str(c))
        for cand in candidates:
            if _normalize_col(cand) in cc:
                return str(c)
    return None


# =========================================================
# HTTP
# =========================================================
class HttpClient:
    def __init__(self) -> None:
        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; swjbs-bot/1.0)",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    def get_text(
        self,
        url: str,
        *,
        referer: str | None = None,
        timeout: int = HTTP_TIMEOUT,
        retries: int = 2,
        encoding: str | None = None,
    ) -> str:
        headers = {}
        if referer:
            headers["Referer"] = referer

        last_err: Exception | None = None
        for attempt in range(retries + 1):
            try:
                r = self.sess.get(url, headers=headers, timeout=timeout)
                r.raise_for_status()
                if encoding:
                    r.encoding = encoding
                else:
                    if not r.encoding:
                        r.encoding = "euc-kr"
                return r.text
            except Exception as e:
                last_err = e
                _sleep(0.3 * (2**attempt))
        raise last_err or RuntimeError("HTTP get_text failed")


_http = HttpClient()


# =========================================================
# Naver helpers (KR)
# =========================================================
def _extract_code_map_from_links(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    m: Dict[str, str] = {}
    for a in soup.select('a[href*="code="]'):
        mm = _CODE_RE.search(a.get("href", "") or "")
        if not mm:
            continue
        name = (a.get_text(strip=True) or "").strip()
        code = mm.group(1)
        if name and code and name not in m:
            m[name] = code
    return m


def _naver_market_sum_url(sosok: int, sort: str, page: int = 1) -> str:
    return f"{NAVER_BASE}/sise/sise_market_sum.naver?sosok={sosok}&sort={sort}&asc=0&page={page}"


def _naver_fetch_latest_date_only(code: str) -> Optional[_date]:
    url = f"{NAVER_BASE}/item/sise_day.nhn?code={code}&page=1"
    html = _http.get_text(url, referer=NAVER_BASE)
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None
    if not tables:
        return None
    df = tables[0]
    col_date = _pick_col(df, ["날짜"])
    if not col_date:
        return None
    df = df.dropna(subset=[col_date])
    if df.empty:
        return None
    try:
        y, m, d = str(df.iloc[0][col_date]).split(".")
        return _date(int(y), int(m), int(d))
    except Exception:
        return None


def _parse_kr_market_sum_top100(sosok: int, limit: int) -> List[dict]:
    url = _naver_market_sum_url(sosok=sosok, sort="market_sum", page=1)
    html = _http.get_text(url, referer=NAVER_BASE)

    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        logger.warning("KR market_sum: read_html failed", exc_info=True)
        return []
    if not tables:
        return []

    df = None
    for t in tables:
        if t is None or t.empty:
            continue
        cols = [str(c) for c in t.columns]
        if any("종목" in c for c in cols) and any("시가총액" in c for c in cols):
            df = t
            break
    if df is None or df.empty:
        return []

    col_name = _pick_col(df, ["종목명", "종목"])
    col_close = _pick_col(df, ["현재가"])
    col_chg = _pick_col(df, ["등락률", "등락율"])
    col_vol = _pick_col(df, ["거래량"])
    col_amount = _pick_col(df, ["거래대금"])
    col_mcap = _pick_col(df, ["시가총액"])

    if not col_name or not col_mcap:
        return []

    code_map = _extract_code_map_from_links(html)
    df = df.dropna(subset=[col_name]).copy()

    mcap_mul = 100_000_000  # 억원 -> KRW
    amount_mul = 1_000_000  # 백만 -> KRW

    out: List[dict] = []
    for _, r in df.iterrows():
        name = str(r.get(col_name) or "").strip()
        if not name:
            continue
        code = code_map.get(name)
        if not code:
            continue

        last_price = _to_decimal(r.get(col_close)) if col_close else None
        chg_pct = _to_decimal(r.get(col_chg)) if col_chg else None
        vol_ = _safe_int(r.get(col_vol)) if col_vol else None

        mcap_raw = _safe_int(r.get(col_mcap))
        market_cap = (mcap_raw * mcap_mul) if mcap_raw is not None else None

        amt_raw = _safe_int(r.get(col_amount)) if col_amount else None
        traded_value = (amt_raw * amount_mul) if amt_raw is not None else None

        out.append(
            {
                "symbol": code,
                "name": name,
                "price": last_price,
                "change_pct": chg_pct,
                "volume": vol_,
                "market_cap": market_cap,
                "traded_value": traded_value,
            }
        )
        if len(out) >= limit:
            break

    return out


def _collect_kr_universe_eod(exchange: str, target: _date) -> Tuple[_date, List[dict]]:
    sosok = 0 if exchange == Exchange.KOSPI else 1
    rows = _parse_kr_market_sum_top100(sosok=sosok, limit=UNIVERSE_N)
    _sleep(REQUEST_SLEEP)

    rep = rows[0]["symbol"] if rows else None
    asof = _naver_fetch_latest_date_only(rep) if rep else None
    asof = asof or target
    return asof, rows


def _collect_kr_universe_intraday(exchange: str) -> List[dict]:
    sosok = 0 if exchange == Exchange.KOSPI else 1
    rows = _parse_kr_market_sum_top100(sosok=sosok, limit=UNIVERSE_N)
    _sleep(REQUEST_SLEEP)
    return rows


# =========================================================
# Slickcharts helpers (NASDAQ 100)
# =========================================================
def _slick_fetch(path: str) -> str:
    url = f"{SLICK_BASE}{path}"
    return _http.get_text(url, referer=SLICK_BASE, encoding="utf-8")


def _parse_money_to_int_usd(text: str) -> Optional[int]:
    if not text:
        return None
    t = str(text).strip().replace(",", "").replace("$", "")
    m = re.search(r"([\d.]+)\s*([TBM])", t, re.IGNORECASE)
    if not m:
        m2 = re.search(r"([\d.]+)", t)
        if not m2:
            return None
        try:
            return int(Decimal(m2.group(1)))
        except Exception:
            return None

    num = Decimal(m.group(1))
    unit = m.group(2).upper()
    mul = {
        "T": Decimal("1000000000000"),
        "B": Decimal("1000000000"),
        "M": Decimal("1000000"),
    }.get(unit, Decimal("1"))
    try:
        return int(num * mul)
    except Exception:
        return None


def _slick_parse_nasdaq100(limit: int) -> List[dict]:
    html_comp = _slick_fetch(SLICK_NDX_COMPANIES_PATH)
    html_an = _slick_fetch(SLICK_NDX_ANALYSIS_PATH)

    # analysis에서 market cap 매핑
    mcap_map: Dict[str, int] = {}
    try:
        tables_an = pd.read_html(StringIO(html_an))
    except Exception:
        tables_an = []

    df_an = None
    best = -1
    for t in tables_an:
        if t is None or t.empty:
            continue
        cols = [_normalize_col(c) for c in t.columns]
        score = 0
        if any("Symbol" in c for c in cols):
            score += 3
        if any("MarketCap" in c for c in cols) or any("MarketCap" in c.replace(" ", "") for c in cols):
            score += 2
        if any("Company" in c for c in cols):
            score += 1
        if score > best:
            best = score
            df_an = t

    if df_an is not None and not df_an.empty:
        col_sym = None
        col_mcap = None
        for c in df_an.columns:
            n = _normalize_col(str(c)).lower()
            if n == "symbol":
                col_sym = c
            elif n in ("marketcap", "market_cap", "marketcapitalization"):
                col_mcap = c

        if col_mcap is None:
            for c in df_an.columns:
                if "Market Cap" in str(c) or "MarketCap" in _normalize_col(str(c)):
                    col_mcap = c
                    break

        if col_sym is not None and col_mcap is not None:
            df_an = df_an.dropna(subset=[col_sym]).copy()
            for _, r in df_an.iterrows():
                sym = _norm_ticker(str(r.get(col_sym) or ""))
                if not sym:
                    continue
                mc_int = _parse_money_to_int_usd(r.get(col_mcap))
                if mc_int is not None:
                    mcap_map[sym] = mc_int

    # companies에서 price/pctchg
    try:
        tables = pd.read_html(StringIO(html_comp))
    except Exception:
        logger.warning("Slickcharts nasdaq100: read_html failed", exc_info=True)
        return []
    if not tables:
        return []

    df = None
    best = -1
    for t in tables:
        if t is None or t.empty:
            continue
        cols = [_normalize_col(c) for c in t.columns]
        score = 0
        if any("Symbol" in c for c in cols):
            score += 3
        if any("Weight" in c for c in cols):
            score += 2
        if any("%Chg" in c or "Chg" in c for c in cols):
            score += 1
        if score > best:
            best = score
            df = t

    if df is None or df.empty:
        return []

    col_symbol = None
    col_company = None
    col_price = None
    col_pctchg = None

    for c in df.columns:
        n = _normalize_col(str(c)).lower()
        if n == "symbol":
            col_symbol = c
        elif n == "company":
            col_company = c
        elif n in ("price", "lastprice", "last"):
            col_price = c
        elif n in ("%chg", "pctchg", "percentchange", "change%"):
            col_pctchg = c

    if col_price is None:
        for c in df.columns:
            if "Price" in str(c):
                col_price = c
                break
    if col_pctchg is None:
        for c in df.columns:
            if "% Chg" in str(c) or "%Chg" in str(c):
                col_pctchg = c
                break

    if col_symbol is None:
        return []

    df = df.dropna(subset=[col_symbol]).copy()

    out: List[dict] = []
    for _, r in df.iterrows():
        tk = _norm_ticker(str(r.get(col_symbol) or ""))
        if not tk:
            continue

        name = str(r.get(col_company) or tk).strip() if col_company is not None else tk
        price_ = _to_decimal(r.get(col_price)) if col_price is not None else None
        pct_ = _to_decimal(r.get(col_pctchg)) if col_pctchg is not None else None

        out.append(
            {
                "symbol": tk,
                "name": name,
                "price": price_,
                "change_pct": pct_,
                "market_cap": mcap_map.get(tk),
                "volume": None,
                "traded_value": None,
            }
        )
        if len(out) >= limit:
            break

    if SLICK_DEBUG:
        non_null_mcap = sum(1 for x in out if x.get("market_cap") is not None)
        logger.warning(
            "[SLICK_DEBUG] analysis_mcap_map=%d out_rows=%d non_null_market_cap=%d",
            len(mcap_map),
            len(out),
            non_null_mcap,
        )
        if out:
            logger.warning("[SLICK_DEBUG] sample=%s", out[0])

    return out


# =========================================================
# Yahoo Chart API
# =========================================================
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _yahoo_chart(symbol: str, range_: str, interval: str) -> Optional[dict]:
    url = YAHOO_CHART.format(symbol=symbol)
    params = {"range": range_, "interval": interval}
    try:
        r = _http.sess.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        logger.warning("Yahoo chart fetch failed: %s", symbol, exc_info=True)
        return None


def _yahoo_latest_close_and_date(symbol: str) -> Tuple[Optional[_date], Optional[Decimal]]:
    js = _yahoo_chart(symbol, range_="5d", interval="1d")
    try:
        result = js["chart"]["result"][0]
        ts = result["timestamp"][-1]
        close = result["indicators"]["quote"][0]["close"][-1]
        dt = pd.Timestamp(ts, unit="s", tz=NY_TZ).date()
        return dt, _to_decimal(close)
    except Exception:
        return None, None


def _yahoo_latest_volume(symbol: str) -> Optional[int]:
    js = _yahoo_chart(symbol, range_="1d", interval="1m")
    try:
        result = js["chart"]["result"][0]
        vols = result["indicators"]["quote"][0]["volume"]
        if not vols:
            return None
        for v in reversed(vols):
            vv = _safe_int(v)
            if vv is not None:
                return vv
        return None
    except Exception:
        return None


def _collect_nasdaq_asof(target: _date) -> _date:
    dt, _ = _yahoo_latest_close_and_date("^IXIC")
    return dt or target


# =========================================================
# DB helpers (DailyStockSnapshot 기반)
# =========================================================
def _latest_snapshot_date_for_exchange(exchange: str, target: _date) -> _date:
    d = (
        DailyStockSnapshot.objects.filter(stock__exchange=exchange, date__lte=target)
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )
    if d:
        return d
    d2 = (
        DailyStockSnapshot.objects.filter(stock__exchange=exchange)
        .order_by("-date")
        .values_list("date", flat=True)
        .first())
    return d2 or target


def _prev_close_map(exchange: str, asof: _date) -> Dict[str, Decimal]:
    prev_date = (
        DailyStockSnapshot.objects.filter(stock__exchange=exchange, date__lt=asof)
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )
    if not prev_date:
        return {}

    qs = (
        DailyStockSnapshot.objects.filter(stock__exchange=exchange, date=prev_date)
        .select_related("stock")
        .values_list("stock__symbol", "close")
    )
    m: Dict[str, Decimal] = {}
    for sym, close in qs:
        dc = _to_decimal(close)
        if sym and dc is not None:
            m[str(sym)] = dc
    return m


# =========================================================
# Persistence
# =========================================================
def _stock_upsert(market: str, exchange: str, symbol: str, name: str, currency: str) -> Stock:
    stock, _ = Stock.objects.update_or_create(
        market=market,
        symbol=symbol,
        defaults={
            "name": name or symbol,
            "exchange": exchange,
            "currency": currency,
            "is_active": True,
        },
    )
    return stock


def _persist_eod_rows(market: str, exchange: str, asof: _date, rows: List[dict], currency: str) -> int:
    upserted = 0
    for r in rows:
        symbol = _norm_ticker(r.get("symbol") or "")
        if not symbol:
            continue
        try:
            stock = _stock_upsert(
                market=market,
                exchange=exchange,
                symbol=symbol,
                name=str(r.get("name") or symbol),
                currency=currency,
            )

            DailyStockSnapshot.objects.update_or_create(
                stock=stock,
                date=asof,
                defaults={
                    "open": None,
                    "close": r.get("price"),
                    "prev_close": None,
                    "intraday_pct": None,
                    "change_pct": r.get("change_pct"),
                    "market_cap": r.get("market_cap"),
                    "volume": r.get("volume"),
                    "volatility_20d": None,
                },
            )
            upserted += 1
        except Exception:
            logger.warning("EOD persist failed: %s %s", exchange, symbol, exc_info=True)

    return upserted


def _persist_intraday_rows(market: str, exchange: str, asof: _date, rows: List[dict], currency: str) -> int:
    prev_map = _prev_close_map(exchange=exchange, asof=asof)

    upserted = 0
    for r in rows:
        symbol = _norm_ticker(r.get("symbol") or "")
        if not symbol:
            continue

        try:
            stock = _stock_upsert(
                market=market,
                exchange=exchange,
                symbol=symbol,
                name=str(r.get("name") or symbol),
                currency=currency,
            )

            last_price = r.get("price")
            prev_close = prev_map.get(symbol)

            intraday_pct = (
                _pct_from_prev(prev_close, last_price)
                if (prev_close is not None and last_price is not None)
                else None
            )
            if intraday_pct is None:
                intraday_pct = r.get("change_pct")

            DailyStockSnapshot.objects.update_or_create(
                stock=stock,
                date=asof,
                defaults={
                    "open": None,
                    "close": last_price,
                    "prev_close": prev_close,
                    "intraday_pct": intraday_pct,
                    "change_pct": intraday_pct,  # UI 호환
                    "market_cap": r.get("market_cap"),
                    "volume": r.get("volume"),
                },
            )
            upserted += 1
        except Exception:
            logger.warning("INTRADAY persist failed: %s %s", exchange, symbol, exc_info=True)

    return upserted


# =========================================================
# Collectors
# =========================================================
def _collect_nasdaq_universe_eod(target: _date) -> Tuple[_date, List[dict]]:
    asof = _collect_nasdaq_asof(target)
    rows = _slick_parse_nasdaq100(limit=UNIVERSE_N)

    for r in rows:
        sym = r.get("symbol")
        if not sym:
            continue
        r["volume"] = _yahoo_latest_volume(sym)
        _sleep(INTRADAY_SLEEP)

    return asof, rows


def _collect_nasdaq_universe_intraday(asof: _date) -> List[dict]:
    rows = _slick_parse_nasdaq100(limit=UNIVERSE_N)

    for r in rows:
        sym = r.get("symbol")
        if not sym:
            continue
        r["volume"] = _yahoo_latest_volume(sym)
        _sleep(INTRADAY_SLEEP)

    return rows


# =========================================================
# Public API
# =========================================================
def sync_market_eod(market: str, target_date: _date | None = None) -> SyncResult:
    m = (market or "KOSDAQ").upper().strip()
    target = target_date or timezone.localdate()

    if m not in {"KOSPI", "KOSDAQ", "NASDAQ", "KR", "US"}:
        raise ValueError("market must be KOSPI, KOSDAQ, NASDAQ, KR, US")

    def _run_one(exchange: str) -> SyncResult:
        if exchange in (Exchange.KOSPI, Exchange.KOSDAQ):
            asof, rows = _collect_kr_universe_eod(exchange=exchange, target=target)
            with transaction.atomic():
                up = _persist_eod_rows(Market.KR, exchange, asof, rows, currency="KRW")
            return SyncResult(market=exchange, asof=asof, stocks_upserted=up, indicators_upserted=0)

        if exchange == Exchange.NASDAQ:
            asof, rows = _collect_nasdaq_universe_eod(target=target)
            with transaction.atomic():
                up = _persist_eod_rows(Market.US, exchange, asof, rows, currency="USD")
            return SyncResult(market=exchange, asof=asof, stocks_upserted=up, indicators_upserted=0)

        raise ValueError("unsupported exchange")

    if m == "KOSPI":
        return _run_one(Exchange.KOSPI)
    if m == "KOSDAQ":
        return _run_one(Exchange.KOSDAQ)
    if m == "NASDAQ":
        return _run_one(Exchange.NASDAQ)

    if m == "KR":
        r1 = _run_one(Exchange.KOSPI)
        r2 = _run_one(Exchange.KOSDAQ)
        return SyncResult(
            market="KR",
            asof=max(r1.asof, r2.asof),
            stocks_upserted=r1.stocks_upserted + r2.stocks_upserted,
            indicators_upserted=0,
        )

    r = _run_one(Exchange.NASDAQ)
    return SyncResult(
        market="US",
        asof=r.asof,
        stocks_upserted=r.stocks_upserted,
        indicators_upserted=0,
    )


def sync_market_intraday(market: str, target_date: _date | None = None) -> SyncResult:
    m = (market or "KOSDAQ").upper().strip()
    target = target_date or timezone.localdate()

    if m not in {"KOSPI", "KOSDAQ", "NASDAQ", "KR", "US"}:
        raise ValueError("market must be KOSPI, KOSDAQ, NASDAQ, KR, US")

    def _run_one(exchange: str) -> SyncResult:
        if exchange in (Exchange.KOSPI, Exchange.KOSDAQ):
            asof = _latest_snapshot_date_for_exchange(exchange=exchange, target=target)
            rows = _collect_kr_universe_intraday(exchange=exchange)
            with transaction.atomic():
                up = _persist_intraday_rows(Market.KR, exchange, asof, rows, currency="KRW")
            return SyncResult(market=exchange, asof=asof, stocks_upserted=up, indicators_upserted=0)

        if exchange == Exchange.NASDAQ:
            asof = _collect_nasdaq_asof(target)
            rows = _collect_nasdaq_universe_intraday(asof=asof)
            with transaction.atomic():
                up = _persist_intraday_rows(Market.US, exchange, asof, rows, currency="USD")
            return SyncResult(market=exchange, asof=asof, stocks_upserted=up, indicators_upserted=0)

        raise ValueError("unsupported exchange")

    if m == "KOSPI":
        return _run_one(Exchange.KOSPI)
    if m == "KOSDAQ":
        return _run_one(Exchange.KOSDAQ)
    if m == "NASDAQ":
        return _run_one(Exchange.NASDAQ)

    if m == "KR":
        r1 = _run_one(Exchange.KOSPI)
        r2 = _run_one(Exchange.KOSDAQ)
        return SyncResult(
            market="KR",
            asof=max(r1.asof, r2.asof),
            stocks_upserted=r1.stocks_upserted + r2.stocks_upserted,
            indicators_upserted=0,
        )

    r = _run_one(Exchange.NASDAQ)
    return SyncResult(
        market="US",
        asof=r.asof,
        stocks_upserted=r.stocks_upserted,
        indicators_upserted=0,
    )
    