from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import pandas as pd
from django.db import transaction

from main.models import DailyStockSnapshot, Market, Stock


@dataclass(frozen=True)
class SyncOptions:
    market: str  # "KR" | "US"
    asof: date
    overwrite_today: bool = True

    topn_market_cap: int = 5
    topn_drawdown: int = 5

    # US needs a universe (practically)
    universe: Optional[Sequence[str]] = None


def _ensure_stocks(market: str, rows: list[dict]) -> dict[str, Stock]:
    """Upsert Stock rows and return {symbol: Stock}."""
    symbols = [r["symbol"] for r in rows]
    existing = Stock.objects.filter(market=market, symbol__in=symbols)
    by_symbol = {s.symbol: s for s in existing}

    to_create: list[Stock] = []
    to_update: list[Stock] = []

    for r in rows:
        sym = r["symbol"]
        if sym in by_symbol:
            obj = by_symbol[sym]
            changed = False
            for f in ("name", "currency", "exchange"):
                v = r.get(f, "") or ""
                if getattr(obj, f) != v:
                    setattr(obj, f, v)
                    changed = True
            if changed:
                to_update.append(obj)
        else:
            to_create.append(
                Stock(
                    market=market,
                    symbol=sym,
                    name=r.get("name") or sym,
                    currency=r.get("currency") or ("KRW" if market == Market.KR else "USD"),
                    exchange=r.get("exchange") or "",
                )
            )

    if to_create:
        Stock.objects.bulk_create(to_create, ignore_conflicts=True)
    if to_update:
        Stock.objects.bulk_update(to_update, fields=["name", "currency", "exchange"])

    refreshed = Stock.objects.filter(market=market, symbol__in=symbols)
    return {s.symbol: s for s in refreshed}


def _intraday_pct(open_price, close_price):
    if open_price is None or close_price is None:
        return None
    try:
        o = float(open_price)
        c = float(close_price)
    except Exception:
        return None
    if o == 0:
        return None
    return (c - o) / o * 100.0


# -----------------------
# KR implementation (pykrx)
# -----------------------

def _fetch_kr_today(asof: date) -> pd.DataFrame:
    """Return df columns: symbol,name,exchange,currency,market_cap,open,close,volume,intraday_pct"""
    from pykrx import stock as krx

    ymd = asof.strftime("%Y%m%d")

    mcap = krx.get_market_cap_by_ticker(ymd)
    ohlcv = krx.get_market_ohlcv_by_ticker(ymd)

    tickers = list(mcap.index)
    names = {t: krx.get_market_ticker_name(t) for t in tickers}

    kospi = set(krx.get_market_ticker_list(ymd, market="KOSPI"))
    kosdaq = set(krx.get_market_ticker_list(ymd, market="KOSDAQ"))
    konex = set(krx.get_market_ticker_list(ymd, market="KONEX"))

    def _ex(sym: str) -> str:
        if sym in kospi:
            return "KOSPI"
        if sym in kosdaq:
            return "KOSDAQ"
        if sym in konex:
            return "KONEX"
        return ""

    df = pd.DataFrame(index=tickers)
    df.index.name = "symbol"

    df["name"] = [names[t] for t in tickers]
    df["market_cap"] = mcap.get("시가총액")

    # ohlcv columns depend on pykrx version; prefer korean names
    # Typically includes: 시가, 고가, 저가, 종가, 거래량
    df["open"] = ohlcv["시가"] if "시가" in ohlcv.columns else None
    df["close"] = ohlcv["종가"] if "종가" in ohlcv.columns else None
    df["volume"] = ohlcv["거래량"] if "거래량" in ohlcv.columns else None

    df["intraday_pct"] = [
        _intraday_pct(df.loc[sym, "open"], df.loc[sym, "close"]) for sym in df.index
    ]

    df["exchange"] = [_ex(sym) for sym in df.index]
    df["currency"] = "KRW"

    return df.reset_index()


# -----------------------
# US implementation (yfinance)
# -----------------------

def _fetch_us_today(asof: date, universe: Sequence[str]) -> pd.DataFrame:
    """Return df columns: symbol,name,exchange,currency,market_cap,open,close,volume,intraday_pct"""
    import yfinance as yf

    # Need a small buffer for holidays/weekends
    start = asof - timedelta(days=7)
    hist = yf.download(
        tickers=list(universe),
        start=start.isoformat(),
        end=(asof + timedelta(days=1)).isoformat(),
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    rows: list[dict] = []

    for sym in universe:
        row = {
            "symbol": sym,
            "name": sym,
            "exchange": "",
            "currency": "USD",
            "market_cap": None,
            "open": None,
            "close": None,
            "volume": None,
            "intraday_pct": None,
        }

        try:
            if isinstance(hist.columns, pd.MultiIndex):
                h = hist[sym].dropna()
            else:
                h = hist.dropna()

            if len(h) >= 1:
                row["open"] = float(h["Open"].iloc[-1]) if "Open" in h.columns else None
                row["close"] = float(h["Close"].iloc[-1]) if "Close" in h.columns else None
                if "Volume" in h.columns and not pd.isna(h["Volume"].iloc[-1]):
                    row["volume"] = int(h["Volume"].iloc[-1])

            row["intraday_pct"] = _intraday_pct(row["open"], row["close"])
        except Exception:
            pass

        try:
            t = yf.Ticker(sym)
            # Try faster info first
            fast = getattr(t, "fast_info", None)
            mc = None
            if fast and hasattr(fast, "get"):
                mc = fast.get("market_cap")
            info = {}
            try:
                info = t.info or {}
            except Exception:
                info = {}

            if mc is None:
                mc = info.get("marketCap")
            if mc is not None:
                row["market_cap"] = int(mc)

            row["name"] = info.get("shortName") or info.get("longName") or row["name"]
            row["exchange"] = info.get("exchange") or row["exchange"]
        except Exception:
            pass

        rows.append(row)

    return pd.DataFrame(rows)


# -----------------------
# Main
# -----------------------

def sync_market_data(opts: SyncOptions) -> dict:
    """
    Fetches today's data, selects:
      - market cap TOP N
      - intraday drawdown (open->close) TOP N (most negative)
    Then overwrites today's rows for that market in DB.

    Notes:
      - Designed to run every 5 minutes (overwrite).
      - For US, universe is required (or set env US_UNIVERSE in command wrapper).
    """
    market = opts.market
    asof = opts.asof

    if market == Market.KR:
        df = _fetch_kr_today(asof)
    elif market == Market.US:
        if not opts.universe or len(opts.universe) == 0:
            raise ValueError("US market requires a universe (e.g., --universe AAPL MSFT NVDA)")
        df = _fetch_us_today(asof, universe=list(opts.universe))
    else:
        raise ValueError("market must be KR or US")

    if df.empty:
        return {"market": market, "asof": asof.isoformat(), "inserted": 0, "message": "no data fetched"}

    # Ensure numeric
    for col in ("market_cap", "open", "close", "intraday_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    top_mcap = (
        df.dropna(subset=["market_cap"]).sort_values("market_cap", ascending=False).head(opts.topn_market_cap)
    )
    # drawdown: 가장 큰 낙폭(가장 음수) 순
    top_dd = (
        df.dropna(subset=["intraday_pct"]).sort_values("intraday_pct", ascending=True).head(opts.topn_drawdown)
    )

    picked = pd.concat([top_mcap, top_dd], axis=0).drop_duplicates(subset=["symbol"], keep="first")

    stock_rows: list[dict] = []
    for _, r in picked.iterrows():
        stock_rows.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r.get("name") or r["symbol"]),
                "currency": str(r.get("currency") or ("KRW" if market == Market.KR else "USD")),
                "exchange": str(r.get("exchange") or ""),
            }
        )

    inserted = 0

    with transaction.atomic():
        stocks = _ensure_stocks(market=market, rows=stock_rows)

        if opts.overwrite_today:
            DailyStockSnapshot.objects.filter(stock__market=market, date=asof).delete()

        snap_objs: list[DailyStockSnapshot] = []
        for _, r in picked.iterrows():
            sym = str(r["symbol"])
            st = stocks.get(sym)
            if not st:
                continue

            open_p = r.get("open")
            close_p = r.get("close")
            intraday = r.get("intraday_pct")
            mcap = r.get("market_cap")
            vol = r.get("volume")

            snap_objs.append(
                DailyStockSnapshot(
                    stock=st,
                    date=asof,
                    open=None if pd.isna(open_p) else open_p,
                    close=None if pd.isna(close_p) else close_p,
                    # keep compatibility: store intraday% in both fields
                    change_pct=None if pd.isna(intraday) else intraday,
                    intraday_pct=None if pd.isna(intraday) else intraday,
                    market_cap=None if pd.isna(mcap) else int(mcap),
                    volume=None if pd.isna(vol) else int(vol) if vol is not None else None,
                )
            )

        if snap_objs:
            DailyStockSnapshot.objects.bulk_create(snap_objs)
            inserted = len(snap_objs)

    return {
        "market": market,
        "asof": asof.isoformat(),
        "inserted": inserted,
        "picked_symbols": picked["symbol"].astype(str).tolist(),
        "top_market_cap": top_mcap["symbol"].astype(str).tolist(),
        "top_drawdown": top_dd["symbol"].astype(str).tolist(),
    }
