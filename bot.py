"""
FastAPI Trade Bot — MetaTrader5 Entegrasyonu
---------------------------------------------
Bu bot ccxt yerine MetaTrader5 python paketini kullanır ve emirleri
DOĞRUDAN bilgisayarınızda çalışan MetaTrader 5 terminali üzerinden açar.

ÖNEMLİ KISITLAMALAR (lütfen okuyun):
- MetaTrader5 paketi SADECE Windows'ta çalışır (resmi olarak desteklenen
  tek platform budur).
- Bu script'in çalışabilmesi için bilgisayarınızda MetaTrader 5 terminali
  KURULU ve genelde AÇIK/login olmuş halde olmalıdır (mt5.initialize()
  çoğu zaman zaten açık bir terminale bağlanır; MT5_LOGIN/PASSWORD/SERVER
  verirseniz bot login işlemini kendisi de yapabilir).
- Bu yüzden bot'u Render gibi bir Linux bulut sunucusunda ÇALIŞTIRAMAZSINIZ.
  Kendi Windows makinenizde ya da MT5 kurulu bir Windows VPS'te çalıştırın.
- Dışarıdan (örn. TradingView) webhook alabilmek için yerelde çalışan bu
  FastAPI sunucusunu bir tünelleme aracıyla (ngrok, Cloudflare Tunnel vb.)
  internete açmanız gerekir.
- MT5 sembolleri ccxt'ten farklıdır: "BTC/USDT" değil "EURUSD", "XAUUSD",
  "BTCUSD" gibi genelde "/" içermeyen isimler kullanılır. Kullandığınız
  brokerin sembol isimlerini MT5 terminalinizdeki Market Watch'tan kontrol edin.

Rotalar öncekiyle aynı kaldı:
- /webhook             : Webhook sinyali (TradingView vb.)
- /api/order           : Manuel panelden emir
- /api/balance         : Hesap özeti (bakiye/equity/margin)
- /api/positions       : Açık pozisyonlar
- /api/open-orders     : Bekleyen (pending) emirler
- /api/ticker/{symbol} : Anlık bid/ask
- /api/orders/{symbol} : (DELETE) sembole ait bekleyen emirleri iptal eder
- /                     : index.html panelini sunar

TP/SL hesaplama aynı mantıkla çalışır:
- "percent" : giriş fiyatına göre yüzde
- "price"   : giriş fiyatına eklenip çıkarılacak mutlak fiyat farkı
MT5'te TP/SL, ccxt'teki gibi ayrı emirler değil — aynı order_send() isteği
içinde "sl" ve "tp" alanlarıyla gönderilir; broker bunları pozisyona bağlı
olarak otomatik yönetir.
"""

import os
import threading
import logging
from typing import Optional, Literal, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from dotenv import load_dotenv

import MetaTrader5 as mt5

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("trade-bot")

# ---------------------------------------------------------------------------
# Ayarlar (ortam değişkenlerinden okunur)
# ---------------------------------------------------------------------------
MT5_PATH = os.getenv("MT5_PATH", "")  # örn: C:\Program Files\MetaTrader 5\terminal64.exe (boş bırakılabilir)
MT5_LOGIN = int(os.getenv("MT5_LOGIN")) if os.getenv("MT5_LOGIN") else None
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER = os.getenv("MT5_SERVER", "")
MT5_TIMEOUT_MS = int(os.getenv("MT5_TIMEOUT_MS", "10000"))

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DEFAULT_VOLUME = float(os.getenv("DEFAULT_VOLUME", "0.01"))       # lot
DEFAULT_DEVIATION = int(os.getenv("DEFAULT_DEVIATION", "20"))     # slippage (points)
DEFAULT_MAGIC = int(os.getenv("DEFAULT_MAGIC", "234000"))

_FILLING_MAP = {
    "IOC": mt5.ORDER_FILLING_IOC,
    "FOK": mt5.ORDER_FILLING_FOK,
    "RETURN": mt5.ORDER_FILLING_RETURN,
}
MT5_FILLING = _FILLING_MAP.get(os.getenv("MT5_FILLING_TYPE", "IOC").upper(), mt5.ORDER_FILLING_IOC)

app = FastAPI(title="Trade Bot (MetaTrader5)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MetaTrader5 modülü tek bir global terminal bağlantısı üzerinden çalışır ve
# eşzamanlı çağrılarda güvenli değildir; bu yüzden tüm mt5.* çağrılarını bu
# lock ile seri hale getiriyoruz.
_mt5_lock = threading.Lock()


def _ensure_initialized():
    """MT5 terminaline bağlantının açık olduğundan emin olur."""
    if mt5.terminal_info() is not None:
        return
    ok = mt5.initialize(
        path=MT5_PATH or None,
        login=MT5_LOGIN,
        password=MT5_PASSWORD or None,
        server=MT5_SERVER or None,
        timeout=MT5_TIMEOUT_MS,
    )
    if not ok:
        raise RuntimeError(f"MT5 initialize başarısız: {mt5.last_error()}")


@app.on_event("startup")
async def startup_event():
    try:
        await run_in_threadpool(_ensure_initialized)
        logger.info("MT5 terminaline bağlanıldı.")
    except Exception as e:
        # Terminal henüz açık değilse bile sunucu ayağa kalksın; her istekte
        # tekrar bağlanmayı dener.
        logger.warning(f"Başlangıçta MT5'e bağlanılamadı, istekler geldikçe tekrar denenecek: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    await run_in_threadpool(mt5.shutdown)


# ---------------------------------------------------------------------------
# Modeller
# ---------------------------------------------------------------------------
class OrderRequest(BaseModel):
    symbol: str                                     # örn. "EURUSD", "XAUUSD"
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"] = "market"
    volume: float                                    # lot miktarı (örn. 0.01)
    price: Optional[float] = None                    # limit emir için fiyat
    tp_mode: Literal["percent", "price", "none"] = "none"
    tp_value: Optional[float] = None
    sl_mode: Literal["percent", "price", "none"] = "none"
    sl_value: Optional[float] = None


class WebhookSignal(BaseModel):
    secret: str
    symbol: str
    side: Literal["buy", "sell", "close"]
    order_type: Literal["market", "limit"] = "market"
    volume: Optional[float] = None                   # boşsa DEFAULT_VOLUME kullanılır
    price: Optional[float] = None
    tp_mode: Literal["percent", "price", "none"] = "none"
    tp_value: Optional[float] = None
    sl_mode: Literal["percent", "price", "none"] = "none"
    sl_value: Optional[float] = None


# ---------------------------------------------------------------------------
# TP / SL hesaplama (borsa/terminalden bağımsız, saf hesap)
# ---------------------------------------------------------------------------
def calculate_tp_sl_prices(
    entry_price: float,
    side: str,
    tp_mode: str,
    tp_value: Optional[float],
    sl_mode: str,
    sl_value: Optional[float],
):
    is_long = side == "buy"
    tp_price = None
    sl_price = None

    if tp_mode == "percent" and tp_value is not None:
        tp_price = entry_price * (1 + tp_value / 100) if is_long else entry_price * (1 - tp_value / 100)
    elif tp_mode == "price" and tp_value is not None:
        tp_price = entry_price + tp_value if is_long else entry_price - tp_value

    if sl_mode == "percent" and sl_value is not None:
        sl_price = entry_price * (1 - sl_value / 100) if is_long else entry_price * (1 + sl_value / 100)
    elif sl_mode == "price" and sl_value is not None:
        sl_price = entry_price - sl_value if is_long else entry_price + sl_value

    return tp_price, sl_price


# ---------------------------------------------------------------------------
# MT5 ile konuşan senkron yardımcı fonksiyonlar (threadpool içinde çalışır)
# ---------------------------------------------------------------------------
def _place_order_sync(req: OrderRequest, comment: str = "trade-bot") -> dict:
    with _mt5_lock:
        _ensure_initialized()

        symbol = req.symbol
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Sembol seçilemedi/bulunamadı: {symbol}")

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"Fiyat alınamadı: {symbol}")

        is_buy = req.side == "buy"

        if req.order_type == "market":
            action = mt5.TRADE_ACTION_DEAL
            order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
            price = tick.ask if is_buy else tick.bid
        else:
            if not req.price:
                raise RuntimeError("Limit emir için 'price' alanı zorunludur")
            action = mt5.TRADE_ACTION_PENDING
            order_type = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT
            price = req.price

        tp_price, sl_price = calculate_tp_sl_prices(
            price, req.side, req.tp_mode, req.tp_value, req.sl_mode, req.sl_value
        )

        symbol_info = mt5.symbol_info(symbol)
        digits = symbol_info.digits if symbol_info else 5
        if tp_price is not None:
            tp_price = round(tp_price, digits)
        if sl_price is not None:
            sl_price = round(sl_price, digits)

        request = {
            "action": action,
            "symbol": symbol,
            "volume": req.volume,
            "type": order_type,
            "price": price,
            "deviation": DEFAULT_DEVIATION,
            "magic": DEFAULT_MAGIC,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": MT5_FILLING,
        }
        if sl_price is not None:
            request["sl"] = sl_price
        if tp_price is not None:
            request["tp"] = tp_price

        result = mt5.order_send(request)
        if result is None:
            raise RuntimeError(f"order_send None döndü: {mt5.last_error()}")
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            raise RuntimeError(f"Emir reddedildi: retcode={result.retcode} comment={result.comment}")

        return {
            "order": result._asdict(),
            "entry_price": price,
            "tp_price": tp_price,
            "sl_price": sl_price,
        }


def _close_symbol_sync(symbol: str) -> dict:
    with _mt5_lock:
        _ensure_initialized()

        positions = mt5.positions_get(symbol=symbol) or []
        closed = []
        for pos in positions:
            tick = mt5.symbol_info_tick(pos.symbol)
            if tick is None:
                logger.error(f"Fiyat alınamadı, pozisyon kapatılamadı: {pos.symbol}")
                continue
            is_buy_pos = pos.type == mt5.POSITION_TYPE_BUY
            close_type = mt5.ORDER_TYPE_SELL if is_buy_pos else mt5.ORDER_TYPE_BUY
            price = tick.bid if is_buy_pos else tick.ask

            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": pos.symbol,
                "volume": pos.volume,
                "type": close_type,
                "position": pos.ticket,
                "price": price,
                "deviation": DEFAULT_DEVIATION,
                "magic": DEFAULT_MAGIC,
                "comment": "trade-bot close",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": MT5_FILLING,
            }
            result = mt5.order_send(request)
            if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
                logger.error(f"Pozisyon kapatılamadı ticket={pos.ticket}: {result}")
                continue
            closed.append(result._asdict())

        pending = mt5.orders_get(symbol=symbol) or []
        cancelled = []
        for o in pending:
            r = mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": o.ticket})
            if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                cancelled.append(o.ticket)

        return {"closed_positions": closed, "cancelled_orders": cancelled}


def _cancel_pending_sync(symbol: str) -> List[int]:
    with _mt5_lock:
        _ensure_initialized()
        pending = mt5.orders_get(symbol=symbol) or []
        cancelled = []
        for o in pending:
            r = mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": o.ticket})
            if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                cancelled.append(o.ticket)
        return cancelled


def _account_summary_sync() -> dict:
    with _mt5_lock:
        _ensure_initialized()
        info = mt5.account_info()
        if info is None:
            raise RuntimeError(f"Hesap bilgisi alınamadı: {mt5.last_error()}")
        d = info._asdict()
        return {
            "name": d.get("name"),
            "server": d.get("server"),
            "currency": d.get("currency"),
            "leverage": d.get("leverage"),
            "balance": d.get("balance"),
            "equity": d.get("equity"),
            "margin": d.get("margin"),
            "margin_free": d.get("margin_free"),
            "profit": d.get("profit"),
        }


def _positions_sync(symbol: Optional[str] = None) -> list:
    with _mt5_lock:
        _ensure_initialized()
        positions = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
        return [p._asdict() for p in (positions or [])]


def _pending_orders_sync(symbol: Optional[str] = None) -> list:
    with _mt5_lock:
        _ensure_initialized()
        orders = mt5.orders_get(symbol=symbol) if symbol else mt5.orders_get()
        return [o._asdict() for o in (orders or [])]


def _ticker_sync(symbol: str) -> dict:
    with _mt5_lock:
        _ensure_initialized()
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Sembol seçilemedi: {symbol}")
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"Fiyat alınamadı: {symbol}")
        return tick._asdict()


# ---------------------------------------------------------------------------
# Rotalar
# ---------------------------------------------------------------------------
@app.get("/")
async def serve_panel():
    return FileResponse("index.html")


@app.get("/api/health")
async def health():
    try:
        await run_in_threadpool(_ensure_initialized)
        connected = True
    except Exception:
        connected = False
    return {"status": "ok" if connected else "mt5_disconnected", "mt5_connected": connected}


@app.get("/api/balance")
async def balance():
    try:
        return await run_in_threadpool(_account_summary_sync)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/ticker/{symbol}")
async def ticker(symbol: str):
    try:
        return await run_in_threadpool(_ticker_sync, symbol)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/positions")
async def positions(symbol: Optional[str] = None):
    try:
        return await run_in_threadpool(_positions_sync, symbol)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/open-orders")
async def open_orders(symbol: Optional[str] = None):
    try:
        return await run_in_threadpool(_pending_orders_sync, symbol)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/orders/{symbol}")
async def cancel_all(symbol: str):
    try:
        cancelled = await run_in_threadpool(_cancel_pending_sync, symbol)
        return {"cancelled": cancelled}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/order")
async def manual_order(req: OrderRequest):
    try:
        return await run_in_threadpool(_place_order_sync, req, "manual-panel")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Manuel emir hatası")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/webhook")
async def webhook(signal: WebhookSignal):
    if not WEBHOOK_SECRET or signal.secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Geçersiz webhook secret")

    if signal.side == "close":
        try:
            result = await run_in_threadpool(_close_symbol_sync, signal.symbol)
            return {"status": "closed", **result}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    req = OrderRequest(
        symbol=signal.symbol,
        side=signal.side,
        order_type=signal.order_type,
        volume=signal.volume or DEFAULT_VOLUME,
        price=signal.price,
        tp_mode=signal.tp_mode,
        tp_value=signal.tp_value,
        sl_mode=signal.sl_mode,
        sl_value=signal.sl_value,
    )
    try:
        result = await run_in_threadpool(_place_order_sync, req, "webhook")
        return {"status": "executed", **result}
    except Exception as e:
        logger.exception("Webhook emir hatası")
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("bot:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
