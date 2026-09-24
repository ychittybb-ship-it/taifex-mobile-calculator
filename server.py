import os
import json
import asyncio
import threading
import logging

from flask import Flask, send_file, jsonify
import websockets

# =========================
# 基本設定
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

KEY = os.getenv("FUGLE_API_KEY", "").strip()
PORT = int(os.getenv("PORT", "8080"))

app = Flask(__name__)

state = {
    "price": None,
    "symbol": "TXF1!",
    "connected": False,
    "authenticated": False,
    "subscribed": False,
    "last_update": None,
    "error": ""
}

URL = "wss://api.fugle.tw/marketdata/v1.1/futopt/streaming"


# =========================
# Fugle WebSocket
# =========================

async def market_loop():

    while True:

        if not KEY:
            state["connected"] = False
            state["authenticated"] = False
            state["subscribed"] = False
            state["error"] = "尚未設定 FUGLE_API_KEY"

            logging.error("❌ 尚未設定 FUGLE_API_KEY")

            await asyncio.sleep(5)
            continue

        try:

            logging.info("🔌 正在連線 Fugle WebSocket...")

            async with websockets.connect(
                URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                state["connected"] = True
                state["authenticated"] = False
                state["subscribed"] = False
                state["error"] = ""

                logging.info("✅ WebSocket 已連線")

                # -------------------------
                # API Key 驗證
                # -------------------------

                auth_message = {
                    "event": "auth",
                    "data": {
                        "apikey": KEY
                    }
                }

                await ws.send(json.dumps(auth_message))

                logging.info("🔑 已送出 API Key 驗證")

                # -------------------------
                # 持續接收 Fugle 訊息
                # -------------------------

                async for raw in ws:

                    logging.info("📩 Fugle 回應：%s", raw[:1000])

                    try:
                        message = json.loads(raw)

                    except Exception as e:

                        state["error"] = f"JSON解析錯誤：{e}"

                        logging.error(
                            "❌ JSON解析錯誤：%s",
                            e
                        )

                        continue

                    event = message.get("event")
                    data = message.get("data", {})

                    # =====================
                    # API 驗證成功
                    # =====================

                    if event == "authenticated":

                        state["authenticated"] = True
                        state["error"] = ""

                        logging.info(
                            "✅ Fugle API 驗證成功"
                        )

                        # ---------------------
                        # 訂閱台指期近月
                        # ---------------------

                        subscribe_message = {
                            "event": "subscribe",
                            "data": {
                                "channel": "trades",
                                "symbol": "TXF1!"
                            }
                        }

                        await ws.send(
                            json.dumps(subscribe_message)
                        )

                        logging.info(
                            "📡 已送出 TXF1! 行情訂閱"
                        )

                    # =====================
                    # 訂閱成功
                    # =====================

                    elif event == "subscribed":

                        state["subscribed"] = True

                        logging.info(
                            "✅ TXF1! 訂閱成功"
                        )

                    # =====================
                    # Fugle 錯誤
                    # =====================

                    elif event == "error":

                        error_message = (
                            data.get("message")
                            if isinstance(data, dict)
                            else str(data)
                        )

                        state["error"] = error_message

                        logging.error(
                            "❌ Fugle 回傳錯誤：%s",
                            error_message
                        )

                    # =====================
                    # 行情資料
                    # =====================

                    if isinstance(data, dict):

                        trades = data.get("trades", [])

                        if trades:

                            trade = trades[0]

                            # 排除試用資料
                            if trade.get("isTrial", False):
                                continue

                            price = trade.get("price")

                            if price is not None:

                                state["price"] = float(price)

                                state["last_update"] = (
                                    trade.get("timestamp")
                                )

                                state["symbol"] = (
                                    data.get(
                                        "symbol",
                                        "TXF1!"
                                    )
                                )

                                logging.info(
                                    "💰 台指期價格：%s",
                                    state["price"]
                                )

        except Exception as e:

            state["connected"] = False
            state["authenticated"] = False
            state["subscribed"] = False
            state["error"] = str(e)[:300]

            logging.exception(
                "❌ Fugle WebSocket 發生錯誤"
            )

            await asyncio.sleep(5)


# =========================
# 網頁
# =========================

@app.get("/")
def home():
    return send_file("index.html")


# =========================
# 行情 API
# =========================

@app.get("/api/quote")
def quote():

    return jsonify(state)


# =========================
# 健康檢查
# =========================

@app.get("/health")
def health():

    return jsonify({
        "ok": True,
        "market_connected": state["connected"],
        "authenticated": state["authenticated"],
        "subscribed": state["subscribed"],
        "price": state["price"],
        "error": state["error"]
    })


# =========================
# 啟動
# =========================

if __name__ == "__main__":

    threading.Thread(
        target=lambda: asyncio.run(market_loop()),
        daemon=True
    ).start()

    app.run(
        host="0.0.0.0",
        port=PORT
    )
