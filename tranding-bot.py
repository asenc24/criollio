import os
import logging
import json
import sqlite3
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta  # noqa: F401
import asyncio
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from openai import AsyncOpenAI
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)

# ==========================================
# 1. CONFIGURACIÓN Y ESTADO CENTRAL
# ==========================================
class BotConfig:
    def __init__(self):
        load_dotenv()
        self.TOKEN = os.environ.get("TELEGRAM_TOKEN")
        self.GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
        self.NEWS_KEY = os.environ.get("NEWS_API_KEY")
        self.ADMIN_ID = int(os.environ.get("ADMIN_TELEGRAM_ID", 0))
        self.CAPITAL = float(os.environ.get("CAPITAL_USDT", 1000.0))
        
        # Estados dinámicos (modificables desde el panel)
        self.riesgo_pct = float(os.environ.get("RIESGO_PCT", 1.0))
        self.whale_threshold = 4.0
        self.is_paused = False
        
        self.db_name = "trading_bot.db"
        self.watchlist = [
            "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", 
            "XRP/USDT", "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT"
        ]

# ==========================================
# 2. GESTOR DE BASE DE DATOS (SQLITE)
# ==========================================
class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp INTEGER,
                action TEXT,
                entry_price REAL,
                stop_loss REAL,
                take_profit REAL,
                status TEXT DEFAULT 'PENDING'
            )''')
            try: conn.execute("ALTER TABLE trades ADD COLUMN tipo TEXT")
            except Exception: pass
            try: conn.execute("ALTER TABLE trades ADD COLUMN puntaje INTEGER")
            except Exception: pass

    def existe_trade_pendiente(self, symbol):
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.execute("SELECT 1 FROM trades WHERE symbol = ? AND status = 'PENDING'", (symbol,))
            return cur.fetchone() is not None

    def registrar_trade(self, symbol, timestamp, action, entry, sl, tp, tipo, puntaje):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute(
                "INSERT INTO trades (symbol, timestamp, action, entry_price, stop_loss, take_profit, tipo, puntaje) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (symbol, timestamp, action, float(entry), float(sl), float(tp), tipo, puntaje)
            )

    def obtener_pendientes(self):
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.execute("SELECT id, symbol, timestamp, action, entry_price, stop_loss, take_profit FROM trades WHERE status = 'PENDING'")
            return cur.fetchall()

    def actualizar_estado(self, trade_id, status):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute("UPDATE trades SET status = ? WHERE id = ?", (status, trade_id))

    def obtener_estadisticas(self):
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.execute("SELECT status, COUNT(*) FROM trades WHERE status != 'PENDING' GROUP BY status")
            res = dict(cur.fetchall())
            wins, losses, expired = res.get('WIN', 0), res.get('LOSS', 0), res.get('EXPIRED', 0)
            tot = wins + losses + expired
            efectivos = wins + losses
            rate = (wins / efectivos * 100) if efectivos > 0 else 0
            return wins, losses, expired, tot, rate

    def obtener_feedback_ia(self, symbol):
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.execute("SELECT status FROM trades WHERE symbol = ? AND status IN ('WIN', 'LOSS') ORDER BY timestamp DESC LIMIT 3", (symbol,))
            res = [r[0] for r in cur.fetchall()]
        if not res: return "Sin historial reciente."
        wins, losses = res.count('WIN'), res.count('LOSS')
        if losses > wins: return f"Tus últimos trades en {symbol} fallaron. Sé EXTREMADAMENTE estricto."
        elif wins > losses: return f"Tus últimos trades en {symbol} ganaron. Buen análisis, mantén el criterio."
        return "Resultados recientes mixtos."

# ==========================================
# 3. MOTOR DE DATOS DEL MERCADO Y BALLENAS
# ==========================================
class MarketData:
    def __init__(self, config: BotConfig):
        self.cfg = config
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.news_cache = {}

    async def obtener_datos_completos(self, symbol: str):
        try:
            v5m = await self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=100)
            v15m = await self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=100)
            v30m = await self.exchange.fetch_ohlcv(symbol, timeframe='30m', limit=100)
            
            i5m = self._calcular_indicadores(v5m)
            i15m = self._calcular_indicadores(v15m)
            i30m = self._calcular_indicadores(v30m)
            
            if not i5m or not i15m or not i30m: return None
            return {"5m": i5m, "15m": i15m, "30m": i30m}
        except Exception as e:
            logging.error(f"CCXT Error: {e}")
            return None

    def _calcular_indicadores(self, ohlcv):
        if not ohlcv or len(ohlcv) < 50: return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.ta.rsi(length=14, append=True)
        df.ta.ema(length=20, append=True)
        df.ta.ema(length=50, append=True)
        df['vol_sma'] = df['volume'].rolling(window=20).mean()
        df.dropna(inplace=True)
        if df.empty: return None

        last = df.iloc[-1]
        ratio_vol = round(last['volume'] / last['vol_sma'], 2) if last['vol_sma'] > 0 else 1.0
        dir_vela = "COMPRANDO 🟢 (Alcista)" if last['close'] > last['open'] else "VENDIENDO 🔴 (Bajista)"

        return {
            "precio_actual": last['close'],
            "rsi_14": round(last['RSI_14'], 2),
            "ema_20": round(last['EMA_20'], 2),
            "ema_50": round(last['EMA_50'], 2),
            "ratio_volumen": ratio_vol,
            "direccion_ballena": dir_vela
        }

    async def obtener_noticias(self, symbol: str):
        query = symbol.split('/')[0]
        ahora = int(datetime.now(UTC).timestamp())
        if query in self.news_cache and (ahora - self.news_cache[query]["ts"]) < (4 * 3600):
            return self.news_cache[query]["data"]

        hoy = datetime.now(UTC).strftime('%Y-%m-%d')
        ayer = (datetime.now(UTC) - timedelta(days=1)).strftime('%Y-%m-%d')
        url = f"https://newsapi.org/v2/everything?q={query}&from={ayer}&to={hoy}&sortBy=popularity&apiKey={self.cfg.NEWS_KEY}"

        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    arts = [{"titulo": a["title"]} for a in res.json().get("articles", [])[:3]]
                    self.news_cache[query] = {"ts": ahora, "data": arts}
                    return arts
            except Exception: pass
        return []

    def calcular_posicion(self, entrada, stop_loss):
        try:
            riesgo_usd = self.cfg.CAPITAL * (self.cfg.riesgo_pct / 100.0)
            dist_pct = abs(entrada - stop_loss) / entrada
            if dist_pct == 0: return 0, 0
            tam_usd = riesgo_usd / dist_pct
            apalancamiento = tam_usd / self.cfg.CAPITAL
            return round(tam_usd, 2), round(apalancamiento, 1)
        except Exception:
            return 0, 0

# ==========================================
# 4. CEREBRO DE INTELIGENCIA ARTIFICIAL
# ==========================================
class AIBrain:
    def __init__(self, config: BotConfig, db: DatabaseManager):
        self.cfg = config
        self.db = db
        self.client = AsyncOpenAI(api_key=self.cfg.GEMINI_KEY, base_url="https://generativelanguage.googleapis.com/v1beta/openai/")

    async def analizar(self, symbol, ind, notis):
        feedback = self.db.obtener_feedback_ia(symbol)
        datos = json.dumps({"indicadores": ind, "noticias": notis})
        
        prompt = (
            f"Analiza el par {symbol}.\nDatos: {datos}\nFeedback Histórico: {feedback}\n\n"
            "Eres un Francotirador Institucional operando en MODO TURBO (Micro-Scalping).\n"
            "Ignora el largo plazo. Tu ÚNICO objetivo es cazar movimientos explosivos y rápidos.\n\n"
            "REGLAS ESTRICTAS:\n"
            "1. Prioriza EXCLUSIVAMENTE entradas de 'SCALPING' puro.\n"
            "2. El Stop Loss debe ser hiper-ajustado (muy corto, por debajo de la vela de volumen o soporte local).\n"
            "3. Si el mercado está muy lateral y sin volumen, pon 'hay_oportunidad' en false.\n"
            "4. Califica tu confianza del 1 al 10 (Puntaje de Confluencia).\n\n"
            "Devuelve un JSON EXACTO:\n"
            "{\n"
            '  "hay_oportunidad": true/false,\n'
            '  "tipo": "SCALPING",\n'
            '  "puntaje": 8,\n'
            '  "recomendacion": "COMPRAR" | "VENDER",\n'
            '  "entrada": 00.0,\n'
            '  "stop_loss": 00.0,\n'
            '  "take_profit": 00.0,\n'
            '  "justificacion": "Breve explicación de la explosión esperada"\n'
            "}"
        )
        try:
            res = await self.client.chat.completions.create(
                model="gemini-3.8-flash",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            return json.loads(res.choices[0].message.content)
        except Exception as e:
            logging.error(f"AI Error: {e}")
            return None

# ==========================================
# 5. MOTOR DE EJECUCIÓN (ESCÁNER Y AUDITOR)
# ==========================================
class TradingEngine:
    def __init__(self, cfg: BotConfig, db: DatabaseManager, market: MarketData, brain: AIBrain):
        self.cfg = cfg
        self.db = db
        self.market = market
        self.brain = brain
        self.telegram_app = None

    async def escanear_mercado(self, chat_id, manual=False):
        if self.cfg.is_paused and not manual: return

        if manual:
            await self.telegram_app.bot.send_message(
                chat_id, "⏳ *Cargando visión microscópica (5m, 15m, 30m)...*", parse_mode="Markdown"
            )

        oportunidades = 0
        ahora = int(self.market.exchange.milliseconds())

        for symbol in self.cfg.watchlist:
            if self.db.existe_trade_pendiente(symbol):
                if manual: await self.telegram_app.bot.send_message(chat_id, f"ℹ️ `{symbol}` omitido (Operación ya activa).", parse_mode="Markdown")
                continue

            ind = await self.market.obtener_datos_completos(symbol)
            if not ind: continue

            # Alertas de Ballena Inmediatas (Ahora en velas de 5 minutos para hiper-velocidad)
            ratio = ind["5m"]["ratio_volumen"]
            if ratio >= self.cfg.whale_threshold:
                dir_ballena = ind["5m"]["direccion_ballena"]
                emoji = "🐳" if "COMPRANDO" in dir_ballena else "🐋"
                await self.telegram_app.bot.send_message(chat_id, (
                    f"{emoji} *¡ALERTA DE BALLENA (MICRO): {symbol}!* {emoji}\n\n"
                    f"*ACCIÓN:* Instituciones *{dir_ballena}*\n"
                    f"*VOLUMEN (5m):* `{ratio}x` mayor a lo normal.\n"
                    f"*PRECIO:* `{ind['5m']['precio_actual']}`\n\n"
                    f"⚠️ _Anticipando inyección rápida de volatilidad._"
                ), parse_mode="Markdown")

            noti = await self.market.obtener_noticias(symbol)
            analisis = await self.brain.analizar(symbol, ind, noti)
            if not analisis: continue

            if analisis.get("hay_oportunidad") and analisis.get("recomendacion") in ["COMPRAR", "VENDER"]:
                oportunidades += 1
                rec = analisis["recomendacion"]
                tipo = analisis.get("tipo", "SCALPING")
                puntos = int(analisis.get("puntaje", 5))
                ent = float(analisis.get("entrada", 0))
                sl = float(analisis.get("stop_loss", 0))
                tp = float(analisis.get("take_profit", 0))
                just = analisis.get("justificacion", "")
                
                tam_usd, apal = self.market.calcular_posicion(ent, sl)
                dist_sl = abs(ent - sl)
                dist_tp = abs(ent - tp)
                ratio_rr = (dist_tp / dist_sl) if dist_sl > 0 else 0

                try:
                    self.db.registrar_trade(symbol, ahora, rec, ent, sl, tp, tipo, puntos)
                except Exception as e: logging.error(e)

                emoji_dir = "🟢 LONG (COMPRA)" if rec == "COMPRAR" else "🔴 SHORT (VENTA)"
                estrellas = "⭐" * (puntos // 2) + ("✨" if puntos % 2 != 0 else "")

                texto = (
                    f"🚨 *NUEVO MICRO-SCALP: {symbol}* 🚨\n"
                    f"Confluencia: {estrellas} ({puntos}/10)\n\n"
                    f"Dirección: *{emoji_dir}*\n"
                    f"Ratio R:R: `1:{ratio_rr:.1f}`\n\n"
                    f"🎯 *Entrada:* `{ent}`\n"
                    f"🛡️ *Stop-Loss:* `{sl}`\n"
                    f"🤑 *Take-Profit:* `{tp}`\n\n"
                    f"💰 *Posición Sugerida:* `${tam_usd} USDT`\n"
                    f"_(Riesgo: {self.cfg.riesgo_pct}% | Apalancamiento ref: {apal}x)_\n\n"
                    f"💡 *Análisis:* _{just}_"
                )
                await self.telegram_app.bot.send_message(chat_id, texto, parse_mode="Markdown")
            await asyncio.sleep(2)

        if manual and oportunidades == 0:
            await self.telegram_app.bot.send_message(chat_id, "📉 El mercado está muy lento para el Modo Turbo. No hay explosiones inminentes.", parse_mode="Markdown")

    async def auditar_trades(self):
        pendientes = self.db.obtener_pendientes()
        if not pendientes: return
        ahora = int(self.market.exchange.milliseconds())
        
        for t in pendientes:
            tid, sym, ts_open, action, ent, sl, tp = t
            
            # Expiración a la 1 HORA (Micro-scalp vencido)
            if ahora - ts_open > (1 * 3600 * 1000):
                self.db.actualizar_estado(tid, 'EXPIRED')
                await self.telegram_app.bot.send_message(self.cfg.ADMIN_ID, f"⏱ *SCALP EXPIRADO (1H)*\nSe cierra {action} en {sym} por falta de impulso.", parse_mode="Markdown")
                continue

            try:
                # Rastreamos con velas de 5 minutos para máxima precisión rápida
                velas = await self.market.exchange.fetch_ohlcv(sym, timeframe='5m', since=ts_open)
                for v in velas:
                    t_v, high, low = v[0], v[2], v[3]
                    h_sl = (low <= sl) if action == "COMPRAR" else (high >= sl)
                    h_tp = (high >= tp) if action == "COMPRAR" else (low <= tp)

                    if h_sl and h_tp:
                        v1m = await self.market.exchange.fetch_ohlcv(sym, timeframe='1m', since=t_v, limit=15)
                        resuelto = False
                        for m in v1m:
                            mh, ml = m[2], m[3]
                            m_sl = (ml <= sl) if action == "COMPRAR" else (mh >= sl)
                            m_tp = (mh >= tp) if action == "COMPRAR" else (ml <= tp)
                            if m_sl:
                                self.db.actualizar_estado(tid, 'LOSS')
                                await self.telegram_app.bot.send_message(self.cfg.ADMIN_ID, f"❌ *PÉRDIDA* (SL: `{sl}`) en {action} {sym}", parse_mode="Markdown")
                                resuelto = True; break
                            elif m_tp:
                                self.db.actualizar_estado(tid, 'WIN')
                                await self.telegram_app.bot.send_message(self.cfg.ADMIN_ID, f"✅ *GANANCIA* (TP: `{tp}`) en {action} {sym}", parse_mode="Markdown")
                                resuelto = True; break
                        if not resuelto:
                            self.db.actualizar_estado(tid, 'LOSS')
                        break
                    elif h_sl:
                        self.db.actualizar_estado(tid, 'LOSS')
                        await self.telegram_app.bot.send_message(self.cfg.ADMIN_ID, f"❌ *PÉRDIDA* (SL: `{sl}`) en {action} {sym}", parse_mode="Markdown")
                        break
                    elif h_tp:
                        self.db.actualizar_estado(tid, 'WIN')
                        await self.telegram_app.bot.send_message(self.cfg.ADMIN_ID, f"✅ *GANANCIA* (TP: `{tp}`) en {action} {sym}", parse_mode="Markdown")
                        break
            except Exception as e: logging.error(e)
            await asyncio.sleep(1)

# ==========================================
# 6. INTERFAZ TELEGRAM Y PANEL DE CONTROL
# ==========================================
class TelegramUI:
    def __init__(self, cfg: BotConfig, db: DatabaseManager, engine: TradingEngine):
        self.cfg = cfg
        self.db = db
        self.engine = engine
        self.app = ApplicationBuilder().token(self.cfg.TOKEN).build()
        self.engine.telegram_app = self.app

        # Bind Commands
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("panel", self.cmd_start))
        self.app.add_handler(CallbackQueryHandler(self.handle_callbacks))
        
        # Modo Turbo: Escáner cada 5 min (300s), Auditor cada 2 min (120s)
        self.app.job_queue.run_repeating(self.job_scanner, interval=300, first=10)
        self.app.job_queue.run_repeating(self.job_auditor, interval=120, first=30)

    async def job_scanner(self, ctx: ContextTypes.DEFAULT_TYPE): await self.engine.escanear_mercado(self.cfg.ADMIN_ID, manual=False)
    async def job_auditor(self, ctx: ContextTypes.DEFAULT_TYPE): await self.engine.auditar_trades()

    def get_main_keyboard(self):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Escanear Mercado", callback_data="scan_all")],
            [InlineKeyboardButton("📊 Mi Rendimiento", callback_data="view_stats"),
             InlineKeyboardButton("📜 Trades Activos", callback_data="view_active")],
            [InlineKeyboardButton("⚙️ Configuración del Bot", callback_data="config_menu")]
        ])

    def get_config_keyboard(self):
        state_btn = "▶️ Reanudar Bot" if self.cfg.is_paused else "⏸️ Pausar Auto-Escáner"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"Riesgo: {self.cfg.riesgo_pct}% ➖", callback_data="risk_down"),
             InlineKeyboardButton(f"Riesgo: {self.cfg.riesgo_pct}% ➕", callback_data="risk_up")],
            [InlineKeyboardButton(f"Umbral Ballena: {self.cfg.whale_threshold}x ➖", callback_data="whale_down"),
             InlineKeyboardButton(f"Umbral Ballena: {self.cfg.whale_threshold}x ➕", callback_data="whale_up")],
            [InlineKeyboardButton(state_btn, callback_data="toggle_pause")],
            [InlineKeyboardButton("🔙 Volver al Inicio", callback_data="back_main")]
        ])

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.cfg.ADMIN_ID: return
        t_pendientes = len(self.db.obtener_pendientes())
        e_str = "🟢 Escáner ACTIVO" if not self.cfg.is_paused else "🔴 Escáner PAUSADO"
        
        txt = (
            "🤖 *PANEL CENTRAL - MODO TURBO (SCALPING)* 🤖\n\n"
            f"*Motor:* {e_str}\n"
            f"*Trades Activos:* {t_pendientes}\n"
            f"*Riesgo Actual:* {self.cfg.riesgo_pct}%\n\n"
            "Elige una opción para administrar el sistema:"
        )
        await update.message.reply_text(txt, reply_markup=self.get_main_keyboard(), parse_mode="Markdown")

    async def handle_callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if q.from_user.id != self.cfg.ADMIN_ID: return
        await q.answer()
        d = q.data

        if d == "scan_all":
            await self.engine.escanear_mercado(q.from_user.id, manual=True)
        
        elif d == "view_stats":
            w, l, e, tot, rate = self.db.obtener_estadisticas()
            if tot == 0: await q.message.reply_text("Sin datos registrados.")
            else:
                await q.message.reply_text(
                    f"📊 *RENDIMIENTO MODO TURBO*\n🎯 Win Rate: `{rate:.1f}%`\n"
                    f"✅ Ganadas: {w} | ❌ Perdidas: {l} | ⏱ Expiradas: {e}", 
                    parse_mode="Markdown"
                )
                
        elif d == "view_active":
            pendientes = self.db.obtener_pendientes()
            if not pendientes: await q.message.reply_text("💤 No hay operaciones abiertas.")
            else:
                txt = "📜 *TRADES EN CURSO*\n\n"
                for t in pendientes: txt += f"🔸 *{t[1]}* ({t[3]})\nE: `{t[4]}` | SL: `{t[5]}` | TP: `{t[6]}`\n\n"
                await q.message.reply_text(txt, parse_mode="Markdown")
                
        elif d == "config_menu":
            await q.edit_message_text("⚙️ *CONFIGURACIÓN DEL SISTEMA*\nAjusta los parámetros en tiempo real:", 
                                      reply_markup=self.get_config_keyboard(), parse_mode="Markdown")
        
        elif d == "back_main":
            t_pend = len(self.db.obtener_pendientes())
            e_str = "🟢 ACTIVO" if not self.cfg.is_paused else "🔴 PAUSADO"
            await q.edit_message_text(f"🤖 *PANEL CENTRAL - MODO TURBO*\nMotor: {e_str}\nTrades: {t_pend}\nRiesgo: {self.cfg.riesgo_pct}%", 
                                      reply_markup=self.get_main_keyboard(), parse_mode="Markdown")
            
        elif d == "risk_up":
            self.cfg.riesgo_pct += 0.5
            await q.edit_message_reply_markup(self.get_config_keyboard())
        elif d == "risk_down":
            if self.cfg.riesgo_pct > 0.5: self.cfg.riesgo_pct -= 0.5
            await q.edit_message_reply_markup(self.get_config_keyboard())
            
        elif d == "whale_up":
            self.cfg.whale_threshold += 0.5
            await q.edit_message_reply_markup(self.get_config_keyboard())
        elif d == "whale_down":
            if self.cfg.whale_threshold > 1.5: self.cfg.whale_threshold -= 0.5
            await q.edit_message_reply_markup(self.get_config_keyboard())
            
        elif d == "toggle_pause":
            self.cfg.is_paused = not self.cfg.is_paused
            await q.edit_message_reply_markup(self.get_config_keyboard())

# ==========================================
# 7. ARRANQUE DEL SISTEMA
# ==========================================
def main():
    cfg = BotConfig()
    db = DatabaseManager(cfg.db_name)
    market = MarketData(cfg)
    brain = AIBrain(cfg, db)
    engine = TradingEngine(cfg, db, market, brain)
    ui = TelegramUI(cfg, db, engine)

    print("Trading Bot V2 - MODO TURBO INICIADO.")
    print("Lectura: 5m/15m/30m | Escaner: 5m | Auditor: 2m | Expira: 1h")
    ui.app.run_polling()

if __name__ == "__main__":
    main()
