# ANTAEUS v1.1 | MEAN REVERSION SPECIALIST
# Named after the giant who drew strength from returning to earth — his mean.
#
# CHANGES FROM v1.0:
# 1. CERBERUS HEARTBEAT — writes to service_heartbeat every 5-min cycle.
#    CERBERUS threshold stays at 15 min.
# 2. FLASK /health ENDPOINT — was missing entirely. Added with threading.
# 3. LIVE OKX PRICE FEED — replaces the fragile signal_attribution / macro_state
#    price source with direct OKX 1-minute candle fetching. ANTAEUS now produces
#    real signals from day one without needing trade history to accumulate.
# 4. CRONUS ON-CHAIN CONTEXT — reads mvrv_zone_flag and cycle_index_signal from
#    macro_indicators to enhance deviation quality scoring. MVRV in buy zone
#    boosts downside reversion conviction. Distribution zone boosts upside.
# 5. SCHEMA SAFETY-NET — ALTER TABLE guards on all original columns.
#
# UNCHANGED FROM v1.0:
# - Multi-timeframe mean calculation logic (EMA)
# - Deviation quality scoring framework
# - Reversion probability computation
# - ARGUS regime compatibility check
# - PHEME contrarian boost
# - pantheon_state writes

import os, time, json, logging, requests, psycopg2, math, threading
from flask import Flask, jsonify
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ANTAEUS - %(levelname)s - %(message)s'
)

DATABASE_URL     = os.getenv('DATABASE_URL')
TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ANTAEUS_INTERVAL = 300  # 5 minutes

# ── v1.1: service identity ────────────────────────────────────────────────────
SERVICE_NAME    = "pantheon-antaeus"
SERVICE_VERSION = "1.1"

PAIRS = ['BTC', 'ETH', 'SOL']

# OKX instrument IDs for price fetching
OKX_INST = {
    'BTC': 'BTC-USDT-SWAP',
    'ETH': 'ETH-USDT-SWAP',
    'SOL': 'SOL-USDT-SWAP',
}

# Deviation thresholds (unchanged)
WEAK_DEVIATION     = 0.005
MODERATE_DEVIATION = 0.010
STRONG_DEVIATION   = 0.020
EXTREME_DEVIATION  = 0.040

app = Flask(__name__)

def get_db():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logging.error(f"DB FAILED: {e}")
        return None

def tg(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"[ANTAEUS] {msg}"},
            timeout=5
        )
    except: pass

# ── v1.1: CERBERUS heartbeat ─────────────────────────────────────────────────
def write_heartbeat(cycle_count=0, status="alive", last_error=None):
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO service_heartbeat
            (service_name, last_heartbeat, version, status, loop_count, last_error, meta)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (service_name) DO UPDATE SET
                last_heartbeat = NOW(),
                version        = EXCLUDED.version,
                status         = EXCLUDED.status,
                loop_count     = EXCLUDED.loop_count,
                last_error     = EXCLUDED.last_error,
                meta           = EXCLUDED.meta
        """, (
            SERVICE_NAME, SERVICE_VERSION, status,
            cycle_count,
            last_error[:500] if last_error else None,
            json.dumps({})
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.warning(f"CERBERUS heartbeat failed: {e}")

def self_heal_schema():
    try:
        conn = get_db()
        if not conn: return
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS antaeus_state (
                id                  SERIAL PRIMARY KEY,
                timestamp           TIMESTAMPTZ DEFAULT NOW(),
                pair                TEXT DEFAULT 'BTC',
                current_price       FLOAT DEFAULT 0.0,
                mean_5m             FLOAT DEFAULT 0.0,
                mean_15m            FLOAT DEFAULT 0.0,
                mean_1h             FLOAT DEFAULT 0.0,
                mean_4h             FLOAT DEFAULT 0.0,
                deviation_5m        FLOAT DEFAULT 0.0,
                deviation_15m       FLOAT DEFAULT 0.0,
                deviation_1h        FLOAT DEFAULT 0.0,
                deviation_4h        FLOAT DEFAULT 0.0,
                composite_deviation FLOAT DEFAULT 0.0,
                deviation_quality   TEXT DEFAULT 'WEAK',
                reversion_prob      FLOAT DEFAULT 0.0,
                reversion_target    FLOAT DEFAULT 0.0,
                reversion_direction TEXT DEFAULT 'NONE',
                regime_compatible   BOOLEAN DEFAULT FALSE,
                sentiment_boost     BOOLEAN DEFAULT FALSE,
                signal_strength     TEXT DEFAULT 'NONE',
                detail              TEXT DEFAULT '{}',
                updated_at          TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Safety-net: all original columns
        safety_cols = [
            ("pair",                "TEXT DEFAULT 'BTC'"),
            ("current_price",       "FLOAT DEFAULT 0.0"),
            ("mean_5m",             "FLOAT DEFAULT 0.0"),
            ("mean_15m",            "FLOAT DEFAULT 0.0"),
            ("mean_1h",             "FLOAT DEFAULT 0.0"),
            ("mean_4h",             "FLOAT DEFAULT 0.0"),
            ("deviation_5m",        "FLOAT DEFAULT 0.0"),
            ("deviation_15m",       "FLOAT DEFAULT 0.0"),
            ("deviation_1h",        "FLOAT DEFAULT 0.0"),
            ("deviation_4h",        "FLOAT DEFAULT 0.0"),
            ("composite_deviation", "FLOAT DEFAULT 0.0"),
            ("deviation_quality",   "TEXT DEFAULT 'WEAK'"),
            ("reversion_prob",      "FLOAT DEFAULT 0.0"),
            ("reversion_target",    "FLOAT DEFAULT 0.0"),
            ("reversion_direction", "TEXT DEFAULT 'NONE'"),
            ("regime_compatible",   "BOOLEAN DEFAULT FALSE"),
            ("sentiment_boost",     "BOOLEAN DEFAULT FALSE"),
            ("signal_strength",     "TEXT DEFAULT 'NONE'"),
            ("detail",              "TEXT DEFAULT '{}'"),
            ("updated_at",          "TIMESTAMPTZ DEFAULT NOW()"),
        ]
        for col, dtype in safety_cols:
            try:
                cur.execute(f"ALTER TABLE antaeus_state ADD COLUMN IF NOT EXISTS {col} {dtype}")
            except: pass

        conn.commit()
        cur.close()
        conn.close()
        logging.info("ANTAEUS v1.1 SCHEMA HEALED")
    except Exception as e:
        logging.error(f"SCHEMA HEAL FAILED: {e}")

# ─────────────────────────────────────────────────────────────
# v1.1 NEW: LIVE PRICE FETCHING FROM OKX
# ─────────────────────────────────────────────────────────────
def fetch_okx_prices(pair, bars=250):
    """
    Fetch recent 1-minute candles from OKX for the given pair.
    Returns list of (price, timestamp) tuples, oldest first.
    Falls back to CoinGecko spot + macro_state if OKX fails.
    """
    inst_id = OKX_INST.get(pair)
    if not inst_id:
        return None

    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/candles",
            params={"instId": inst_id, "bar": "1m", "limit": str(bars)},
            timeout=10
        )
        if r.status_code == 200:
            candles = r.json().get('data', [])
            if candles:
                # OKX returns newest first: [ts, open, high, low, close, vol, ...]
                # Reverse to get oldest first, use close price (index 4)
                prices = []
                for c in reversed(candles):
                    try:
                        ts_ms = int(c[0])
                        close = float(c[4])
                        ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                        if close > 0:
                            prices.append((close, ts_dt))
                    except: pass
                if prices:
                    logging.info(f"ANTAEUS {pair}: fetched {len(prices)} candles from OKX")
                    return prices
    except Exception as e:
        logging.warning(f"ANTAEUS OKX price fetch failed for {pair}: {e}")

    # Fallback: CoinGecko + macro_state
    return fetch_price_fallback(pair)

def fetch_price_fallback(pair):
    """Fallback price source using CoinGecko + macro_state estimate."""
    cg_ids = {'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana'}
    cg_id = cg_ids.get(pair)
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd"},
            timeout=8
        )
        if r.status_code == 200:
            price = float(r.json().get(cg_id, {}).get('usd', 0))
            if price > 0:
                # Single price point — limited usefulness but better than nothing
                now = datetime.now(timezone.utc)
                return [(price, now)]
    except: pass
    return None

# ─────────────────────────────────────────────────────────────
# v1.1 NEW: ON-CHAIN CONTEXT FROM CRONUS
# ─────────────────────────────────────────────────────────────
def get_onchain_context():
    """
    Read CRONUS on-chain signals from macro_indicators.
    Used to enhance deviation quality scoring.
    MVRV in buy zone = boost downside reversion conviction.
    Distribution zone = boost upside reversion conviction.
    """
    ctx = {
        'mvrv_zone':   'UNKNOWN',
        'cycle_signal':'UNKNOWN',
        'onchain_risk': 0.5,
    }
    try:
        conn = get_db()
        if not conn: return ctx
        cur = conn.cursor()
        cur.execute("""
            SELECT mvrv_zone_flag, cycle_index_signal, onchain_risk_score
            FROM macro_indicators
            WHERE mvrv_zone_flag IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            ctx['mvrv_zone']    = str(row[0]) if row[0] else 'UNKNOWN'
            ctx['cycle_signal'] = str(row[1]) if row[1] else 'UNKNOWN'
            ctx['onchain_risk'] = float(row[2]) if row[2] else 0.5
    except Exception as e:
        logging.debug(f"On-chain context read: {e}")
    return ctx

def get_current_context(pair):
    """Get current regime and sentiment context from DB."""
    try:
        conn = get_db()
        if not conn: return None
        cur = conn.cursor()

        cur.execute("""
            SELECT dominant_regime, dominant_prob, confidence
            FROM argus_state
            WHERE pair = %s
            ORDER BY timestamp DESC LIMIT 1
        """, (pair,))
        argus = cur.fetchone()

        cur.execute("""
            SELECT sentiment_score, contrarian_signal,
                   retail_score, institutional_score
            FROM pheme_state
            ORDER BY timestamp DESC LIMIT 1
        """)
        pheme = cur.fetchone()

        cur.execute("""
            SELECT btc_price_change_pct, fg_current, vix_level
            FROM sentinel_state
            ORDER BY timestamp DESC LIMIT 1
        """)
        sentinel = cur.fetchone()

        cur.execute("""
            SELECT btc_price, dma_200, above_200dma
            FROM macro_state
            ORDER BY timestamp DESC LIMIT 1
        """)
        macro = cur.fetchone()

        cur.close()
        conn.close()

        return {
            'argus_regime':   str(argus[0]) if argus else 'UNKNOWN',
            'argus_prob':     float(argus[1]) if argus else 0.2,
            'argus_conf':     int(argus[2]) if argus else 0,
            'pheme_score':    int(pheme[0]) if pheme else 0,
            'pheme_contrast': bool(pheme[1]) if pheme else False,
            'pheme_retail':   int(pheme[2]) if pheme else 0,
            'pheme_inst':     int(pheme[3]) if pheme else 0,
            'btc_pct':        float(sentinel[0]) if sentinel else 0.0,
            'fg':             int(sentinel[1]) if sentinel else 50,
            'vix':            float(sentinel[2]) if sentinel else 20.0,
            'btc_price':      float(macro[0]) if macro else 0.0,
            'dma_200':        float(macro[1]) if macro else 0.0,
            'above_200dma':   bool(macro[2]) if macro and macro[2] is not None else False,
        }

    except Exception as e:
        logging.error(f"CONTEXT FAILED: {e}")
        return None

def compute_multi_timeframe_means(prices_with_time):
    """EMA means at multiple timeframes from price history. Unchanged from v1.0."""
    if not prices_with_time or len(prices_with_time) < 2:
        return {}

    prices = [p[0] for p in prices_with_time]
    now    = datetime.now(timezone.utc)
    means  = {}

    def ema(data, period):
        if len(data) < 2: return data[-1] if data else 0
        k = 2 / (period + 1)
        e = data[0]
        for p in data[1:]:
            e = p * k + e * (1 - k)
        return e

    def prices_in_window(minutes):
        cutoff = now - timedelta(minutes=minutes)
        filtered = [p for p, t in prices_with_time
                    if (t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t) > cutoff]
        return filtered if filtered else prices

    p5m  = prices_in_window(5)
    p15m = prices_in_window(15)
    p1h  = prices_in_window(60)
    p4h  = prices_in_window(240)

    if p5m:  means['5m']  = ema(p5m,  min(20, len(p5m)))
    if p15m: means['15m'] = ema(p15m, min(20, len(p15m)))
    if p1h:  means['1h']  = ema(p1h,  min(50, len(p1h)))
    if p4h:  means['4h']  = ema(p4h,  min(200, len(p4h)))

    if not means:
        if len(prices) >= 4:
            means['15m'] = ema(prices[-4:], 4)
            means['1h']  = ema(prices, min(20, len(prices)))
        elif prices:
            means['1h']  = sum(prices) / len(prices)

    return means

def compute_deviation_quality(deviation, context, onchain_ctx, pair):
    """
    Score the quality of deviation.
    v1.1: adds on-chain context from CRONUS.
    """
    abs_dev = abs(deviation)
    details = {}

    # Base quality from deviation size (unchanged)
    if abs_dev < WEAK_DEVIATION:
        base_quality = 'INSUFFICIENT'
        base_prob    = 0.1
    elif abs_dev < MODERATE_DEVIATION:
        base_quality = 'WEAK'
        base_prob    = 0.35
    elif abs_dev < STRONG_DEVIATION:
        base_quality = 'MODERATE'
        base_prob    = 0.55
    elif abs_dev < EXTREME_DEVIATION:
        base_quality = 'STRONG'
        base_prob    = 0.70
    else:
        base_quality = 'EXTREME'
        base_prob    = 0.40

    details['base'] = base_quality

    # Regime modifier (unchanged)
    regime = context.get('argus_regime', 'UNKNOWN')
    if regime in ('CHOP_TIGHT', 'CHOP_WIDE'):
        base_prob += 0.15
        details['regime'] = 'CHOP_BOOST'
    elif regime in ('TREND_BULL', 'TREND_BEAR'):
        base_prob -= 0.20
        details['regime'] = 'TREND_PENALTY'
    elif regime == 'CRISIS':
        base_prob -= 0.30
        details['regime'] = 'CRISIS_PENALTY'

    # PHEME contrarian boost (unchanged)
    if context.get('pheme_contrast'):
        base_prob += 0.10
        details['pheme'] = 'CONTRARIAN_BOOST'

    pheme_score = context.get('pheme_score', 0)
    if pheme_score < -50 and deviation < 0:
        base_prob += 0.10
        details['sentiment'] = 'EXTREME_FEAR_BOOST'
    elif pheme_score > 50 and deviation > 0:
        base_prob += 0.10
        details['sentiment'] = 'EXTREME_GREED_BOOST'

    # VIX modifier (unchanged)
    vix = context.get('vix', 20.0)
    if vix > 30:
        base_prob -= 0.10
        details['vix'] = 'HIGH_VIX_PENALTY'
    elif vix < 15:
        base_prob += 0.05
        details['vix'] = 'LOW_VIX_BOOST'

    # 200DMA context (unchanged)
    if not context.get('above_200dma') and deviation < 0:
        base_prob += 0.05
        details['dma'] = 'BELOW_200DMA_BOOST'

    # v1.1 NEW: CRONUS on-chain context
    mvrv_zone   = onchain_ctx.get('mvrv_zone', 'UNKNOWN')
    cycle_sig   = onchain_ctx.get('cycle_signal', 'UNKNOWN')
    onchain_risk= onchain_ctx.get('onchain_risk', 0.5)

    # MVRV zone — in historical buy zone with price below mean = strong conviction
    if mvrv_zone == 'HISTORICAL_BUY_ZONE' and deviation < 0:
        base_prob += 0.10
        details['mvrv_zone'] = 'BUY_ZONE_BOOST'
    elif mvrv_zone == 'BELOW_COST_BASIS' and deviation < 0:
        base_prob += 0.12
        details['mvrv_zone'] = 'BELOW_COST_EXTREME_BOOST'
    elif mvrv_zone == 'OVERVALUED_ZONE' and deviation > 0:
        base_prob += 0.08
        details['mvrv_zone'] = 'OVERVALUED_SHORT_BOOST'

    # Cycle signal — accumulation = boost downside reversion (buyers likely)
    if cycle_sig in ('ACCUMULATION', 'DEEP_ACCUMULATION') and deviation < 0:
        base_prob += 0.08
        details['cycle'] = 'ACCUMULATION_BOOST'
    elif cycle_sig in ('DISTRIBUTION', 'EXTREME_DISTRIBUTION') and deviation > 0:
        base_prob += 0.08
        details['cycle'] = 'DISTRIBUTION_BOOST'

    # High onchain risk = reduce reversion confidence (could keep falling)
    if onchain_risk > 0.7 and deviation < 0:
        base_prob -= 0.08
        details['onchain_risk'] = 'HIGH_RISK_PENALTY'

    final_prob = max(0.05, min(0.90, base_prob))

    if final_prob >= 0.65:    final_quality = 'HIGH_CONVICTION'
    elif final_prob >= 0.50:  final_quality = 'MODERATE'
    elif final_prob >= 0.35:  final_quality = 'WEAK'
    else:                     final_quality = 'INSUFFICIENT'

    return final_quality, round(final_prob, 3), details

def compute_reversion_target(current_price, means, deviation):
    """Reversion target calculation. Unchanged from v1.0."""
    if not means: return current_price, current_price

    primary_tf = '15m' if '15m' in means else '1h' if '1h' in means else list(means.keys())[0]
    primary_target = means[primary_tf]

    secondary_tf = '1h' if '1h' in means else '4h' if '4h' in means else primary_tf
    secondary_target = means[secondary_tf]

    return round(primary_target, 2), round(secondary_target, 2)

def compute_signal_strength(quality, prob, regime_compatible, deviation):
    """Signal strength classification. Unchanged from v1.0."""
    if not regime_compatible:
        return 'BLOCKED_REGIME'
    if quality == 'INSUFFICIENT':
        return 'NONE'
    if quality == 'HIGH_CONVICTION' and prob >= 0.65:
        return 'STRONG'
    if quality in ('HIGH_CONVICTION', 'MODERATE') and prob >= 0.50:
        return 'MODERATE'
    if prob >= 0.35:
        return 'WEAK'
    return 'NONE'

def write_antaeus_state(pair, data):
    """Write ANTAEUS assessment to DB. Unchanged from v1.0."""
    try:
        conn = get_db()
        if not conn: return
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO antaeus_state (
                pair, current_price,
                mean_5m, mean_15m, mean_1h, mean_4h,
                deviation_5m, deviation_15m, deviation_1h, deviation_4h,
                composite_deviation, deviation_quality,
                reversion_prob, reversion_target, reversion_direction,
                regime_compatible, sentiment_boost,
                signal_strength, detail, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, NOW()
            )
        """, (
            pair,
            data['current_price'],
            data['means'].get('5m', 0.0),
            data['means'].get('15m', 0.0),
            data['means'].get('1h', 0.0),
            data['means'].get('4h', 0.0),
            data['deviations'].get('5m', 0.0),
            data['deviations'].get('15m', 0.0),
            data['deviations'].get('1h', 0.0),
            data['deviations'].get('4h', 0.0),
            data['composite_deviation'],
            data['quality'],
            data['reversion_prob'],
            data['primary_target'],
            data['direction'],
            data['regime_compatible'],
            data['sentiment_boost'],
            data['signal_strength'],
            json.dumps(data['detail']),
        ))

        for key, val in [
            (f"antaeus:signal:{pair}", data['signal_strength']),
            (f"antaeus:prob:{pair}",   str(data['reversion_prob'])),
            (f"antaeus:target:{pair}", str(data['primary_target'])),
        ]:
            cur.execute("""
                INSERT INTO pantheon_state (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value=EXCLUDED.value, updated_at=NOW()
            """, (key, val))

        conn.commit()
        cur.close()
        conn.close()

        logging.info(
            f"ANTAEUS {pair}: {data['signal_strength']} | "
            f"dev={data['composite_deviation']:+.3%} | "
            f"quality={data['quality']} prob={data['reversion_prob']:.0%} | "
            f"target={data['primary_target']:.2f} [{data['direction']}]"
        )

        if data['signal_strength'] == 'STRONG':
            tg(
                f"MEAN REVERSION SIGNAL\n"
                f"Pair: {pair}\n"
                f"Direction: {data['direction']}\n"
                f"Deviation: {data['composite_deviation']:+.2%}\n"
                f"Probability: {data['reversion_prob']:.0%}\n"
                f"Target: {data['primary_target']:.2f}\n"
                f"Quality: {data['quality']}"
            )

    except Exception as e:
        logging.error(f"ANTAEUS WRITE FAILED: {e}")

def analyse_pair(pair, onchain_ctx):
    """Full ANTAEUS analysis for one pair. v1.1: uses live OKX prices."""
    context = get_current_context(pair)
    if not context:
        logging.warning(f"ANTAEUS: no context for {pair}")
        return

    # v1.1: fetch live prices from OKX (250 1-min candles = ~4 hours)
    prices = fetch_okx_prices(pair, bars=250)

    if not prices:
        logging.warning(f"ANTAEUS {pair}: no prices available, writing neutral state")
        data = {
            'current_price':      0.0,
            'means':              {},
            'deviations':         {},
            'composite_deviation': 0.0,
            'quality':            'INSUFFICIENT',
            'reversion_prob':     0.0,
            'primary_target':     0.0,
            'direction':          'NONE',
            'regime_compatible':  False,
            'sentiment_boost':    False,
            'signal_strength':    'NONE',
            'detail':             {'reason': 'no_price_data'},
        }
        write_antaeus_state(pair, data)
        return

    # Compute multi-timeframe means
    means = compute_multi_timeframe_means(prices)
    current_price = prices[-1][0]

    if not means or current_price == 0:
        logging.warning(f"ANTAEUS {pair}: could not compute means")
        return

    # Deviations from each mean
    deviations = {}
    for tf, mean in means.items():
        if mean > 0:
            deviations[tf] = (current_price - mean) / mean
        else:
            deviations[tf] = 0.0

    # Composite deviation — weighted
    tf_weights = {'5m': 0.15, '15m': 0.35, '1h': 0.35, '4h': 0.15}
    weighted_dev = 0.0
    total_weight = 0.0
    for tf, dev in deviations.items():
        w = tf_weights.get(tf, 0.25)
        weighted_dev += dev * w
        total_weight += w
    composite = weighted_dev / total_weight if total_weight > 0 else 0.0

    direction = 'LONG' if composite < 0 else 'SHORT' if composite > 0 else 'NONE'

    regime = context.get('argus_regime', 'UNKNOWN')
    regime_compatible = regime in ('CHOP_TIGHT', 'CHOP_WIDE', 'UNKNOWN')
    sentiment_boost   = context.get('pheme_contrast', False)

    # v1.1: pass onchain_ctx to quality scorer
    quality, prob, detail = compute_deviation_quality(composite, context, onchain_ctx, pair)

    primary_target, secondary_target = compute_reversion_target(current_price, means, composite)
    signal = compute_signal_strength(quality, prob, regime_compatible, composite)

    data = {
        'current_price':       current_price,
        'means':               means,
        'deviations':          deviations,
        'composite_deviation': round(composite, 6),
        'quality':             quality,
        'reversion_prob':      prob,
        'primary_target':      primary_target,
        'secondary_target':    secondary_target,
        'direction':           direction,
        'regime_compatible':   regime_compatible,
        'sentiment_boost':     sentiment_boost,
        'signal_strength':     signal,
        'detail':              detail,
    }
    write_antaeus_state(pair, data)

def run_antaeus_pulse(cycle_count):
    """Main ANTAEUS pulse."""
    logging.info("=== ANTAEUS v1.1 PULSE STARTING ===")
    # Fetch on-chain context once per pulse (shared across all pairs)
    onchain_ctx = get_onchain_context()
    for pair in PAIRS:
        analyse_pair(pair, onchain_ctx)
    logging.info("=== ANTAEUS v1.1 PULSE COMPLETE ===")

# ── FLASK ENDPOINTS (v1.1 NEW) ────────────────────────────────────────────────
@app.route('/health')
def health():
    return jsonify({"status": "ok", "version": "1.1", "service": "ANTAEUS"})

@app.route('/signals')
def signals():
    """Return current mean reversion signals for all pairs."""
    try:
        conn = get_db()
        if not conn: return jsonify({"error": "db"}), 500
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (pair)
                pair, signal_strength, reversion_prob,
                composite_deviation, reversion_target,
                reversion_direction, deviation_quality,
                regime_compatible, timestamp
            FROM antaeus_state
            ORDER BY pair, timestamp DESC
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        result = {}
        for row in rows:
            result[row[0]] = {
                "signal_strength":    row[1],
                "reversion_prob":     round(row[2], 3),
                "composite_deviation":round(row[3], 6),
                "reversion_target":   row[4],
                "direction":          row[5],
                "quality":            row[6],
                "regime_compatible":  row[7],
                "timestamp":          str(row[8])
            }
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── MAIN LOOP — v1.1: heartbeat + Flask threading ─────────────────────────────
def antaeus_loop():
    cycle_count = 0
    try:
        run_antaeus_pulse(cycle_count)
        write_heartbeat(cycle_count=cycle_count, status="alive")
    except Exception as e:
        logging.error(f"Initial pulse failed: {e}")
        write_heartbeat(cycle_count=cycle_count, status="error", last_error=str(e))
    while True:
        time.sleep(ANTAEUS_INTERVAL)
        cycle_count += 1
        try:
            run_antaeus_pulse(cycle_count)
            write_heartbeat(cycle_count=cycle_count, status="alive")
        except Exception as e:
            logging.error(f"ANTAEUS loop error: {e}")
            write_heartbeat(cycle_count=cycle_count, status="error", last_error=str(e))

if __name__ == "__main__":
    logging.info("ANTAEUS v1.1 ONLINE — MEAN REVERSION + OKX LIVE PRICES + CERBERUS + ON-CHAIN CONTEXT")
    self_heal_schema()
    t = threading.Thread(target=antaeus_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8080, debug=False)
