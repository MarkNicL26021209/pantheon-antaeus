# ANTAEUS v1.0 | MEAN REVERSION SPECIALIST
# Named after the giant who drew strength from returning to earth — his mean.
# He was unbeatable as long as he could touch the ground.
# Hercules defeated him by lifting him away from it.
#
# ANTAEUS specialises in identifying when price has strayed too far from
# its mean and is likely to return. It distinguishes genuine reversion
# opportunities from trend continuation moves that look oversold but aren't.
#
# Key functions:
# 1. Multi-timeframe mean calculation (5m, 15m, 1h, 4h)
# 2. Deviation quality scoring — genuine oversold vs trend move
# 3. Reversion probability estimation
# 4. Mean reversion target calculation
# 5. Integration with ARGUS — only fires in CHOP regimes
# 6. Integration with PHEME — contrarian sentiment boosts reversion signal
#
# Output: antaeus_state table
# PANTHEON reads antaeus_state for enhanced meanrev strategy decisions

import os, time, json, logging, requests, psycopg2
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ANTAEUS - %(levelname)s - %(message)s'
)

DATABASE_URL     = os.getenv('DATABASE_URL')
TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ANTAEUS_INTERVAL = 300  # 5 minutes

PAIRS = ['BTC', 'ETH', 'SOL']

# Deviation thresholds
WEAK_DEVIATION    = 0.005  # 0.5% — marginal
MODERATE_DEVIATION = 0.010  # 1.0% — tradeable
STRONG_DEVIATION  = 0.020  # 2.0% — high conviction
EXTREME_DEVIATION = 0.040  # 4.0% — potential trend, not reversion

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
        conn.commit()
        cur.close()
        conn.close()
        logging.info("ANTAEUS v1.0 SCHEMA HEALED")
    except Exception as e:
        logging.error(f"SCHEMA HEAL FAILED: {e}")

def get_price_history(pair):
    """
    Get recent price history from pantheon_state websocket data.
    Falls back to trade_log prices if websocket data unavailable.
    """
    try:
        conn = get_db()
        if not conn: return None
        cur = conn.cursor()

        # Try to get recent prices from market_archives
        cur.execute("""
            SELECT price, timestamp
            FROM market_archives
            WHERE pair = %s
            AND timestamp > NOW() - INTERVAL '4 hours'
            ORDER BY timestamp ASC
        """, (pair,))
        rows = cur.fetchall()

        if rows and len(rows) >= 10:
            cur.close()
            conn.close()
            return [(float(r[0]), r[1]) for r in rows]

        # Fallback — use signal_attribution price data
        cur.execute("""
            SELECT ofi_at_entry, trade_timestamp
            FROM signal_attribution
            WHERE pair = %s
            AND trade_timestamp > NOW() - INTERVAL '4 hours'
            ORDER BY trade_timestamp ASC
        """, (pair,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if rows:
            return [(float(r[0]), r[1]) for r in rows]

        return None

    except Exception as e:
        logging.warning(f"PRICE HISTORY FAILED for {pair}: {e}")
        return None

def get_current_context(pair):
    """Get current price and regime context from DB."""
    try:
        conn = get_db()
        if not conn: return None
        cur = conn.cursor()

        # Get latest ARGUS regime
        cur.execute("""
            SELECT dominant_regime, dominant_prob, confidence
            FROM argus_state
            WHERE pair = %s
            ORDER BY timestamp DESC LIMIT 1
        """, (pair,))
        argus = cur.fetchone()

        # Get PHEME sentiment
        cur.execute("""
            SELECT sentiment_score, contrarian_signal,
                   retail_score, institutional_score
            FROM pheme_state
            ORDER BY timestamp DESC LIMIT 1
        """)
        pheme = cur.fetchone()

        # Get current BTC price from sentinel
        cur.execute("""
            SELECT btc_price_change_pct, fg_current, vix_level
            FROM sentinel_state
            ORDER BY timestamp DESC LIMIT 1
        """)
        sentinel = cur.fetchone()

        # Get macro state
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
    """
    Calculate EMA means at multiple timeframes from price history.
    Returns dict of {timeframe: mean_price}
    """
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

    # Filter prices by timeframe
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

    # If insufficient history use available prices with different periods
    if not means:
        if len(prices) >= 4:
            means['15m'] = ema(prices[-4:],  4)
            means['1h']  = ema(prices,       min(20, len(prices)))
        elif prices:
            means['1h']  = sum(prices) / len(prices)

    return means

def compute_deviation_quality(deviation, context, pair):
    """
    Score the quality of deviation — is this a genuine reversion opportunity
    or price discovery in a new trend?

    Returns: quality label, probability, details
    """
    abs_dev = abs(deviation)
    details = {}

    # Base quality from deviation size
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
        base_prob    = 0.40  # Extreme deviation may be trending, not reverting

    details['base'] = base_quality

    # Regime modifier — CHOP regimes favour reversion
    regime = context.get('argus_regime', 'UNKNOWN')
    if regime in ('CHOP_TIGHT', 'CHOP_WIDE'):
        base_prob += 0.15
        details['regime'] = 'CHOP_BOOST'
    elif regime in ('TREND_BULL', 'TREND_BEAR'):
        base_prob -= 0.20  # Trending — deviation may continue
        details['regime'] = 'TREND_PENALTY'
    elif regime == 'CRISIS':
        base_prob -= 0.30  # Crisis — extreme moves can continue
        details['regime'] = 'CRISIS_PENALTY'

    # PHEME contrarian boost
    if context.get('pheme_contrast'):
        base_prob += 0.10
        details['pheme'] = 'CONTRARIAN_BOOST'

    # Extreme retail fear with institutional neutral = reversion likely
    pheme_score = context.get('pheme_score', 0)
    if pheme_score < -50 and deviation < 0:  # Price below mean AND extreme fear
        base_prob += 0.10
        details['sentiment'] = 'EXTREME_FEAR_BOOST'
    elif pheme_score > 50 and deviation > 0:  # Price above mean AND extreme greed
        base_prob += 0.10
        details['sentiment'] = 'EXTREME_GREED_BOOST'

    # VIX modifier
    vix = context.get('vix', 20.0)
    if vix > 30:
        base_prob -= 0.10  # High vol = less predictable reversion
        details['vix'] = 'HIGH_VIX_PENALTY'
    elif vix < 15:
        base_prob += 0.05  # Low vol = tight ranges, reversion reliable
        details['vix'] = 'LOW_VIX_BOOST'

    # 200DMA context — below 200DMA oversold bounces more reliable
    if not context.get('above_200dma') and deviation < 0:
        base_prob += 0.05
        details['dma'] = 'BELOW_200DMA_BOOST'

    final_prob = max(0.05, min(0.90, base_prob))

    if final_prob >= 0.65:    final_quality = 'HIGH_CONVICTION'
    elif final_prob >= 0.50:  final_quality = 'MODERATE'
    elif final_prob >= 0.35:  final_quality = 'WEAK'
    else:                     final_quality = 'INSUFFICIENT'

    return final_quality, round(final_prob, 3), details

def compute_reversion_target(current_price, means, deviation):
    """
    Calculate the most likely reversion target.
    Primary target: 15m mean (fastest to revert to)
    Secondary target: 1h mean (deeper reversion)
    """
    if not means: return current_price, current_price

    # Primary target — 15m or shortest available mean
    primary_tf = '15m' if '15m' in means else '1h' if '1h' in means else list(means.keys())[0]
    primary_target = means[primary_tf]

    # Secondary target — 1h or 4h mean
    secondary_tf = '1h' if '1h' in means else '4h' if '4h' in means else primary_tf
    secondary_target = means[secondary_tf]

    return round(primary_target, 2), round(secondary_target, 2)

def compute_signal_strength(quality, prob, regime_compatible, deviation):
    """Determine overall signal strength for PANTHEON consumption."""
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
    """Write ANTAEUS assessment to DB."""
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

        # Write key signals to pantheon_state
        cur.execute("""
            INSERT INTO pantheon_state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET
                value=EXCLUDED.value, updated_at=NOW()
        """, (f"antaeus:signal:{pair}", data['signal_strength']))

        cur.execute("""
            INSERT INTO pantheon_state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET
                value=EXCLUDED.value, updated_at=NOW()
        """, (f"antaeus:prob:{pair}", str(data['reversion_prob'])))

        cur.execute("""
            INSERT INTO pantheon_state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET
                value=EXCLUDED.value, updated_at=NOW()
        """, (f"antaeus:target:{pair}", str(data['primary_target'])))

        conn.commit()
        cur.close()
        conn.close()

        logging.info(
            f"ANTAEUS {pair}: {data['signal_strength']} | "
            f"dev={data['composite_deviation']:+.3%} | "
            f"quality={data['quality']} prob={data['reversion_prob']:.0%} | "
            f"target={data['primary_target']:.2f} [{data['direction']}]"
        )

        # Telegram on strong signals
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

def analyse_pair(pair):
    """Full ANTAEUS analysis for one pair."""
    context = get_current_context(pair)
    if not context:
        logging.warning(f"ANTAEUS: no context for {pair}")
        return

    # Try to get price history
    prices = get_price_history(pair)

    # If no price history use BTC price from macro as single point reference
    if not prices:
        btc_price = context.get('btc_price', 0.0)
        if btc_price > 0 and pair == 'BTC':
            # Use price change to estimate recent movement
            pct_change = context.get('btc_pct', 0.0) / 100
            estimated_prev = btc_price / (1 + pct_change) if pct_change != -1 else btc_price
            prices = [
                (estimated_prev, datetime.now(timezone.utc) - timedelta(hours=1)),
                (btc_price, datetime.now(timezone.utc))
            ]
        else:
            logging.info(f"ANTAEUS {pair}: insufficient price history, using macro data")
            # Still write a neutral state so table exists
            data = {
                'current_price': context.get('btc_price', 0),
                'means': {},
                'deviations': {},
                'composite_deviation': 0.0,
                'quality': 'INSUFFICIENT',
                'reversion_prob': 0.0,
                'primary_target': 0.0,
                'direction': 'NONE',
                'regime_compatible': False,
                'sentiment_boost': False,
                'signal_strength': 'NONE',
                'detail': {'reason': 'insufficient_price_history'},
            }
            write_antaeus_state(pair, data)
            return

    # Compute multi-timeframe means
    means = compute_multi_timeframe_means(prices)
    current_price = prices[-1][0] if prices else 0.0

    if not means or current_price == 0:
        logging.warning(f"ANTAEUS {pair}: could not compute means")
        return

    # Compute deviations from each mean
    deviations = {}
    for tf, mean in means.items():
        if mean > 0:
            deviations[tf] = (current_price - mean) / mean
        else:
            deviations[tf] = 0.0

    # Composite deviation — weighted average
    tf_weights = {'5m': 0.15, '15m': 0.35, '1h': 0.35, '4h': 0.15}
    weighted_dev = 0.0
    total_weight = 0.0
    for tf, dev in deviations.items():
        w = tf_weights.get(tf, 0.25)
        weighted_dev += dev * w
        total_weight += w
    composite = weighted_dev / total_weight if total_weight > 0 else 0.0

    # Direction
    direction = 'LONG' if composite < 0 else 'SHORT' if composite > 0 else 'NONE'

    # Regime compatibility — ANTAEUS works best in CHOP
    regime = context.get('argus_regime', 'UNKNOWN')
    regime_compatible = regime in ('CHOP_TIGHT', 'CHOP_WIDE', 'UNKNOWN')

    # Sentiment boost from PHEME contrarian signal
    sentiment_boost = context.get('pheme_contrast', False)

    # Quality and probability
    quality, prob, detail = compute_deviation_quality(composite, context, pair)

    # Reversion targets
    primary_target, secondary_target = compute_reversion_target(
        current_price, means, composite
    )

    # Signal strength
    signal = compute_signal_strength(quality, prob, regime_compatible, composite)

    data = {
        'current_price':      current_price,
        'means':              means,
        'deviations':         deviations,
        'composite_deviation': round(composite, 6),
        'quality':            quality,
        'reversion_prob':     prob,
        'primary_target':     primary_target,
        'secondary_target':   secondary_target,
        'direction':          direction,
        'regime_compatible':  regime_compatible,
        'sentiment_boost':    sentiment_boost,
        'signal_strength':    signal,
        'detail':             detail,
    }

    write_antaeus_state(pair, data)

def run_antaeus_pulse():
    """Main ANTAEUS pulse — analyse all pairs."""
    logging.info("=== ANTAEUS v1.0 PULSE STARTING ===")

    for pair in PAIRS:
        analyse_pair(pair)

    logging.info("=== ANTAEUS PULSE COMPLETE ===")

if __name__ == "__main__":
    logging.info("ANTAEUS v1.0 ONLINE - MEAN REVERSION SPECIALIST")
    self_heal_schema()
    run_antaeus_pulse()
    while True:
        time.sleep(ANTAEUS_INTERVAL)
        run_antaeus_pulse()
