#!/usr/bin/env python3
"""Pre-compute aggregate tables for fast API queries"""
import sqlite3, os, sys
from datetime import datetime

DB = sys.argv[1] if len(sys.argv) > 1 else '/opt/pm-expert/pm_temperature.db'

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

conn = sqlite3.connect(DB)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA busy_timeout=30000')
c = conn.cursor()

# 1. daily_city_stats
log("Creating daily_city_stats...")
c.execute('''CREATE TABLE IF NOT EXISTS daily_city_stats (
    day TEXT, city TEXT, wallets INTEGER, trades INTEGER, volume REAL, events INTEGER,
    PRIMARY KEY (day, city)
)''')

# Only recompute last 7 days (historical doesn't change)
c.execute("DELETE FROM daily_city_stats WHERE day >= date('now','-7 days')")
c.execute('''INSERT OR REPLACE INTO daily_city_stats
    SELECT date(timestamp,'unixepoch'), city, COUNT(DISTINCT wallet), COUNT(*),
        ROUND(SUM(amount)), COUNT(DISTINCT event_id)
    FROM trades WHERE timestamp >= strftime('%s','now') - 604800
    GROUP BY date(timestamp,'unixepoch'), city''')
total = c.execute('SELECT COUNT(*) FROM daily_city_stats').fetchone()[0]
log(f"  daily_city_stats: {total} rows")

# 2. global_stats
log("Updating global_stats...")
c.execute('''CREATE TABLE IF NOT EXISTS global_stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    total_wallets INTEGER, total_events INTEGER, total_trades INTEGER,
    total_cities INTEGER, profitable INTEGER, losing INTEGER,
    total_profit REAL, total_loss REAL, updated_at TEXT
)''')

c.execute('''INSERT OR REPLACE INTO global_stats (id, total_wallets, total_events, total_trades,
    total_cities, profitable, losing, total_profit, total_loss, updated_at)
    SELECT 1,
        (SELECT COUNT(*) FROM wallets),
        (SELECT COUNT(*) FROM events),
        (SELECT COUNT(*) FROM trades),
        (SELECT COUNT(DISTINCT city) FROM events WHERE city NOT IN ('Other','DC','Dubai')),
        SUM(CASE WHEN total_pnl>0.01 THEN 1 ELSE 0 END),
        SUM(CASE WHEN total_pnl<-0.01 THEN 1 ELSE 0 END),
        COALESCE(SUM(CASE WHEN total_pnl>0 THEN total_pnl ELSE 0 END),0),
        COALESCE(SUM(CASE WHEN total_pnl<0 THEN total_pnl ELSE 0 END),0),
        datetime('now')
    FROM wallets''')
log("  global_stats updated")

# 3. city_all_stats  
log("Updating city_all_stats...")
c.execute('''CREATE TABLE IF NOT EXISTS city_all_stats (
    city TEXT PRIMARY KEY, wallets INTEGER, trades INTEGER, volume REAL, events INTEGER
)''')
c.execute('DELETE FROM city_all_stats')
c.execute('''INSERT INTO city_all_stats
    SELECT wc.city, COUNT(DISTINCT wc.wallet), SUM(wc.trades),
        ROUND(SUM(wc.spent+wc.recv+wc.near_settle)),
        (SELECT COUNT(*) FROM events e WHERE e.city=wc.city)
    FROM wallet_city wc GROUP BY wc.city''')
total = c.execute('SELECT COUNT(*) FROM city_all_stats').fetchone()[0]
log(f"  city_all_stats: {total} rows")

# 4. Add missing indexes (if not locked)
log("Adding indexes...")
try:
    c.execute('CREATE INDEX IF NOT EXISTS idx_trades_ts_city ON trades(timestamp, city)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wallets_pnl_a ON wallets(pnl_a)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wallets_curve ON wallets(curve_score)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wallets_total_pnl ON wallets(total_pnl)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet, timestamp DESC)')
    log("  Indexes created")
except Exception as e:
    log(f"  Index creation skipped: {e}")

conn.commit()

# 5. WAL checkpoint
log("WAL checkpoint...")
c.execute('PRAGMA wal_checkpoint(PASSIVE)')

conn.close()
log("Done!")
