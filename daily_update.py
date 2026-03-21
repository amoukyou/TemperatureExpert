#!/usr/bin/env python3
"""
Daily incremental update script for Temperature Expert Dashboard.
Run via cron: 0 */6 * * * python3 /opt/pm-expert/daily_update.py >> /opt/pm-expert/update.log 2>&1

Steps:
1. Discover new events from Gamma API
2. Discover missing conditionIds from top wallets' activity
3. Fetch new trades for all known conditionIds (incremental)
4. Update resolution results for closed events
5. Re-tag B-type strategies for new trades
6. Recalculate wallet stats
"""
import sqlite3, json, urllib.request, time, re, math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

DB = '/opt/pm-expert/pm_temperature.db'
LOG_PREFIX = datetime.now().strftime('%Y-%m-%d %H:%M')

def log(msg):
    print(f"[{LOG_PREFIX}] {msg}", flush=True)

def api_get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except:
        return None

conn = sqlite3.connect(DB)
c = conn.cursor()

# === Step 1: Discover new events from Gamma API ===
log("Step 1: Scanning Gamma API for new events...")
existing_eids = set(str(r[0]) for r in c.execute('SELECT event_id FROM events').fetchall())
new_events = []

for closed_val in ['true', 'false']:
    for offset in range(0, 50000, 100):
        data = api_get(f"https://gamma-api.polymarket.com/events?limit=100&offset={offset}&closed={closed_val}")
        if not data: break
        for e in data:
            title = e.get('title', '').lower()
            if 'temperature' in title and ('highest' in title or 'high temp' in title):
                if str(e['id']) not in existing_eids:
                    new_events.append(e)
                    existing_eids.add(str(e['id']))
        time.sleep(0.03)

log(f"  Found {len(new_events)} new events from Gamma API")

# Insert new events + markets
for e in new_events:
    eid = str(e['id'])
    match = re.search(r'temperature\s+in\s+(.+?)\s+on\s+(.+?)[\?]?\s*$', e['title'], re.I)
    city = match.group(1).strip() if match else 'Unknown'
    date_str = match.group(2).strip().rstrip('?') if match else ''
    c.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)',
        (eid, e['title'], e.get('slug',''), city, date_str,
         e.get('endDate',''), 1 if e.get('closed') else 0, e.get('volume',0)))
    for m in e.get('markets', []):
        prices = m.get('outcomePrices','')
        if isinstance(prices, str):
            try: prices = json.loads(prices)
            except: prices = []
        iw = 1 if (prices and len(prices)>=2 and e.get('closed') and float(prices[0])>=0.99) else 0
        c.execute('INSERT OR IGNORE INTO markets (event_id,condition_id,question,slug,is_winner) VALUES (?,?,?,?,?)',
            (eid, m.get('conditionId',''), m.get('question',''), m.get('slug',''), iw))
conn.commit()

# === Step 2: Discover missing conditionIds from top wallets ===
log("Step 2: Discovering missing conditionIds from top 50 wallets...")
our_cids = set(r[0] for r in c.execute('SELECT condition_id FROM markets').fetchall())
top_wallets = [r[0] for r in c.execute('SELECT wallet FROM wallets ORDER BY trades_count DESC LIMIT 50').fetchall()]

def scan_wallet(wallet):
    cids = {}
    start = None
    for page in range(100):
        params = f"user={wallet}&limit=500&type=TRADE"
        if start: params += f"&end={start}"
        data = api_get(f"https://data-api.polymarket.com/activity?{params}")
        if not data: break
        for a in data:
            if 'temperature' not in a.get('title','').lower(): continue
            cid = a.get('conditionId','')
            if cid: cids[cid] = a.get('title','')
        start = data[-1].get('timestamp')
        if len(data) < 500: break
    return cids

missing = {}
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(scan_wallet, w): w for w in top_wallets}
    for future in as_completed(futures):
        for cid, title in future.result().items():
            if cid not in our_cids: missing[cid] = title

log(f"  Found {len(missing)} missing conditionIds")

# Insert missing markets + events
events_group = defaultdict(list)
for cid, title in missing.items():
    m = re.search(r'temperature in (.+?) (?:be |on )', title, re.I)
    d = re.search(r' on (.+?)[\?]?$', title, re.I)
    city = m.group(1) if m else 'Unknown'
    date = d.group(1).strip().rstrip('?') if d else ''
    events_group[f"{city}|{date}"].append((cid, title))

for key, cids_list in events_group.items():
    city, date_str = key.split('|', 1)
    existing = c.execute("SELECT event_id FROM events WHERE city=? AND date=?", (city, date_str)).fetchone()
    eid = existing[0] if existing else f"syn_{abs(hash(key)) % 10**8}"
    if not existing:
        c.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)',
            (eid, f"Highest temperature in {city} on {date_str}?", '', city, date_str, '', 1, 0))
    for cid, q in cids_list:
        c.execute('INSERT OR IGNORE INTO markets (event_id,condition_id,question,slug,is_winner) VALUES (?,?,?,?,?)',
            (eid, cid, q, '', 0))
        our_cids.add(cid)
conn.commit()

# === Step 3: Fetch new trades ===
log("Step 3: Fetching new trades...")
# Get the latest timestamp we have
max_ts = c.execute('SELECT MAX(timestamp) FROM trades').fetchone()[0] or 0
log(f"  Latest trade timestamp: {max_ts} ({datetime.fromtimestamp(max_ts)})")

# Fetch trades for all conditionIds that might have new activity
all_cids = list(our_cids)
total_new = 0

def fetch_new_trades(cid):
    trades = []
    url = f"https://data-api.polymarket.com/trades?market={cid}&limit=1000&takerOnly=false"
    data = api_get(url, timeout=30)
    if data:
        trades = [t for t in data if t['timestamp'] > max_ts - 86400]  # Last 24h buffer
    return cid, trades

with ThreadPoolExecutor(max_workers=5) as executor:
    # Only check cids from recent events (last 7 days)
    recent_cids = [r[0] for r in c.execute('''
        SELECT DISTINCT m.condition_id FROM markets m
        JOIN events e ON m.event_id=e.event_id
        WHERE e.end_date > date('now', '-7 days') OR e.closed=0
    ''').fetchall()]
    # Plus all missing cids we just discovered
    check_cids = list(set(recent_cids) | set(missing.keys()))
    log(f"  Checking {len(check_cids)} conditionIds for new trades...")

    futures = {executor.submit(fetch_new_trades, cid): cid for cid in check_cids}
    for future in as_completed(futures):
        cid, trades = future.result()
        if not trades: continue
        # Get event info for this cid
        row = c.execute('SELECT event_id FROM markets WHERE condition_id=?', (cid,)).fetchone()
        if not row: continue
        eid = row[0]
        erow = c.execute('SELECT city, date FROM events WHERE event_id=?', (eid,)).fetchone()
        city = erow[0] if erow else ''
        date_str = erow[1] if erow else ''

        for t in trades:
            # Check if trade already exists (by condition_id + wallet + timestamp + price + size)
            exists = c.execute('SELECT 1 FROM trades WHERE condition_id=? AND wallet=? AND timestamp=? AND price=? LIMIT 1',
                (t['conditionId'], t['proxyWallet'], t['timestamp'], float(t['price']))).fetchone()
            if exists: continue

            p = float(t['price']); s = float(t['size'])
            c.execute('INSERT INTO trades (wallet,event_id,condition_id,side,outcome,price,size,amount,timestamp,city,event_date,name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                (t['proxyWallet'], eid, t['conditionId'], t['side'], t['outcome'],
                 p, s, round(p*s,4), t['timestamp'], city, date_str, t.get('name','')))
            total_new += 1

    conn.commit()

log(f"  Added {total_new} new trades")

# === Step 4: Update resolution results ===
log("Step 4: Updating resolution results...")
unresolved = c.execute('''
    SELECT DISTINCT e.event_id FROM events e
    WHERE e.closed=0 AND e.end_date < date('now')
''').fetchall()

updated = 0
for (eid,) in unresolved:
    data = api_get(f"https://gamma-api.polymarket.com/events/{eid}")
    if not data: continue
    if data.get('closed'):
        c.execute('UPDATE events SET closed=1 WHERE event_id=?', (eid,))
        for m in data.get('markets', []):
            prices = m.get('outcomePrices','')
            if isinstance(prices, str):
                try: prices = json.loads(prices)
                except: prices = []
            iw = 1 if (prices and len(prices)>=2 and float(prices[0])>=0.99) else 0
            c.execute('UPDATE markets SET is_winner=? WHERE condition_id=?', (iw, m.get('conditionId','')))
        updated += 1
    time.sleep(0.03)
conn.commit()
log(f"  Updated {updated} newly closed events")

# === Step 5+6: Run full recalc if there are new trades ===
if total_new > 0 or updated > 0 or len(new_events) > 0:
    log("Step 5+6: Running full recalculation...")
    import subprocess
    result = subprocess.run(['python3', '/opt/pm-expert/full_recalc.py'],
                          capture_output=True, text=True, timeout=1800)
    log(f"  Recalc output (last 500 chars): {result.stdout[-500:]}")
else:
    log("No new data, skipping recalculation")

# Final stats
events_count = c.execute('SELECT COUNT(*) FROM events').fetchone()[0]
trades_count = c.execute('SELECT COUNT(*) FROM trades').fetchone()[0]
wallets_count = c.execute('SELECT COUNT(*) FROM wallets').fetchone()[0]
log(f"Final: {events_count} events, {trades_count} trades, {wallets_count} wallets")

conn.close()
log("Update complete!")
