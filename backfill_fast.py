#!/usr/bin/env python3
"""Fast backfill with concurrent scanning"""
import urllib.request, json, sqlite3, time, re, sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = '/Users/victor/pm_temperature.db'

def scan_wallet(wallet):
    """Scan one wallet's activity, return set of temperature conditionIds"""
    cids = {}
    start = None
    for page in range(100):
        params = f"user={wallet}&limit=500&type=TRADE"
        if start: params += f"&end={start}"
        try:
            req = urllib.request.Request(f"https://data-api.polymarket.com/activity?{params}", headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                batch = json.loads(resp.read())
        except: break
        if not batch: break
        for a in batch:
            if 'temperature' not in a.get('title','').lower(): continue
            cid = a.get('conditionId','')
            if cid: cids[cid] = a.get('title','')
        start = batch[-1].get('timestamp')
        if len(batch) < 500: break
    return cids

def fetch_trades(cid):
    """Fetch all trades for a conditionId"""
    all_trades = []
    offset = 0
    while True:
        url = f"https://data-api.polymarket.com/trades?market={cid}&limit=10000&offset={offset}&takerOnly=false"
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                tds = json.loads(resp.read())
        except: break
        if not tds: break
        all_trades.extend(tds)
        if len(tds) < 10000: break
        offset += 10000
    return all_trades

conn = sqlite3.connect(DB)
c = conn.cursor()
our_cids = set(r[0] for r in c.execute('SELECT condition_id FROM markets').fetchall())
print(f"Starting conditionIds: {len(our_cids)}", flush=True)

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200
wallets = [r[0] for r in c.execute('SELECT wallet FROM wallets ORDER BY trades_count DESC LIMIT ?', (N,)).fetchall()]
print(f"Scanning {len(wallets)} wallets with 10 threads...", flush=True)

# Phase 1: Discover missing cids (parallel)
missing = {}
done = 0
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(scan_wallet, w): w for w in wallets}
    for future in as_completed(futures):
        result = future.result()
        for cid, title in result.items():
            if cid not in our_cids:
                missing[cid] = title
        done += 1
        if done % 20 == 0:
            print(f"  [{done}/{len(wallets)}] discovered {len(missing)} missing cids", flush=True)

print(f"\nTotal missing: {len(missing)}", flush=True)

# Group by event
events_group = defaultdict(list)
for cid, title in missing.items():
    m = re.search(r'temperature in (.+?) (?:be |on )', title, re.I)
    d = re.search(r' on (.+?)[\?]?$', title, re.I)
    city = m.group(1) if m else 'Unknown'
    date = d.group(1).strip().rstrip('?') if d else ''
    events_group[f"{city}|{date}"].append((cid, title))

print(f"Grouped into {len(events_group)} events", flush=True)

# Phase 2: Fetch trades (parallel, 5 threads)
print("Fetching trades...", flush=True)
total_trades = 0; new_events = 0
all_cids_to_fetch = list(missing.keys())

# First insert all events and markets
for key, cids_list in events_group.items():
    city, date_str = key.split('|', 1)
    existing = c.execute("SELECT event_id FROM events WHERE city=? AND date=?", (city, date_str)).fetchone()
    eid = existing[0] if existing else f"syn_{abs(hash(key)) % 10**8}"
    if not existing:
        c.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)',
            (eid, f"Highest temperature in {city} on {date_str}?", '', city, date_str, '', 1, 0))
        new_events += 1
    for cid, q_title in cids_list:
        c.execute('INSERT OR IGNORE INTO markets (event_id,condition_id,question,slug,is_winner) VALUES (?,?,?,?,?)',
            (eid, cid, q_title, '', 0))
conn.commit()
print(f"Inserted {new_events} events, {len(missing)} markets", flush=True)

# Build cid → eid mapping
cid_eid = {}
for key, cids_list in events_group.items():
    city, date_str = key.split('|', 1)
    existing = c.execute("SELECT event_id FROM events WHERE city=? AND date=?", (city, date_str)).fetchone()
    if existing:
        for cid, _ in cids_list:
            cid_eid[cid] = (existing[0], city, date_str)

# Fetch trades in parallel
fetched = 0
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(fetch_trades, cid): cid for cid in all_cids_to_fetch}
    for future in as_completed(futures):
        cid = futures[future]
        trades = future.result()
        if trades and cid in cid_eid:
            eid, city, date_str = cid_eid[cid]
            tb = []
            for t in trades:
                p=float(t['price']);s=float(t['size'])
                tb.append((t['proxyWallet'],eid,t['conditionId'],t['side'],t['outcome'],
                    p,s,round(p*s,4),t['timestamp'],city,date_str,t.get('name','')))
            c.executemany('INSERT INTO trades (wallet,event_id,condition_id,side,outcome,price,size,amount,timestamp,city,event_date,name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', tb)
            total_trades += len(tb)
        
        fetched += 1
        if fetched % 100 == 0:
            conn.commit()
            print(f"  [{fetched}/{len(all_cids_to_fetch)}] {total_trades} trades", flush=True)

conn.commit()
print(f"\nDone! Events: {new_events}, Trades: {total_trades}", flush=True)
print(f"Total events: {c.execute('SELECT COUNT(*) FROM events').fetchone()[0]}", flush=True)
print(f"Total markets: {c.execute('SELECT COUNT(*) FROM markets').fetchone()[0]}", flush=True)
print(f"Total trades: {c.execute('SELECT COUNT(*) FROM trades').fetchone()[0]}", flush=True)
conn.close()
