#!/usr/bin/env python3
"""
Fetch REDEEM/SPLIT/MERGE/REBATE activities for temperature markets.
Uses 'end' cursor pagination (no 3000 offset limit).
"""
import sqlite3, json, urllib.request, time, sys, re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = sys.argv[1] if len(sys.argv) > 1 else '/opt/pm-expert/pm_temperature.db'

conn = sqlite3.connect(DB)
c = conn.cursor()

# Get all temperature conditionIds
our_cids = set(r[0] for r in c.execute('SELECT condition_id FROM markets').fetchall())
print(f"Temperature conditionIds: {len(our_cids)}", flush=True)

# Get top wallets by trade count
N = 500
wallets = [r[0] for r in c.execute('SELECT wallet FROM wallets ORDER BY trades_count DESC LIMIT ?', (N,)).fetchall()]
print(f"Scanning {len(wallets)} wallets for REDEEM/SPLIT/MERGE...", flush=True)

# Also find missing conditionIds (discovery)
missing_cids = {}

def scan_wallet(wallet):
    """Fetch ALL activity for a wallet using 'end' cursor pagination"""
    activities = []
    new_cids = {}
    end_ts = None
    
    for page in range(200):  # up to 100K activities
        params = f"user={wallet}&limit=500"
        if end_ts: params += f"&end={end_ts}"
        
        try:
            req = urllib.request.Request(
                f"https://data-api.polymarket.com/activity?{params}",
                headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                batch = json.loads(resp.read())
        except:
            break
        if not batch:
            break
        
        for a in batch:
            title = a.get('title', '')
            if 'temperature' not in title.lower():
                continue
            
            cid = a.get('conditionId', '')
            atype = a.get('type', '')
            
            # Discover missing conditionIds
            if cid and cid not in our_cids:
                new_cids[cid] = title
            
            # Collect non-TRADE activities
            if atype in ('REDEEM', 'SPLIT', 'MERGE', 'REWARD', 'CONVERSION'):
                activities.append({
                    'wallet': a.get('proxyWallet', wallet),
                    'cid': cid,
                    'type': atype,
                    'size': float(a.get('size', 0)),
                    'usdc': float(a.get('usdcSize', 0)),
                    'ts': a.get('timestamp', 0),
                    'title': title,
                })
        
        # Use 'end' cursor: timestamp of last item minus 1
        end_ts = batch[-1].get('timestamp', 0) - 1
        if len(batch) < 500:
            break
    
    return wallet, activities, new_cids

total_acts = 0
total_new_cids = 0
done = 0

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(scan_wallet, w): w for w in wallets}
    
    for future in as_completed(futures):
        wallet, activities, new_cids = future.result()
        done += 1
        
        # Insert activities
        for a in activities:
            # Find event_id from conditionId
            row = c.execute('SELECT event_id FROM markets WHERE condition_id=?', (a['cid'],)).fetchone()
            eid = row[0] if row else ''
            erow = c.execute('SELECT city, date FROM events WHERE event_id=?', (eid,)).fetchone() if eid else None
            city = erow[0] if erow else ''
            date_str = erow[1] if erow else ''
            
            c.execute('''INSERT OR IGNORE INTO activities 
                (wallet, event_id, condition_id, type, size, usdc_size, timestamp, city, event_date)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (a['wallet'], eid, a['cid'], a['type'], a['size'], a['usdc'], a['ts'], city, date_str))
            total_acts += 1
        
        # Track missing cids
        for cid, title in new_cids.items():
            if cid not in our_cids:
                missing_cids[cid] = title
                total_new_cids += 1
        
        if done % 50 == 0:
            conn.commit()
            print(f"  [{done}/{len(wallets)}] activities={total_acts}, new_cids={len(missing_cids)}", flush=True)

conn.commit()

# Stats
print(f"\nDone! Scanned {done} wallets", flush=True)
print(f"Activities inserted: {total_acts}", flush=True)
print(f"Missing conditionIds discovered: {len(missing_cids)}", flush=True)

for atype in ['REDEEM', 'SPLIT', 'MERGE', 'REWARD', 'CONVERSION']:
    cnt = c.execute('SELECT COUNT(*) FROM activities WHERE type=?', (atype,)).fetchone()[0]
    if cnt > 0:
        print(f"  {atype}: {cnt}", flush=True)

conn.close()
