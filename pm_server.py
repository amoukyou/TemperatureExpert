#!/usr/bin/env python3
"""Polymarket Temperature Expert Dashboard - Backend API"""
import sqlite3, json, os, time
import urllib.request
from flask import Flask, jsonify, request, send_from_directory, g
from flask_cors import CORS

app = Flask(__name__, static_folder='/Users/victor', static_url_path='')
CORS(app)

DB_PATH = '/Users/victor/pm_temperature.db'
STATIC_DIR = '/Users/victor'

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute('PRAGMA journal_mode=WAL')
        g.db.execute('PRAGMA cache_size=-64000')
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db: db.close()

# === Static ===
@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'pm_temperature_dashboard.html')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)

# === Polymarket API Proxy ===
@app.route('/api/positions')
def proxy_positions():
    query = request.query_string.decode()
    url = f"https://data-api.polymarket.com/positions?{query}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return jsonify(json.loads(resp.read())), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 502

# === Wallets / Leaderboard ===
@app.route('/api/wallets')
def get_wallets():
    db = get_db()
    sort = request.args.get('sort', 'pnl_a')
    order = request.args.get('order', 'desc')
    page = int(request.args.get('page', 0))
    limit = int(request.args.get('limit', 200))
    search = request.args.get('search', '')

    allowed = {'total_pnl','pnl_a','pnl_low','total_spent','roi','conv','win_rate_a','events_a','events_low','trades_count','a7','avg_buy_low','max_opts','b_ratio','curve_score','sharpe','profit_factor','max_drawdown'}
    if sort not in allowed: sort = 'pnl_a'

    where = ['1=1']; params = []
    if search:
        where.append('(wallet LIKE ? OR name LIKE ?)'); params.extend([f'%{search}%',f'%{search}%'])
    for k,col,op in [
        ('min_pnl_a','pnl_a','>='),('min_pnl','total_pnl','>='),('min_spent','total_spent','>='),
        ('min_roi','roi','>='),('min_win_rate','win_rate_a','>='),
    ]:
        v = request.args.get(k)
        if v: where.append(f'{col} {op} ?'); params.append(float(v))
    for k,col,op in [
        ('min_events','events_a','>='),('max_opts','max_opts','<='),
    ]:
        v = request.args.get(k)
        if v: where.append(f'{col} {op} ?'); params.append(int(v))
    v = request.args.get('min_conv')
    if v: where.append('conv >= ?'); params.append(float(v)/100)
    v = request.args.get('max_avg_buy')
    if v: where.append('avg_buy_low <= ? AND avg_buy_low > 0'); params.append(float(v))
    v = request.args.get('max_b_ratio')
    if v: where.append('b_ratio <= ?'); params.append(float(v))
    v = request.args.get('min_curve_score')
    if v: where.append('curve_score >= ?'); params.append(float(v))
    v = request.args.get('active_days')
    if v:
        d=int(v); col='a7' if d<=7 else 'a14' if d<=14 else 'a30'
        where.append(f'{col} > 0')

    w = ' AND '.join(where)
    rows = db.execute(f'SELECT * FROM wallets WHERE {w} ORDER BY {sort} {"ASC" if order=="asc" else "DESC"} LIMIT ? OFFSET ?', params+[limit,page*limit]).fetchall()
    total = db.execute(f'SELECT COUNT(*) FROM wallets WHERE {w}', params).fetchone()[0]
    return jsonify({'total':total,'page':page,'limit':limit,'data':[dict(r) for r in rows]})

# === Wallet Detail ===
@app.route('/api/wallet/<wallet>')
def get_wallet(wallet):
    db = get_db()
    w = db.execute('SELECT * FROM wallets WHERE wallet=?',(wallet,)).fetchone()
    if not w: return jsonify({"error":"not found"}),404
    cities = db.execute('SELECT * FROM wallet_city WHERE wallet=?',(wallet,)).fetchall()
    return jsonify({'wallet':dict(w),'cities':[dict(c) for c in cities]})

# === Wallet Trades ===
@app.route('/api/wallet/<wallet>/trades')
def get_wallet_trades(wallet):
    db = get_db()
    eid = request.args.get('event_id')
    limit = int(request.args.get('limit', 5000))
    where = ['t.wallet=?']; params = [wallet]
    if eid: where.append('t.event_id=?'); params.append(eid)
    rows = db.execute(f'''
        SELECT t.side,t.outcome,t.price,t.size,t.amount,t.timestamp,t.condition_id,t.event_id,t.city,t.event_date,t.name,t.btag,
               e.title as event_title, m.question,
               CASE WHEN e.closed=1 THEN m.is_winner ELSE NULL END as won
        FROM trades t
        LEFT JOIN markets m ON t.condition_id=m.condition_id
        LEFT JOIN events e ON t.event_id=e.event_id
        WHERE {' AND '.join(where)}
        ORDER BY t.timestamp DESC LIMIT ?
    ''', params+[limit]).fetchall()
    return jsonify([dict(r) for r in rows])

# === Events ===
@app.route('/api/events')
def get_events():
    db = get_db()
    city = request.args.get('city')
    closed = request.args.get('closed')
    limit = int(request.args.get('limit',100))
    where=['1=1']; params=[]
    if city: where.append('city=?'); params.append(city)
    if closed is not None: where.append('closed=?'); params.append(int(closed))
    rows = db.execute(f'SELECT * FROM events WHERE {" AND ".join(where)} ORDER BY end_date DESC LIMIT ?', params+[limit]).fetchall()
    return jsonify([dict(r) for r in rows])

# === City Stats ===
@app.route('/api/cities')
def get_cities():
    db = get_db()
    days = request.args.get('days')
    if days and int(days) > 0:
        rows = db.execute('''
            SELECT t.city, COUNT(DISTINCT t.wallet) as wallets, COUNT(*) as trades,
                ROUND(SUM(t.amount)) as volume,
                COUNT(DISTINCT t.event_id) as events
            FROM trades t
            WHERE t.timestamp >= strftime('%s','now') - ? * 86400
            GROUP BY t.city ORDER BY volume DESC
        ''', (int(days),)).fetchall()
    else:
        rows = db.execute('''
            SELECT wc.city, COUNT(DISTINCT wc.wallet) as wallets, SUM(wc.trades) as trades,
                ROUND(SUM(wc.spent+wc.recv+wc.near_settle)) as volume,
                (SELECT COUNT(*) FROM events e WHERE e.city=wc.city) as events
            FROM wallet_city wc GROUP BY wc.city ORDER BY volume DESC
        ''').fetchall()
    return jsonify([dict(r) for r in rows])

# === Daily Stats ===
@app.route('/api/daily')
def get_daily():
    db = get_db()
    city = request.args.get('city')
    days = int(request.args.get('days', 90))
    where=['1=1']; params=[]
    if city: where.append('city=?'); params.append(city)
    rows = db.execute(f'''
        SELECT date(timestamp,'unixepoch') as day, COUNT(DISTINCT wallet) as wallets,
            COUNT(*) as trades, ROUND(SUM(amount)) as volume
        FROM trades WHERE {' AND '.join(where)}
        GROUP BY day ORDER BY day DESC LIMIT ?
    ''', params+[days]).fetchall()
    return jsonify([dict(r) for r in rows])

# === Groups ===
@app.route('/api/groups', methods=['GET'])
def get_groups():
    db = get_db()
    groups = db.execute('SELECT * FROM groups ORDER BY created_at').fetchall()
    result = []
    for gg in groups:
        ws = db.execute('SELECT gw.wallet,gw.display_name,w.btags,w.pnl_a,w.total_pnl,w.roi,w.win_rate_a,w.b_ratio FROM group_wallets gw LEFT JOIN wallets w ON gw.wallet=w.wallet WHERE gw.group_id=?',(gg['id'],)).fetchall()
        result.append({**dict(gg),'wallets':[dict(w) for w in ws]})
    return jsonify(result)

@app.route('/api/groups', methods=['POST'])
def create_group():
    db = get_db()
    data = request.json
    gid = data.get('id', f'g_{int(time.time()*1000)}')
    name = data.get('name','Untitled')
    db.execute('INSERT OR IGNORE INTO groups(id,name) VALUES(?,?)',(gid,name))
    db.commit()
    return jsonify({'id':gid,'name':name})

@app.route('/api/groups/<gid>', methods=['PUT'])
def update_group(gid):
    db = get_db()
    data = request.json
    if 'name' in data: db.execute('UPDATE groups SET name=? WHERE id=?',(data['name'],gid))
    db.commit()
    return jsonify({'ok':True})

@app.route('/api/groups/<gid>', methods=['DELETE'])
def delete_group(gid):
    db = get_db()
    db.execute('DELETE FROM group_wallets WHERE group_id=?',(gid,))
    db.execute('DELETE FROM groups WHERE id=?',(gid,))
    db.commit()
    return jsonify({'ok':True})

@app.route('/api/groups/<gid>/wallets', methods=['POST'])
def add_to_group(gid):
    db = get_db()
    data = request.json
    for w in data.get('wallets',[]):
        db.execute('INSERT OR IGNORE INTO group_wallets(group_id,wallet,display_name) VALUES(?,?,?)',
            (gid,w['wallet'],w.get('displayName','')))
    db.commit()
    return jsonify({'ok':True})

@app.route('/api/groups/<gid>/wallets/<wallet>', methods=['DELETE'])
def remove_from_group(gid, wallet):
    db = get_db()
    db.execute('DELETE FROM group_wallets WHERE group_id=? AND wallet=?',(gid,wallet))
    db.commit()
    return jsonify({'ok':True})

# === Filtered Stats (uses same wallet filters) ===
@app.route('/api/filtered_stats')
def filtered_stats():
    db = get_db()
    where = ['1=1']; params = []
    for k,col,op in [('min_pnl_a','pnl_a','>='),('min_pnl','total_pnl','>='),('min_spent','total_spent','>='),
        ('min_roi','roi','>='),('min_win_rate','win_rate_a','>=')]:
        v = request.args.get(k)
        if v: where.append(f'{col} {op} ?'); params.append(float(v))
    for k,col,op in [('min_events','events_a','>='),('max_opts','max_opts','<=')]:
        v = request.args.get(k)
        if v: where.append(f'{col} {op} ?'); params.append(int(v))
    v = request.args.get('min_conv')
    if v: where.append('conv >= ?'); params.append(float(v)/100)
    v = request.args.get('max_avg_buy')
    if v: where.append('avg_buy_low <= ? AND avg_buy_low > 0'); params.append(float(v))
    v = request.args.get('max_b_ratio')
    if v: where.append('b_ratio <= ?'); params.append(float(v))
    v = request.args.get('min_curve_score')
    if v: where.append('curve_score >= ?'); params.append(float(v))
    v = request.args.get('active_days')
    if v:
        d=int(v); col='a7' if d<=7 else 'a14' if d<=14 else 'a30'
        where.append(f'{col} > 0')
    search = request.args.get('search','')
    if search:
        where.append('(wallet LIKE ? OR name LIKE ?)'); params.extend([f'%{search}%',f'%{search}%'])
    w = ' AND '.join(where)
    row = db.execute(f'''
        SELECT COUNT(*) as total,
            SUM(CASE WHEN pnl_a>0.01 THEN 1 ELSE 0 END) as profitable,
            SUM(CASE WHEN pnl_a<-0.01 THEN 1 ELSE 0 END) as losing,
            ROUND(SUM(total_spent)) as total_spent,
            ROUND(SUM(pnl_a)) as total_pnl_a,
            ROUND(SUM(total_pnl)) as total_pnl
        FROM wallets WHERE {w}
    ''', params).fetchone()
    return jsonify(dict(row))

# === Filtered Daily (uses same wallet filters) ===
@app.route('/api/filtered_daily')
def filtered_daily():
    db = get_db()
    days = int(request.args.get('days', 90))

    # Build same wallet filter as /api/wallets
    where = ['1=1']; params = []
    for k,col,op in [('min_pnl_a','pnl_a','>='),('min_pnl','total_pnl','>='),('min_spent','total_spent','>='),
        ('min_roi','roi','>='),('min_win_rate','win_rate_a','>=')]:
        v = request.args.get(k)
        if v: where.append(f'{col} {op} ?'); params.append(float(v))
    for k,col,op in [('min_events','events_a','>='),('max_opts','max_opts','<=')]:
        v = request.args.get(k)
        if v: where.append(f'{col} {op} ?'); params.append(int(v))
    v = request.args.get('min_conv')
    if v: where.append('conv >= ?'); params.append(float(v)/100)
    v = request.args.get('max_avg_buy')
    if v: where.append('avg_buy_low <= ? AND avg_buy_low > 0'); params.append(float(v))
    v = request.args.get('max_b_ratio')
    if v: where.append('b_ratio <= ?'); params.append(float(v))
    v = request.args.get('min_curve_score')
    if v: where.append('curve_score >= ?'); params.append(float(v))
    v = request.args.get('active_days')
    if v:
        d=int(v); col='a7' if d<=7 else 'a14' if d<=14 else 'a30'
        where.append(f'{col} > 0')
    search = request.args.get('search','')
    if search:
        where.append('(wallet LIKE ? OR name LIKE ?)'); params.extend([f'%{search}%',f'%{search}%'])

    w = ' AND '.join(where)
    rows = db.execute(f'''
        SELECT date(t.timestamp,'unixepoch') as day, COUNT(DISTINCT t.wallet) as wallets
        FROM trades t
        WHERE t.wallet IN (SELECT wallet FROM wallets WHERE {w})
        GROUP BY day ORDER BY day DESC LIMIT ?
    ''', params + [days]).fetchall()
    return jsonify([dict(r) for r in rows])

# === Stats ===
@app.route('/api/stats')
def get_stats():
    db = get_db()
    total_w = db.execute('SELECT COUNT(*) FROM wallets').fetchone()[0]
    profitable = db.execute('SELECT COUNT(*) FROM wallets WHERE total_pnl>0.01').fetchone()[0]
    losing = db.execute('SELECT COUNT(*) FROM wallets WHERE total_pnl<-0.01').fetchone()[0]
    unsettled_w = total_w - profitable - losing

    total_profit = db.execute('SELECT COALESCE(SUM(total_pnl),0) FROM wallets WHERE total_pnl>0').fetchone()[0]
    total_loss = db.execute('SELECT COALESCE(SUM(total_pnl),0) FROM wallets WHERE total_pnl<0').fetchone()[0]

    unsettled_vol = db.execute('''SELECT COALESCE(SUM(t.amount),0) FROM trades t
        JOIN events e ON t.event_id=e.event_id WHERE e.closed=0''').fetchone()[0]
    no_winner_vol = db.execute('''SELECT COALESCE(SUM(t.amount),0) FROM trades t
        JOIN events e ON t.event_id=e.event_id WHERE e.closed=1
        AND (SELECT COUNT(*) FROM markets m WHERE m.event_id=e.event_id AND m.is_winner=1)=0''').fetchone()[0]

    return jsonify({
        'total_wallets': total_w,
        'total_events': db.execute('SELECT COUNT(*) FROM events').fetchone()[0],
        'total_trades': db.execute('SELECT COUNT(*) FROM trades').fetchone()[0],
        'total_cities': db.execute("SELECT COUNT(DISTINCT city) FROM events WHERE city NOT IN ('Other','DC','Dubai')").fetchone()[0],
        'profitable': profitable,
        'losing': losing,
        'unsettled_wallets': unsettled_w,
        'total_profit': round(total_profit),
        'total_loss': round(total_loss),
        'unsettled_volume': round(unsettled_vol),
        'no_winner_volume': round(no_winner_vol),
    })

if __name__ == '__main__':
    print(f"Database: {DB_PATH} ({os.path.getsize(DB_PATH)/1024/1024:.0f} MB)")
    print(f"Server: http://localhost:8899")
    app.run(host='0.0.0.0', port=8899, debug=False)
