#!/usr/bin/env python3
"""
Streaming + incremental recalculation for Temperature Expert.

Usage:
  python3 full_recalc.py              # Incremental (only dirty wallets)
  python3 full_recalc.py --full       # Full recalc (streaming, low memory)
  python3 full_recalc.py --wallets=w1,w2  # Recalc specific wallets

Memory: ~50MB peak (streams one wallet at a time)
vs old version: ~5GB (loaded everything)
"""
import sqlite3, math, sys, os
from collections import defaultdict
from datetime import datetime, date

DB = os.environ.get('PM_DB', '/opt/pm-expert/pm_temperature.db')
MODE = 'incremental'
TARGET_WALLETS = None

for arg in sys.argv[1:]:
    if arg == '--full': MODE = 'full'
    elif arg.startswith('--wallets='): TARGET_WALLETS = arg.split('=')[1].split(',')
    elif arg.startswith('--db='): DB = arg.split('=')[1]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

conn = sqlite3.connect(DB)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA cache_size=-64000')
c = conn.cursor()

# Resolution map (only closed events)
resolved = {}
for row in c.execute('SELECT m.condition_id, m.is_winner FROM markets m JOIN events e ON m.event_id=e.event_id WHERE e.closed=1'):
    resolved[row[0]] = row[1]
log(f"Resolved: {len(resolved)} conditionIds")

# Event conditionId sets
event_cids = defaultdict(set)
for row in c.execute('SELECT event_id, condition_id FROM markets'):
    event_cids[row[0]].add(row[1])

# Determine wallets to process
if TARGET_WALLETS:
    wallets = TARGET_WALLETS
    log(f"Mode: targeted, {len(wallets)} wallets")
elif MODE == 'incremental':
    rows = c.execute("SELECT DISTINCT wallet FROM trades WHERE needs_retag=1").fetchall()
    wallets = [r[0] for r in rows]
    if not wallets:
        log("No dirty wallets, nothing to do")
        conn.close(); sys.exit(0)
    log(f"Mode: incremental, {len(wallets)} wallets")
else:
    wallets = [r[0] for r in c.execute('SELECT wallet FROM wallets').fetchall()]
    # Also find wallets in trades but not in wallets table
    extra = c.execute('SELECT DISTINCT wallet FROM trades WHERE wallet NOT IN (SELECT wallet FROM wallets)').fetchall()
    wallets += [r[0] for r in extra]
    log(f"Mode: full, {len(wallets)} wallets")

today = date.today()
now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
processed = 0

def tag_event(st, eid):
    """Tag trades in one event, return {trade_id: btag_str}"""
    tm = {t['id']:[] for t in st}
    cn = {'b1':0,'b2':0,'b3':0,'b4':0,'b5':0,'b7':0}

    for t in st:
        if t['side']=='BUY' and t['price']>=0.95:
            cn['b1']+=1; tm[t['id']].append(f"B1.{cn['b1']:03d}")

    if not any(t['side']=='BUY' for t in st) and any(t['side']=='SELL' for t in st):
        cn['b5']+=1; tag=f"B5.{cn['b5']:03d}"
        for t in st: tm[t['id']].append(tag)

    cb = defaultdict(lambda:{'Yes':[],'No':[]})
    for t in st:
        if t['side']=='BUY': cb[t['cid']][t['oc']].append(t)
    for _,sides in cb.items():
        if not sides['Yes'] or not sides['No']: continue
        ok=False
        for yt in sides['Yes']:
            for nt in sides['No']:
                if abs(yt['ts']-nt['ts'])<=300:
                    ya=sum(x['amount'] for x in sides['Yes']); na=sum(x['amount'] for x in sides['No'])
                    if ya>0 and na>0 and 0.5<=ya/na<=2: ok=True; break
            if ok: break
        if ok:
            cn['b4']+=1; tag=f"B4.{cn['b4']:03d}"
            for t in sides['Yes']+sides['No']: tm[t['id']].append(tag)

    ec = event_cids.get(eid, set(t['cid'] for t in st))
    if len(ec)>=3:
        for btype,sf,of in [('b2','SELL','Yes'),('b3','BUY','Yes')]:
            fl=[t for t in st if t['side']==sf and t['oc']==of]
            if not fl: continue
            fs=sorted(fl,key=lambda t:t['ts'])
            for i in range(len(fs)):
                w2=set();wt=[]
                for j in range(i,len(fs)):
                    if fs[j]['ts']-fs[i]['ts']>300: break
                    w2.add(fs[j]['cid']); wt.append(fs[j])
                if len(w2)>=len(ec)*0.7 and len(w2)>=3:
                    ca=defaultdict(float)
                    for t in wt: ca[t['cid']]+=t['amount']
                    a=list(ca.values())
                    if max(a)/max(min(a),0.001)<=3:
                        cn[btype]+=1; tag=f"{btype.upper()}.{cn[btype]:03d}"
                        for t in wt: tm[t['id']].append(tag)
                    break

    logical=[]
    for t in st:
        key=(t['cid'],t['side'],t['oc'],round(t['price'],4))
        if logical and logical[-1]['key']==key and t['ts']-logical[-1]['te']<=2:
            logical[-1]['te']=t['ts']; logical[-1]['trades'].append(t)
        else:
            logical.append({'key':key,'ts':t['ts'],'te':t['ts'],'trades':[t]})
    burst=set()
    if len(logical)>=5:
        for i in range(len(logical)-4):
            if logical[i+4]['ts']-logical[i]['ts']<=300:
                for j in range(i,min(i+5,len(logical))):
                    for t in logical[j]['trades']: burst.add(t['id'])
    bcids=set(t['cid'] for t in st if t['side']=='BUY' and t['oc']=='Yes')
    isB3=any('B3' in ' '.join(tm[t['id']]) for t in st)
    multi=len(bcids)>=3 and not isB3
    if burst or multi:
        cn['b7']+=1; tag=f"B7.{cn['b7']:03d}"
        for tid in burst:
            if tag not in tm.get(tid,[]): tm.setdefault(tid,[]).append(tag)
        if multi:
            for t in st:
                if t['side']=='BUY' and t['oc']=='Yes' and t['cid'] in bcids:
                    if tag not in tm[t['id']]: tm[t['id']].append(tag)

    return {tid: ' '.join(tgs) for tid, tgs in tm.items()}

# === Main loop: process each wallet ===
for wallet in wallets:
    processed += 1
    if processed % 5000 == 0:
        conn.commit()
        log(f"  {processed}/{len(wallets)}...")

    rows = c.execute("SELECT DISTINCT wallet FROM trades WHERE needs_retag=1").fetchall()
    if not rows: continue

    events = defaultdict(list)
    for r in rows:
        events[r[1]].append({'id':r[0],'eid':r[1],'cid':r[2],'side':r[3],'oc':r[4],'price':r[5],'size':r[6],'ts':r[7],'amount':round(r[5]*r[6],4)})

    # Phase 1: B-type tags
    wallet_btags = set()
    btag_updates = []
    tagged_ids = set()
    for eid, ev in events.items():
        st = sorted(ev, key=lambda x: x['ts'])
        result = tag_event(st, eid)
        for tid, btag_str in result.items():
            btag_updates.append((btag_str, tid))
            if btag_str:
                tagged_ids.add(tid)
                for tg in btag_str.split(): wallet_btags.add(tg[:2])

    c.execute('UPDATE trades SET btag="", needs_retag=0 WHERE wallet=?', (wallet,))
    if btag_updates:
        c.executemany('UPDATE trades SET btag=? WHERE id=?', btag_updates)

    # Phase 2: Stats
    tp=0;ts_=0;tr=0;tst=0;tn=0;pa=0;ea=0;wa_=0;la_=0;te=0;tt=0;eps=[]
    trade_dates = set()

    for eid, ev in events.items():
        te+=1; tt+=len(ev)
        sp=0;rv=0;nr=0;pos=defaultdict(float)
        asp=0;apos=defaultdict(float);arv=0;has_a=False
        for t in ev:
            amt=t['amount']; is_a=t['id'] not in tagged_ids
            trade_dates.add(datetime.fromtimestamp(t['ts']).date())
            if t['side']=='BUY':
                sp+=amt;pos[(t['cid'],t['oc'])]+=t['size']
                if is_a:asp+=amt;apos[(t['cid'],t['oc'])]+=t['size'];has_a=True
            else:
                if t['price']>=0.98:nr+=amt
                else:rv+=amt
                pos[(t['cid'],t['oc'])]-=t['size']
                if is_a:arv+=amt;apos[(t['cid'],t['oc'])]-=t['size']
        stl=0;astl=0;hr=False
        for(ci,oc),ns in pos.items():
            if ci in resolved:hr=True;yw=resolved[ci];sv=1.0 if((oc=='Yes'and yw)or(oc=='No'and not yw))else 0.0;stl+=ns*sv
        for(ci,oc),ns in apos.items():
            if ci in resolved:yw=resolved[ci];sv=1.0 if((oc=='Yes'and yw)or(oc=='No'and not yw))else 0.0;astl+=ns*sv
        ep=rv+nr+stl-sp;tp+=ep;ts_+=sp;tr+=rv;tst+=stl;tn+=nr;eps.append(ep)
        if has_a:
            ap=arv+astl-asp;pa+=ap
            if hr and asp>0:
                ea+=1
                if ap>0.01:wa_+=1
                elif ap<-0.01:la_+=1

    roi=tp/ts_*100 if ts_>0 else 0
    tret=tr+tst+tn;sn=tst+tn;conv=min(sn/tret,1) if tret>0 and sn>=0 else 0
    wra=wa_/ea*100 if ea>0 else 0
    btstr=','.join(sorted(wallet_btags))
    bamt=sum(t['amount'] for ev in events.values() for t in ev if t['id'] in tagged_ids)
    tamt=sum(t['amount'] for ev in events.values() for t in ev)
    br=bamt/tamt*100 if tamt>0 else 0

    # Curve
    if len(eps)>=2:
        cum=[0]
        for p in eps:cum.append(cum[-1]+p)
        pk=cum[0];mdd=0
        for v in cum:
            if v>pk:pk=v;
            mdd=max(mdd,pk-v)
        ws2=sum(p for p in eps if p>0);ls2=abs(sum(p for p in eps if p<0))
        pf=min(ws2/ls2 if ls2>0.01 else(99 if ws2>0 else 0),99)
        mp=sum(eps)/len(eps);vp=sum((p-mp)**2 for p in eps)/len(eps)
        sp2=math.sqrt(vp) if vp>0 else 0.001;sh=mp/sp2
        rf=min(tp/mdd if mdd>0.01 else(99 if tp>0 else 0),99)
        mw=0;ml=0;cw=0;cl=0
        for p in eps:
            if p>0.01:cw+=1;cl=0;mw=max(mw,cw)
            elif p<-0.01:cl+=1;cw=0;ml=max(ml,cl)
            else:cw=0;cl=0
        s1=min(max(sh,-2),3)/3*100;s2=min(pf,5)/5*100;s3=min(max(rf,0),10)/10*100
        s4=max(0,100-mdd/max(abs(tp),1)*100) if tp>0 else 0;s5=mw/max(mw+ml,1)*100
        cs=max(0,min(100,round(s1*0.3+s2*0.25+s3*0.2+s4*0.15+s5*0.1,1)))
    else:
        mdd=0;pf=0;sh=0;rf=0;mw=0;ml=0;cs=0

    a7=sum(1 for d in trade_dates if(today-d).days<7)
    a14=sum(1 for d in trade_dates if(today-d).days<14)
    a30=sum(1 for d in trade_dates if(today-d).days<30)
    la=max(trade_dates).isoformat() if trade_dates else ''

    # Get name
    nm_row = c.execute('SELECT name FROM trades WHERE wallet=? AND name!="" LIMIT 1',(wallet,)).fetchone()
    nm = nm_row[0] if nm_row else ''

    c.execute('''INSERT OR REPLACE INTO wallets (wallet,name,total_pnl,total_spent,total_recv,total_settle,total_near,
        roi,conv,events_total,trades_count,pnl_a,events_a,wins_a,losses_a,win_rate_a,
        btags,b_ratio,max_drawdown,profit_factor,sharpe,recovery_factor,
        max_win_streak,max_lose_streak,curve_score,a7,a14,a30,last_active,last_updated)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (wallet,nm,round(tp,2),round(ts_,2),round(tr,2),round(tst,2),round(tn,2),
         round(roi,2),round(conv,4),te,tt,round(pa,2),ea,wa_,la_,round(wra,1),
         btstr,round(br,1),round(mdd,2),round(pf,2),round(sh,3),round(rf,2),mw,ml,cs,
         a7,a14,a30,la,now_str))

conn.commit()
log(f"\nDone! Processed {processed} wallets in {MODE} mode")
log(f"Events: {c.execute('SELECT COUNT(*) FROM events').fetchone()[0]}")
log(f"Trades: {c.execute('SELECT COUNT(*) FROM trades').fetchone()[0]}")
log(f"Wallets: {c.execute('SELECT COUNT(*) FROM wallets').fetchone()[0]}")
conn.close()
