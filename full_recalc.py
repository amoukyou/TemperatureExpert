import sqlite3, math
from collections import defaultdict

DB = '/Users/victor/pm_temperature.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

resolved = {}
for row in c.execute('SELECT m.condition_id, m.is_winner FROM markets m JOIN events e ON m.event_id=e.event_id WHERE e.closed=1'):
    resolved[row[0]] = row[1]
print(f"Resolved conditionIds: {len(resolved)}")

print("\n=== Step 1: B-type tagging ===")
c.execute("UPDATE trades SET btag=''")
conn.commit()

event_cids = defaultdict(set)
for row in c.execute('SELECT event_id, condition_id FROM markets'):
    event_cids[row[0]].add(row[1])

print("Loading trades...")
wallet_events = defaultdict(lambda: defaultdict(list))
for row in c.execute('SELECT id,wallet,event_id,condition_id,side,outcome,price,size,timestamp FROM trades ORDER BY timestamp'):
    tid,wallet,eid,cid,side,oc,price,size,ts = row
    wallet_events[wallet][eid].append({'id':tid,'cid':cid,'side':side,'oc':oc,'price':price,'size':size,'ts':ts,'amount':round(price*size,4)})

print(f"Wallets: {len(wallet_events)}")
updates = []
wc = 0

for wallet, events in wallet_events.items():
    wc += 1
    if wc % 10000 == 0:
        print(f"  Tagging {wc}...")
        if updates:
            c.executemany('UPDATE trades SET btag=? WHERE id=?', updates); conn.commit(); updates = []
    
    for eid, trades in events.items():
        st = sorted(trades, key=lambda x: x['ts'])
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
        
        ac = set(t['cid'] for t in st)
        ec = event_cids.get(eid, ac)
        if len(ec)>=3:
            sy=[t for t in st if t['side']=='SELL' and t['oc']=='Yes']
            if sy:
                ss=sorted(sy, key=lambda t:t['ts'])
                for i in range(len(ss)):
                    w2=set();wt2=[]
                    for j in range(i,len(ss)):
                        if ss[j]['ts']-ss[i]['ts']>300:break
                        w2.add(ss[j]['cid']);wt2.append(ss[j])
                    if len(w2)>=len(ec)*0.7 and len(w2)>=3:
                        ca=defaultdict(float)
                        for t in wt2:ca[t['cid']]+=t['amount']
                        amts=list(ca.values())
                        if max(amts)/max(min(amts),0.001)<=3:
                            cn['b2']+=1;tag=f"B2.{cn['b2']:03d}"
                            for t in wt2:tm[t['id']].append(tag)
                        break
            by=[t for t in st if t['side']=='BUY' and t['oc']=='Yes']
            if by:
                bs=sorted(by,key=lambda t:t['ts'])
                for i in range(len(bs)):
                    w3=set();wt3=[]
                    for j in range(i,len(bs)):
                        if bs[j]['ts']-bs[i]['ts']>300:break
                        w3.add(bs[j]['cid']);wt3.append(bs[j])
                    if len(w3)>=len(ec)*0.7 and len(w3)>=3:
                        ca=defaultdict(float)
                        for t in wt3:ca[t['cid']]+=t['amount']
                        amts=list(ca.values())
                        if max(amts)/max(min(amts),0.001)<=3:
                            cn['b3']+=1;tag=f"B3.{cn['b3']:03d}"
                            for t in wt3:tm[t['id']].append(tag)
                        break
        
        logical=[]
        for t in st:
            key=(t['cid'],t['side'],t['oc'],round(t['price'],4))
            if logical and logical[-1]['key']==key and t['ts']-logical[-1]['te']<=2:
                logical[-1]['te']=t['ts'];logical[-1]['trades'].append(t)
            else:
                logical.append({'key':key,'ts':t['ts'],'te':t['ts'],'trades':[t]})
        burst=set()
        if len(logical)>=5:
            for i in range(len(logical)-4):
                if logical[i+4]['ts']-logical[i]['ts']<=300:
                    for j in range(i,min(i+5,len(logical))):
                        for t in logical[j]['trades']:burst.add(t['id'])
        bcids=set(t['cid'] for t in st if t['side']=='BUY' and t['oc']=='Yes')
        isB3=any('B3' in ' '.join(tm[t['id']]) for t in st)
        multi=len(bcids)>=3 and not isB3
        if burst or multi:
            cn['b7']+=1;tag=f"B7.{cn['b7']:03d}"
            for tid in burst:
                if tag not in tm.get(tid,[]):tm.setdefault(tid,[]).append(tag)
            if multi:
                for t in st:
                    if t['side']=='BUY' and t['oc']=='Yes' and t['cid'] in bcids:
                        if tag not in tm[t['id']]:tm[t['id']].append(tag)
        
        for tid,tgs in tm.items():
            if tgs:updates.append((' '.join(tgs),tid))

if updates:
    c.executemany('UPDATE trades SET btag=? WHERE id=?',updates);conn.commit()

for b in ['B1','B2','B3','B4','B5','B7']:
    q = f"SELECT COUNT(*) FROM trades WHERE btag LIKE '%{b}%'"
    print(f"  {b}: {c.execute(q).fetchone()[0]} trades")

# === Step 2: Wallet stats ===
print("\n=== Step 2: Wallet stats + curve ===")

wallet_tags = defaultdict(set)
for row in c.execute("SELECT wallet,btag FROM trades WHERE btag!=''"):
    for tag in row[1].split():wallet_tags[row[0]].add(tag[:2])

trade_btags = set()
for row in c.execute("SELECT id FROM trades WHERE btag!=''"):
    trade_btags.add(row[0])

batch = [];proc = 0
for wallet, events in wallet_events.items():
    proc += 1
    if proc % 10000 == 0:
        print(f"  Stats {proc}...")
        if batch:
            c.executemany('UPDATE wallets SET total_pnl=?,total_spent=?,total_recv=?,total_settle=?,total_near=?,roi=?,conv=?,events_total=?,trades_count=?,pnl_a=?,events_a=?,wins_a=?,losses_a=?,win_rate_a=?,btags=?,b_ratio=?,max_drawdown=?,profit_factor=?,sharpe=?,recovery_factor=?,max_win_streak=?,max_lose_streak=?,curve_score=? WHERE wallet=?',batch)
            conn.commit();batch=[]
    
    tp=0;ts_=0;tr=0;tst=0;tn=0;pa=0;ea=0;wa_=0;la_=0;te=0;tt=0;eps=[]
    for eid,trades in events.items():
        te+=1;tt+=len(trades);sp=0;rv=0;nr=0;pos=defaultdict(float)
        asp=0;apos=defaultdict(float);arv=0;has_a=False
        for t in trades:
            amt=t['price']*t['size'];is_a=t['id'] not in trade_btags
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
    tret=tr+tst+tn;sn=tst+tn;conv=min(sn/tret,1)if tret>0 and sn>=0 else 0
    wra=wa_/ea*100 if ea>0 else 0
    btstr=','.join(sorted(wallet_tags.get(wallet,set())))
    bamt=sum(t['amount'] for ei in events for t in events[ei] if t['id'] in trade_btags)
    tamt=sum(t['amount'] for ei in events for t in events[ei])
    br=bamt/tamt*100 if tamt>0 else 0
    
    pnls=eps
    if len(pnls)>=2:
        cum=[0]
        for p in pnls:cum.append(cum[-1]+p)
        pk=cum[0];mdd=0
        for v in cum:
            if v>pk:pk=v
            dd=pk-v;mdd=max(mdd,dd)
        ws2=sum(p for p in pnls if p>0);ls2=abs(sum(p for p in pnls if p<0))
        pf=min(ws2/ls2 if ls2>0.01 else(99 if ws2>0 else 0),99)
        mp=sum(pnls)/len(pnls);vp=sum((p-mp)**2 for p in pnls)/len(pnls)
        sp2=math.sqrt(vp)if vp>0 else 0.001;sh=mp/sp2
        rf=min(tp/mdd if mdd>0.01 else(99 if tp>0 else 0),99)
        mw=0;ml=0;cw=0;cl=0
        for p in pnls:
            if p>0.01:cw+=1;cl=0;mw=max(mw,cw)
            elif p<-0.01:cl+=1;cw=0;ml=max(ml,cl)
            else:cw=0;cl=0
        s1=min(max(sh,-2),3)/3*100;s2=min(pf,5)/5*100;s3=min(max(rf,0),10)/10*100
        s4=max(0,100-mdd/max(abs(tp),1)*100)if tp>0 else 0;s5=mw/max(mw+ml,1)*100
        cs=max(0,min(100,round(s1*0.3+s2*0.25+s3*0.2+s4*0.15+s5*0.1,1)))
    else:
        mdd=0;pf=0;sh=0;rf=0;mw=0;ml=0;cs=0
    
    batch.append((round(tp,2),round(ts_,2),round(tr,2),round(tst,2),round(tn,2),
        round(roi,2),round(conv,4),te,tt,round(pa,2),ea,wa_,la_,round(wra,1),
        btstr,round(br,1),round(mdd,2),round(pf,2),round(sh,3),round(rf,2),mw,ml,cs,wallet))

if batch:
    c.executemany('UPDATE wallets SET total_pnl=?,total_spent=?,total_recv=?,total_settle=?,total_near=?,roi=?,conv=?,events_total=?,trades_count=?,pnl_a=?,events_a=?,wins_a=?,losses_a=?,win_rate_a=?,btags=?,b_ratio=?,max_drawdown=?,profit_factor=?,sharpe=?,recovery_factor=?,max_win_streak=?,max_lose_streak=?,curve_score=? WHERE wallet=?',batch)
    conn.commit()

# New wallets
existing=set(r[0] for r in c.execute('SELECT wallet FROM wallets').fetchall())
new_w=set(wallet_events.keys())-existing
if new_w:
    print(f"  Adding {len(new_w)} new wallets...")
    for w in new_w:
        nm=''
        for ei,tds in wallet_events[w].items():
            for t in tds:
                n=c.execute('SELECT name FROM trades WHERE id=?',(t['id'],)).fetchone()
                if n and n[0]:nm=n[0];break
            if nm:break
        c.execute('INSERT INTO wallets (wallet,name) VALUES (?,?)',(w,nm))
    conn.commit()

print(f"\n=== Final ===")
for tbl in ['events','markets','trades','wallets']:
    print(f"  {tbl}: {c.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]}")
q_prof = "SELECT COUNT(*) FROM wallets WHERE total_pnl>0.01"
q_prof_a = "SELECT COUNT(*) FROM wallets WHERE pnl_a>0.01"
print(f"  Profitable(total): {c.execute(q_prof).fetchone()[0]}")
print(f"  Profitable(A): {c.execute(q_prof_a).fetchone()[0]}")
for b in ['B1','B2','B3','B4','B5','B7']:
    q = f"SELECT COUNT(*) FROM wallets WHERE btags LIKE '%{b}%'"
    print(f"  {b}: {c.execute(q).fetchone()[0]} wallets")
q_pure = "SELECT COUNT(*) FROM wallets WHERE btags=''"
print(f"  Pure A: {c.execute(q_pure).fetchone()[0]} wallets")

conn.close()
print("\nDone!")
