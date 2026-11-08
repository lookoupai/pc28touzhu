"""Reproducible read-only research. See methodology.md for the fixed selection rule.
Run with AITradingSimulator/.venv/bin/python and PYTHONPATH=src.
"""
from __future__ import annotations
import argparse, bisect, collections, csv, gzip, hashlib, json, math
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
from pc28touzhu.domain.win_streak_roll import _settle_leg, create_cycle_state, normalize_win_streak_roll_policy, settle_period
from pc28touzhu.domain.split_roll_strategy import STRUCTURES

ROOT=Path(__file__).resolve().parent
TZ=timezone(timedelta(hours=8))
RULE='pc28_fullpay_2_8_regular'
COMBOS=('小单','大单','小双','大双')
COMBO_STRUCT={v:i for i,v in enumerate(COMBOS)}
METRICS=('big_small','odd_even','combo')
SPLITS=(('train','2026-09-06','2026-10-01'),('validation','2026-10-01','2026-10-21'),('test','2026-10-21','2026-11-06'))


def stamp(value, tz=timezone.utc):
    if not value:return None
    d=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    return d.replace(tzinfo=tz).timestamp() if d.tzinfo is None else d.timestamp()


def save_json(name, value):
    (ROOT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=lambda x:x.item() if isinstance(x,np.generic) else str(x)))


def save_csv(name, rows):
    if not rows:return
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with (ROOT/name).open('w',newline='',encoding='utf-8-sig') as f:
        writer=csv.DictWriter(f,fields);writer.writeheader();writer.writerows(rows)


def load(path):
    with gzip.open(path,'rt') as f:p=json.load(f)
    draws=[];invalid=[]
    for r in p['draws']:
        raw=json.loads(r['source_payload'])
        if 'number' in raw:
            triplet=[int(x) for x in str(raw['number']).split('+')]
        else:
            triplet=[int(raw['canada28_num'+str(i)]) for i in (1,2,3)]
        if len(triplet)!=3 or not all(0<=x<=9 for x in triplet) or sum(triplet)!=r['result_number']:
            invalid.append(r['issue_no']);continue
        d={**r,'issue':int(r['issue_no']),'triplet':triplet,'code':triplet[0]*100+triplet[1]*10+triplet[2],
           'open_ts':stamp(r['open_time'],TZ),'available_ts':stamp(r['created_at'])}
        d['deadline']=d['open_ts']-40
        local=datetime.fromtimestamp(d['deadline'],TZ)
        d['day']=local.date().isoformat();d['minute']=local.hour*60+local.minute
        d['actual_big_small']='大' if r['result_number']>=14 else '小'
        d['actual_odd_even']='单' if r['result_number']%2 else '双'
        d['actual_combo']=d['actual_big_small']+d['actual_odd_even']
        draws.append(d)
    issues={d['issue']:i for i,d in enumerate(draws)}
    clean=collections.defaultdict(dict);audit=collections.defaultdict(collections.Counter)
    for r in p['predictions']:
        pid=r['predictor_id'];a=audit[pid];a['rows']+=1
        if r['status']!='settled':a['not_settled']+=1;continue
        i=issues.get(int(r['issue_no']))
        if i is None:a['no_draw']+=1;continue
        d=draws[i];created=stamp(r['created_at']);settled=stamp(r['settled_at'])
        if created is None or settled is None or settled < d['open_ts']:
            a['bad_timestamps']+=1;continue
        if created>d['deadline']:a['too_late_for_40s_cutoff']+=1;continue
        if i in clean[pid]:a['duplicates']+=1;continue
        a['eligible']+=1
        clean[pid][i]={**r,'index':i,'created_ts':created,'settled_ts':settled}
        for m in METRICS:
            value=r['prediction_'+m]
            if value:
                a[m+'_count']+=1
                if r['hit_'+m] != int(value==d['actual_'+m]):a[m+'_hit_field_mismatch']+=1
    metadata={'snapshot_sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest(),'extracted_at_utc':p['extracted_at_utc'],
        'draw_count':len(draws),'first_draw':draws[0]['open_time'],'last_draw':draws[-1]['open_time'],
        'first_issue':draws[0]['issue'],'last_issue':draws[-1]['issue'],'invalid_draws':invalid,
        'issue_gaps':sum(draws[i]['issue']!=draws[i-1]['issue']+1 for i in range(1,len(draws))),
        'prediction_audit':dict(audit),'split_definition':SPLITS,'decision_time':'open_time minus 40 seconds',
        'caveat':'Recorded created_at/settled_at provide chronology checks, not immutable historical version proof.'}
    return p,draws,clean,metadata


def outcome_table(rule=RULE):
    table=np.zeros((4,1000,2),dtype=np.int32)
    rows=[]
    for k,(key,st) in enumerate(STRUCTURES.items()):
        for code in range(1000):
            balls=[code//100,code//10%10,code%10]
            for leg,(metric,value) in enumerate((('odd_even',st['main_value']),('combo',st['aux_value']))):
                result=_settle_leg(stake=100,bet_type=metric,bet_value=value,draw_context={'triplet':balls},settlement_rule_id=rule)
                table[k,code,leg]=round(result['payout'])
        ratios=table[k,:,0]*.7+table[k,:,1]*.3
        rows.append({'rule':rule,'structure':key,'label':st['label'],'profit_probability':float(np.mean(ratios>100)),
            'refund_partial_loss_probability':float(np.mean((ratios>0)&(ratios<100))),
            'full_loss_probability':float(np.mean(ratios==0)),'flat_probability':float(np.mean(ratios==100)),
            'ev_per_100':float(ratios.mean()-100)})
    return table,rows


def actual_audit(p,draws,table):
    issue_index={d['issue']:i for i,d in enumerate(draws)}
    jobs={j['progression_event_id']:j for j in p['roll_jobs']}
    attempts=collections.defaultdict(list)
    for a in p['roll_attempts']:attempts[a['job_id']].append(a)
    grouped=collections.defaultdict(list);void=collections.Counter();void_net=0;void_stake=0;void_sent=0
    for e in p['roll_events']:
        if e['status']=='void':
            ctx=json.loads(e['result_context_json']);void[ctx.get('void_reason','')]+=1
            void_net+=e['net_delta'];void_stake+=e['stake_amount']
            void_sent+=any(a['delivery_status']=='delivered' for a in attempts[jobs[e['id']]['id']])
            continue
        if e['status']=='settled':grouped[(e['auto_trigger_rule_run_id'],int(e['issue_no']))].append(e)
    periods=[];runs=collections.defaultdict(list);mismatches=[]
    for (run,issue),legs in sorted(grouped.items()):
        assert len(legs)==2 and {e['leg_key'] for e in legs}=={'main','aux'}
        d=draws[issue_index[issue]];amount=sum(e['stake_amount'] for e in legs);net=sum(e['net_delta'] for e in legs)
        bets=[];results=[];refunds=[]
        for e in sorted(legs,key=lambda e:e['leg_key']!='main'):
            snap=json.loads(e['settlement_snapshot_json']);ctx=json.loads(e['result_context_json']);s=snap['signal']
            rec=_settle_leg(stake=e['stake_amount'],bet_type=s['bet_type'],bet_value=s['bet_value'],draw_context={'triplet':d['triplet']},settlement_rule_id=e['settlement_rule_id'])
            if abs(rec['payout']-e['stake_amount']-e['net_delta'])>.001:mismatches.append(e['id'])
            sent=[a for a in attempts[jobs[e['id']]['id']] if a['delivery_status']=='delivered']
            assert sent and min(stamp(a['executed_at']) for a in sent)<d['open_ts']
            bets.append(s['bet_value']+str(int(e['stake_amount'])));results.append(e['resolved_result_type'])
            if ctx.get('refund_reason'):refunds.append(ctx['refund_reason'])
        row={'run_id':run,'rule_id':legs[0]['auto_trigger_rule_id'],'issue':issue,'beijing_open_time':d['open_time'],
             'bets':' '.join(bets),'triplet':'+'.join(map(str,d['triplet'])),'results':','.join(results),'stake':amount,
             'net':round(net,2),'payout':round(amount+net,2),'refund_reasons':';'.join(refunds)}
        periods.append(row);runs[run].append(row)
    summary=[]
    for run,rs in runs.items():
        streak=0;best=0
        for r in rs:
            streak=streak+1 if r['net']>0 else 0 if r['net']<0 else streak;best=max(best,streak)
        record=next(r for r in p['roll_runs'] if r['id']==run)
        summary.append({'run_id':run,'rule_id':rs[0]['rule_id'],'start_time':rs[0]['beijing_open_time'],'initial':rs[0]['stake'],
            'periods':len(rs),'profitable_periods':sum(r['net']>0 for r in rs),'max_streak':best,'target_reached':best>=3,
            'net':round(sum(r['net'] for r in rs),2),'ending_balance':round(rs[0]['stake']+sum(r['net'] for r in rs),2),
            'stop_reason':record['stop_reason']})
    save_csv('actual_periods.csv',periods);save_csv('actual_rounds.csv',summary)
    result={'normal_rounds':summary,'normal_periods':len(periods),'normal_net':round(sum(x['net'] for x in summary),2),
        'normal_settlement_mismatch_ids':mismatches,'void_event_reasons':dict(void),'void_delivered_legs':void_sent,
        'void_recorded_stake':void_stake,'void_recorded_net_unverified':round(void_net,2),
        'void_note':'Historical cycle bug. Real delivery exists; excluded only from correct three-stage strategy statistics. Stored net not independently reconciled to group account.'}
    save_json('actual_audit.json',result)
    assert not mismatches
    return result


def cycles_for(draws,structure,table,base=100,target=3):
    n=len(draws);valid=np.flatnonzero(structure>=0);nextvalid=np.full(n+1,n,dtype=int)
    pos=n
    for i in range(n-1,-1,-1):
        if structure[i]>=0:pos=i
        nextvalid[i]=pos
    result={k:np.zeros(n,dtype=np.int64) for k in ('end','net','success','profit_periods','periods','peak','trough','drawdown')}
    result['end'][:]=n
    for start in valid:
        capital=base*100;net=0;streak=0;peak=0;trough=0;dd=0;pp=0;count=0;i=int(start)
        while i<n:
            total=capital//100
            if total<2:break
            main=total*7//10;aux=total-main
            m,a=table[structure[i],draws[i]['code']]
            payout=main*int(m)+aux*int(a)
            delta=payout-total*100;net+=delta;capital=payout;count+=1
            peak=max(peak,net);trough=min(trough,net);dd=max(dd,peak-net)
            if delta>0:streak+=1;pp+=1
            elif delta<0:streak=0
            if streak>=target or capital<200:
                result['end'][start]=i;result['success'][start]=int(streak>=target);break
            available=draws[i]['available_ts']
            j=int(nextvalid[i+1])
            while j<n and draws[j]['deadline']<=available:j=int(nextvalid[j+1])
            if j<n and draws[j]['issue']!=draws[i]['issue']+(j-i):break
            i=j
        for key,val in (('net',net),('periods',count),('profit_periods',pp),('peak',peak),('trough',trough),('drawdown',dd)):
            result[key][start]=val
    return result


def verify_cycle_engine(draws,structure,cache,base=100,target=3):
    starts=np.flatnonzero((structure>=0)&(cache['end']<len(draws)))
    for start in starts[::max(1,len(starts)//60)]:
        policy=normalize_win_streak_roll_policy({'base_amount':base,'structure':'follow_signal','target_streak':target,'roll_mode':'chain'})
        state=create_cycle_state(policy,stat_date=draws[start]['day']);n=0
        prev_available=0
        for j in range(int(start),int(cache['end'][start])+1):
            if structure[j]<0 or draws[j]['deadline']<=prev_available:continue
            st=int(structure[j]);signal={'bet_type':'combo','bet_value':COMBOS[st]}
            r=settle_period(policy,state,{'triplet':draws[j]['triplet']},signal=signal,settlement_rule_id=RULE)
            state=r['new_state'] if 'new_state' in r else r['state'];n+=1;prev_available=draws[j]['available_ts']
        assert abs((state['balance']-base)*100-cache['net'][start])<.01,(start,state,cache['net'][start])
        assert n==cache['periods'][start]
        assert (state['stop_reason']=='win_streak_target')==bool(cache['success'][start])


def source_features(draws,predictions):
    n=len(draws);features={}
    for metric in METRICS:
        hits=np.full(n,np.nan);streak=np.zeros(n,dtype=int);rates={w:np.full(n,np.nan) for w in (20,50,100)}
        events=sorted((r['settled_ts'],i,int(r['prediction_'+metric]==draws[i]['actual_'+metric]))
            for i,r in predictions.items() if r['prediction_'+metric])
        indices=[];outcomes=[];cursor=0
        for i,d in enumerate(draws):
            while cursor<len(events) and events[cursor][0]<=d['deadline']:
                _,j,hit=events[cursor];cursor+=1
                if j>=i:continue
                at=bisect.bisect_left(indices,j);indices.insert(at,j);outcomes.insert(at,hit)
            if not indices or indices[-1]!=i-1:continue
            hist=outcomes[-100:]
            for w in rates:
                if len(hist)>=w:rates[w][i]=sum(hist[-w:])/w*100
            for h in reversed(hist):
                if h:break
                streak[i]+=1
        features[metric]={'miss':streak,**rates}
    return features


def conditions(draws,feature=None):
    n=len(draws);yield 'always','无条件',np.ones(n,dtype=bool)
    for m,values in (('big_small',('大','小')),('odd_even',('单','双')),('combo',COMBOS)):
        for value in values:
            counts=np.zeros(n,dtype=int);streak=0
            for i in range(1,n):
                prev=draws[i-1]
                streak=streak+1 if prev['actual_'+m]==value and (i<2 or prev['issue']==draws[i-2]['issue']+1) else 1 if prev['actual_'+m]==value else 0
                counts[i]=streak if prev['available_ts']<=draws[i]['deadline'] else 0
            for threshold in (1,2,3,4,5):yield f'draw:{value}>={threshold}',f'连续开{value}≥{threshold}',counts>=threshold
    mins=np.array([d['minute'] for d in draws])
    for h in (8,14,20):yield f'time:{h:02}:01',f'每天{h:02}:01–{h:02}:30',(mins>=h*60+1)&(mins<=h*60+30)
    if feature:
        for m,ths in (('odd_even',(3,4,5,6,7,8)),('combo',(5,7,9,11,13))):
            for t in ths:yield f'miss:{m}>={t}',f'预测{m}连错≥{t}',feature[m]['miss']>=t
        for m,thresholds in (('odd_even',(35,40,45,55,60,65)),('combo',(15,20,25,30,35,40))):
            for w in (20,50,100):
                for idx,t in enumerate(thresholds):
                    op='<=' if idx<3 else '>='
                    arr=feature[m][w]
                    mask=arr<=t if idx<3 else arr>=t
                    yield f'rate:{m}:{w}{op}{t}',f'预测{m}近{w}期命中率{op}{t}%',mask


def selection(draws,structure,cache,mask,start_day,end_day,daily=True,cooldown=0):
    indices=np.flatnonzero(mask&(structure>=0))
    chosen=[];blocked=-1;used=set();boundary=0;last_trigger=-100000
    for i in indices:
        day=draws[i]['day']
        if day<start_day or day>=end_day or i<=blocked or (daily and day in used) or i-last_trigger<cooldown:continue
        end=int(cache['end'][i])
        if end>=len(draws) or draws[end]['day']>=end_day:
            boundary+=1;break
        chosen.append(int(i));used.add(day);blocked=end;last_trigger=i
    return chosen,boundary


def summarize(draws,cache,indices,boundary=0):
    if not indices:return dict(n=0,success_rate=None,profitable_rate=None,net=0.,mean=None,mdd=0.,longest_loss=0,periods=0,censored=boundary)
    eq=0;peak=0;dd=0;loss=0;longest=0
    for i in indices:
        dd=max(dd,peak-(eq+cache['trough'][i]),cache['drawdown'][i]);peak=max(peak,eq+cache['peak'][i]);eq+=cache['net'][i]
        loss=loss+1 if cache['net'][i]<0 else 0;longest=max(longest,loss)
    return dict(n=len(indices),success_rate=float(np.mean(cache['success'][indices])),profitable_rate=float(np.mean(cache['net'][indices]>0)),
        net=round(eq/100,2),mean=round(eq/100/len(indices),4),mdd=round(dd/100,2),longest_loss=longest,
        periods=int(sum(cache['periods'][indices])),censored=boundary)


def run(path):
    p,draws,clean,metadata=load(path);table,theory=outcome_table();actual=actual_audit(p,draws,table)
    n=len(draws);sources={r['id']:r['name'] for r in p['predictors']}
    np.random.seed(20261105)
    streams={};features={}
    for pid,preds in clean.items():
        st=np.full(n,-1,dtype=np.int8)
        for i,r in preds.items():st[i]=COMBO_STRUCT.get(r['prediction_combo'],-1)
        if np.count_nonzero(st>=0)<100:continue
        if pid in (7,8,9,15,16,17,21,22,23,24,2,4,5):
            streams[f'follow:{pid}']={'structure':st,'pid':pid,'label':sources[pid]+'·逐期跟随组合'}
    for pid in (8,16):
        for k,key in enumerate(STRUCTURES):
            st=np.full(n,-1,dtype=np.int8)
            for i,r in clean[pid].items():
                if r['prediction_combo'] in COMBO_STRUCT:st[i]=k
            streams[f'fixed:{pid}:{key}']={'structure':st,'pid':pid,'label':sources[pid]+'·固定'+STRUCTURES[key]['label']}
    for pid in {s['pid'] for s in streams.values()}:
        features[pid]=source_features(draws,clean[pid])
    metadata['valid_combo_streams']=list(streams)
    records=[];all_conditions={};caches={}
    for sid,s in streams.items():
        cache=cycles_for(draws,s['structure'],table);caches[sid]=cache
        verify_cycle_engine(draws,s['structure'],cache)
        conds=list(conditions(draws,features[s['pid']]));all_conditions[sid]={k:(label,mask) for k,label,mask in conds}
        for cid,label,mask in conds:
            r={'stream':sid,'source_id':s['pid'],'structure':sid.split(':')[-1] if sid.startswith('fixed') else 'follow_signal','strategy':s['label'],'condition':cid,'condition_label':label}
            for split,a,b in SPLITS:
                idx,bound=selection(draws,s['structure'],cache,mask,a,b)
                r.update({split+'_'+k:v for k,v in summarize(draws,cache,idx,bound).items()})
            records.append(r)
        print('evaluated',sid,len(conds),flush=True)
    save_csv('candidate_results.csv',records)
    eligible=[r for r in records if r['train_n']>=15 and r['validation_n']>=15 and r['train_mean']>0 and r['validation_mean']>0]
    eligible.sort(key=lambda r:(min(r['train_mean'],r['validation_mean']),r['train_mean'],r['stream'],r['condition']),reverse=True)
    top=eligible[:5]
    metadata['candidate_count']=len(records);metadata['eligible_positive_train_validation']=len(eligible)
    metadata['selection']='minimum train/validation mean, then train mean; each segment >=15 completed rounds, both positive'
    save_csv('selected_candidates.csv',top)
    basekeys=[('follow:8','time:20:01'),('follow:8','miss:odd_even>=5'),('fixed:16:B','draw:大双>=2'),('fixed:16:B','always'),('follow:8','always')]
    baselines=[r for key in basekeys for r in records if (r['stream'],r['condition'])==key]
    save_csv('baseline_results.csv',baselines)
    chosen=list({(r['stream'],r['condition']):r for r in top+baselines}.values())
    details=[];sens=[];intervals=[]
    for r in chosen:
        sid=r['stream'];cid=r['condition'];s=streams[sid];mask=all_conditions[sid][cid][1];cache=caches[sid]
        for split,a,b in SPLITS:
            idx,bound=selection(draws,s['structure'],cache,mask,a,b)
            for i in idx:details.append({'stream':sid,'condition':cid,'split':split,'start_issue':draws[i]['issue'],'start_day':draws[i]['day'],
                'start_time':draws[i]['open_time'],'end_issue':draws[int(cache['end'][i])]['issue'],'periods':int(cache['periods'][i]),
                'target_reached':int(cache['success'][i]),'net':int(cache['net'][i])/100})
            if split=='test' and idx:
                vals=cache['net'][idx]/100
                rng=np.random.default_rng(20261105)
                # One round per day: resampling complete trigger days preserves within-round dependence.
                means=np.mean(rng.choice(vals,size=(10000,len(vals))),axis=1)
                lo,hi=np.quantile(means,[.025,.975]);nidx=len(idx);prop=float(np.mean(cache['success'][idx]));z=1.96
                center=(prop+z*z/(2*nidx))/(1+z*z/nidx);half=z*math.sqrt(prop*(1-prop)/nidx+z*z/(4*nidx*nidx))/(1+z*z/nidx)
                intervals.append({'stream':sid,'condition':cid,'test_n':nidx,'test_mean_ci_low':float(lo),'test_mean_ci_high':float(hi),
                    'success_ci_low':center-half,'success_ci_high':center+half})
        for daily in (True,False):
            for base,target in ((100,3),(10,3),(100,2),(100,1)):
                c=cache if (base,target)==(100,3) else cycles_for(draws,s['structure'],table,base,target)
                for split,a,b in SPLITS:
                    idx,bound=selection(draws,s['structure'],c,mask,a,b,daily=daily,cooldown=10 if not daily else 0)
                    sens.append({'stream':sid,'condition':cid,'daily_limit':1 if daily else 'unlimited_cooldown10','base':base,'target':target,'split':split,**summarize(draws,c,idx,bound)})
    save_csv('selected_rounds.csv',details);save_csv('sensitivity.csv',sens);save_csv('confidence_intervals.csv',intervals)
    # Conditional scheduling currently saved on rule 11, evaluate separately without tuning.
    sid='fixed:16:B';s=streams[sid];mins=np.array([d['minute'] for d in draws]);mask=all_conditions[sid]['draw:大双>=2'][1]&(mins>=8*60+3)&(mins<=8*60+30)
    current=[]
    for split,a,b in SPLITS:
        idx,bound=selection(draws,s['structure'],caches[sid],mask,a,b)
        current.append({'split':split,**summarize(draws,caches[sid],idx,bound)})
    save_csv('current_rule11_condition_schedule.csv',current)
    save_csv('theoretical_periods.csv',theory);save_json('metadata.json',metadata)
    save_json('summary.json',{'actual':actual,'selected':top,'baselines':baselines,'intervals':intervals,'current_rule11':current,'theory':theory})
    print(json.dumps({'candidates':len(records),'eligible':len(eligible),'selected':[(r['stream'],r['condition'],r['train_mean'],r['validation_mean'],r['test_mean'],r['test_n']) for r in top]},ensure_ascii=False),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('snapshot');args=parser.parse_args();run(args.snapshot)
