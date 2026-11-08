"""Post-selection sensitivity and uncertainty; never reselect using the test slice."""
import argparse,csv,gzip,json,math
from pathlib import Path
import numpy as np
from analyze import ROOT,RULE,SPLITS,STRUCTURES,COMBO_STRUCT,load,outcome_table,cycles_for,conditions,source_features,selection,summarize,save_csv,save_json,verify_cycle_engine


def simulate(table,base,target,n=300000,seed=20261105):
    results=[]
    for k,key in enumerate(STRUCTURES):
        rng=np.random.default_rng(seed+k+base+target*100)
        capital=np.full(n,base*100,dtype=np.int64);net=np.zeros(n,dtype=np.int64);streak=np.zeros(n,dtype=np.int8)
        periods=np.zeros(n,dtype=np.int32);active=np.ones(n,dtype=bool);successful=np.zeros(n,dtype=bool)
        count=0
        while active.any() and count<500:
            ids=np.flatnonzero(active);code=rng.integers(0,1000,len(ids));total=capital[ids]//100;main=total*7//10;aux=total-main
            payout=main*table[k,code,0]+aux*table[k,code,1];delta=payout-total*100
            net[ids]+=delta;capital[ids]=payout;periods[ids]+=1
            streak[ids]=np.where(delta>0,streak[ids]+1,np.where(delta<0,0,streak[ids]))
            win=streak[ids]>=target;successful[ids[win]]=True;active[ids[win|(payout<200)]]=False;count+=1
        vals=net/100
        results.append({'structure':key,'base':base,'target':target,'simulations':n,'unresolved':int(active.sum()),
            'success_rate':float(successful.mean()),'profitable_rate':float(np.mean(net>0)),
            'mean_net':float(vals.mean()),'mean_net_standard_error':float(vals.std(ddof=1)/np.sqrt(n)),
            'median_net':float(np.median(vals)),'mean_periods':float(periods.mean()),
            'p_three_failed_rounds':float((1-successful.mean())**3),'successful_but_losing':int(np.sum(successful&(net<0)))})
    return results


def main(path):
    p,draws,clean,meta=load(path);table,_=outcome_table();n=len(draws)
    summary=json.loads((ROOT/'summary.json').read_text());selected=summary['selected']
    theoretical=[]
    for base,target in ((100,3),(10,3),(100,2),(100,1)):
        theoretical.extend(simulate(table,base,target))
    save_csv('iid_round_simulation.csv',theoretical)
    rows=[];timing=[];one_period=[];all_rounds=[]
    for r in selected+summary['baselines']:
        sid=r['stream'];cid=r['condition'];pid=int(sid.split(':')[1]);st=np.full(n,-1,dtype=np.int8)
        for i,pred in clean[pid].items():
            if pred['prediction_combo'] in COMBO_STRUCT:
                st[i]=COMBO_STRUCT[pred['prediction_combo']] if sid.startswith('follow') else 'ABCD'.index(sid.split(':')[2])
        cache=cycles_for(draws,st,table);features=source_features(draws,clean[pid]);conds={c:(lab,mask) for c,lab,mask in conditions(draws,features)}
        label,mask=conds[cid]
        testmask=np.array(['2026-10-21'<=d['day']<'2026-11-06' for d in draws])&(st>=0)
        total_ones=table[st[testmask],np.array([d['code'] for d in draws])[testmask]]
        ratios=total_ones[:,0]*.7+total_ones[:,1]*.3
        chosen=mask&testmask;cv=table[st[chosen],np.array([d['code'] for d in draws])[chosen]]
        pr=cv[:,0]*.7+cv[:,1]*.3
        one_period.append({'stream':sid,'condition':cid,'all_test_periods':int(testmask.sum()),'trigger_test_periods':int(chosen.sum()),
            'baseline_first_period_profit_rate':float(np.mean(ratios>100)),'trigger_first_period_profit_rate':float(np.mean(pr>100)),
            'baseline_first_period_ev100':float(np.mean(ratios)-100),'trigger_first_period_ev100':float(np.mean(pr)-100)})
        for delay in (0,1,2):
            shifted=np.concatenate([np.zeros(delay,dtype=bool),mask[:n-delay]]) if delay else mask
            for split,a,b in SPLITS:
                idx,bnd=selection(draws,st,cache,shifted,a,b)
                timing.append({'stream':sid,'condition':cid,'trigger_delay_issues':delay,'split':split,**summarize(draws,cache,idx,bnd)})
        for split,a,b in SPLITS:
            idx,bnd=selection(draws,st,cache,mask,a,b,daily=False,cooldown=10)
            for i in idx:all_rounds.append({'stream':sid,'condition':cid,'split':split,'day':draws[i]['day'],'net':float(cache['net'][i])/100,'success':int(cache['success'][i])})
        # These neighboring thresholds were part of the original pre-defined search, not new optimized values.
        relatives=[]
        if cid.startswith('draw:'):
            val,t=cid[5:].split('>=');t=int(t)
            relatives=[f'draw:{val}>={nt}' for nt in (t-1,t,t+1) if f'draw:{val}>={nt}' in conds]
        elif cid.startswith('rate:'):
            relatives=[f'rate:odd_even:20<={nt}' for nt in (35,40,45)]
        for c in relatives:
            m=conds[c][1]
            for split,a,b in SPLITS:
                ix,b=selection(draws,st,cache,m,a,b)
                rows.append({'selected_stream':sid,'selected_condition':cid,'neighbor_condition':c,'split':split,**summarize(draws,cache,ix,b)})
    save_csv('threshold_neighbors.csv',rows);save_csv('trigger_delay_sensitivity.csv',timing);save_csv('first_period_edge.csv',one_period)
    # Compare selected results to original time rule by pairing the 16 held-out calendar days.
    rounds=list(csv.DictReader((ROOT/'selected_rounds.csv').open(encoding='utf-8-sig')))
    baseline={r['start_day']:float(r['net']) for r in rounds if r['stream']=='follow:8' and r['condition']=='time:20:01' and r['split']=='test'}
    comparisons=[]
    for chosen in selected:
        values={r['start_day']:float(r['net']) for r in rounds if r['stream']==chosen['stream'] and r['condition']==chosen['condition'] and r['split']=='test'}
        days=sorted(set(baseline)|set(values));d=np.array([values.get(day,0)-baseline.get(day,0) for day in days]);rng=np.random.default_rng(20261105)
        means=np.mean(rng.choice(d,size=(20000,len(d))),axis=1)
        lo,hi=np.quantile(means,[.025,.975])
        daily={}
        ar=[r for r in all_rounds if r['stream']==chosen['stream'] and r['condition']==chosen['condition'] and r['split']=='test']
        for r in ar:daily.setdefault(r['day'],[]).append(r['net'])
        ds=np.array([sum(v) for v in daily.values()]);ns=np.array([len(v) for v in daily.values()]);idx=rng.integers(0,len(ds),(20000,len(ds)))
        multi=np.sum(ds[idx],axis=1)/np.sum(ns[idx],axis=1);ml,mh=np.quantile(multi,[.025,.975])
        comparisons.append({'stream':chosen['stream'],'condition':chosen['condition'],'paired_days':len(days),'daily_gain_vs_20pm':float(d.mean()),
            'paired_gain_ci_low':float(lo),'paired_gain_ci_high':float(hi),'frequency_test_rounds':len(ar),
            'frequency_mean_ci_low':float(ml),'frequency_mean_ci_high':float(mh)})
    save_csv('paired_baseline_comparison.csv',comparisons)
    pvalues=[]
    for chosen in selected:
        values={r['start_day']:float(r['net']) for r in rounds if r['stream']==chosen['stream'] and r['condition']==chosen['condition'] and r['split']=='test'}
        days=sorted(set(baseline)|set(values));delta=np.array([values.get(day,0)-baseline.get(day,0) for day in days])
        signs=2*((np.arange(1<<len(days))[:,None]>>np.arange(len(days)))&1)-1
        pvalue=float(np.mean(signs@delta>=delta.sum()-1e-8))
        pvalues.append({'stream':chosen['stream'],'condition':chosen['condition'],'paired_signflip_p_one_sided':pvalue})
    order=sorted(range(len(pvalues)),key=lambda i:pvalues[i]['paired_signflip_p_one_sided']);adjusted=0
    for rank,i in enumerate(order):
        adjusted=max(adjusted,min(1,pvalues[i]['paired_signflip_p_one_sided']*(len(pvalues)-rank)))
        pvalues[i]['holm_p_five_holdout_comparisons']=adjusted
    save_csv('paired_randomization.csv',pvalues)
    save_json('robustness_summary.json',{'iid':theoretical,'comparisons':comparisons,'one_period_edges':one_period})
    print(json.dumps({'iid_base100_target3':[r for r in theoretical if r['base']==100 and r['target']==3],'comparisons':comparisons},ensure_ascii=False))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('snapshot');args=parser.parse_args();main(args.snapshot)
