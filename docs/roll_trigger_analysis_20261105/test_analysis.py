import unittest
from datetime import datetime,timezone
import numpy as np
from analyze import source_features,conditions,cycles_for,selection,stamp,verify_cycle_engine,outcome_table


class ResearchChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.table,_=outcome_table()

    def draws(self,n=30):
        return [dict(issue=100+i,day='2026-10-21' if i<15 else '2026-10-22',minute=9*60,
                     deadline=1000+i*210,open_ts=1040+i*210,available_ts=1060+i*210,
                     code=158,triplet=[1,5,8],actual_big_small='大',actual_odd_even='双',actual_combo='大双') for i in range(n)]

    def test_features_wait_for_recorded_settlement(self):
        draws=self.draws();preds={i:dict(prediction_big_small='大',prediction_odd_even='双',prediction_combo='大双',settled_ts=d['open_ts']+10) for i,d in enumerate(draws)}
        preds[19]['settled_ts']=draws[20]['deadline']+1
        feature=source_features(draws,preds)
        self.assertTrue(np.isnan(feature['odd_even'][20][19]))
        self.assertTrue(np.isnan(feature['odd_even'][20][20]))
        self.assertEqual(feature['odd_even'][20][21],100)

    def test_draw_streak_does_not_use_current_or_unpublished_draw(self):
        draws=self.draws(7)
        draws[2]['available_ts']=draws[3]['deadline']+1
        masks={key:mask for key,_,mask in conditions(draws)}
        self.assertFalse(masks['draw:双>=3'][2])
        self.assertFalse(masks['draw:双>=3'][3])
        self.assertTrue(masks['draw:双>=3'][4])

    def test_gap_breaks_draw_streak(self):
        draws=self.draws(7)
        for d in draws[3:]:d['issue']+=1
        mask={key:mask for key,_,mask in conditions(draws)}['draw:双>=3']
        self.assertTrue(mask[3])
        self.assertFalse(mask[4])

    def test_refund_reduces_round_stake_and_resets_streak(self):
        draws=self.draws(5)
        for i,balls in enumerate(([5,8,2],[2,7,7],[7,1,4],[8,3,0],[7,1,4])):
            draws[i]['triplet']=list(balls);draws[i]['code']=100*balls[0]+10*balls[1]+balls[2]
        st=np.full(len(draws),1)
        cache=cycles_for(draws,st,self.table)
        self.assertEqual(cache['success'][0],0)
        self.assertEqual(cache['end'][0],2)
        self.assertEqual(cache['net'][0],-9920) # 100 ->198.80, integer198 ->60 refund ->0, remainder0.80
        verify_cycle_engine(draws,st,cache)

    def test_daily_limit_and_no_overlapping_rounds(self):
        draws=self.draws();st=np.zeros(len(draws),dtype=int)
        cache={'end':np.minimum(np.arange(len(draws))+2,len(draws))}
        mask=np.ones(len(draws),dtype=bool)
        chosen,_=selection(draws,st,cache,mask,'2026-10-21','2026-10-23')
        self.assertEqual(chosen,[0,15])
        chosen,_=selection(draws,st,cache,mask,'2026-10-21','2026-10-23',daily=False)
        self.assertTrue(all(b>a+2 for a,b in zip(chosen,chosen[1:])))

    def test_round_crossing_split_is_censored(self):
        draws=self.draws();st=np.zeros(len(draws),dtype=int);mask=np.zeros(len(draws),dtype=bool);mask[14]=True
        chosen,count=selection(draws,st,{'end':np.arange(len(draws))+2},mask,'2026-10-21','2026-10-22')
        self.assertEqual(chosen,[]);self.assertEqual(count,1)

if __name__=='__main__':unittest.main()
