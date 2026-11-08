from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib import font_manager
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parent
font_manager.fontManager.addfont('/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc')
plt.rcParams.update({'font.family':font.get_name(),'axes.unicode_minus':False,'font.size':10,'figure.dpi':160})
rounds=pd.read_csv(ROOT/'selected_rounds.csv');ci=pd.read_csv(ROOT/'confidence_intervals.csv')
choices=[('follow:8','time:20:01','现有：20点跟马尔可夫','#64748b'),('fixed:16:B','draw:大双>=2','原条件：全天大双连出2次 → 单＋大双','#bc5863'),('fixed:8:B','draw:单>=5','观察：单连出5次 → 单＋大双','#177d80'),('follow:9','draw:双>=4','观察：双连出4次 → 遗漏回补','#8270aa')]
fig,(ax,bx)=plt.subplots(1,2,figsize=(13.2,4.7),gridspec_kw={'width_ratios':[1.45,1]})
for sid,cid,label,color in choices:
    data=rounds[(rounds.stream==sid)&(rounds.condition==cid)&(rounds.split=='test')].sort_values('start_day')
    ax.plot(np.arange(len(data)+1),np.r_[0,data.net.cumsum()],label=label,color=color,lw=2)
ax.axhline(0,color='#94a3b8',lw=.8);ax.set_xlabel('留出段按日回放（10月21日—11月5日）');ax.set_ylabel('累计净收益（每轮起始100）');ax.set_title('每天最多一轮：回测结果');ax.grid(axis='y',alpha=.15);ax.legend(loc='upper left',fontsize=8,frameon=False)
for j,(sid,cid,label,color) in enumerate(reversed(choices)):
    row=ci[(ci.stream==sid)&(ci.condition==cid)].iloc[0]
    data=rounds[(rounds.stream==sid)&(rounds.condition==cid)&(rounds.split=='test')]
    mean=data.net.mean();lo=row.test_mean_ci_low;hi=row.test_mean_ci_high
    bx.errorbar(mean,j,xerr=[[mean-lo],[hi-mean]],fmt='o',color=color,capsize=4)
    bx.text(hi+5,j,f'{mean:+.1f}',va='center',fontsize=9,color=color)
bx.set_yticks(range(len(choices)),[c[2].split('：')[0]+' '+c[2].split('：')[1].split(' → ')[0] for c in reversed(choices)],fontsize=9)
bx.axvline(0,color='#94a3b8',lw=1,ls='--');bx.set_xlabel('每轮平均净收益及95%区间');bx.set_title('各16轮，盈利优势仍不确定');bx.set_ylim(-.6,3.6);bx.grid(axis='x',alpha=.15)
for a in (ax,bx):
    a.spines[['top','right']].set_visible(False)
fig.suptitle('过三关触发条件分析 · 满赔2.8 / 70:30 / 整数滚仓',fontsize=14,y=1.02)
fig.text(.02,-.015,'只读历史回放；区间为按触发日重采样的百分位区间。候选来自多条件筛选，尚未经过未来实盘验证。',fontsize=9,color='#64748b')
fig.tight_layout();fig.savefig(ROOT/'holdout_comparison.png',bbox_inches='tight');fig.savefig(ROOT/'holdout_comparison.pdf',bbox_inches='tight')
