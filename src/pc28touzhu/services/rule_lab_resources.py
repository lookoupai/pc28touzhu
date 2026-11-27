"""Bounded control of a single systemd unit, independent of simulation state."""
from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

from pc28touzhu.services.rule_lab_store import RuleLabError

UNIT='pc28touzhu-rule-lab.service'
HELPER=Path('/usr/local/libexec/pc28-rule-lab-resource-mode')
CGROUP=Path('/sys/fs/cgroup/system.slice')/UNIT
MODES={'economy':dict(name='省资源',cpu_cores=.2,cpu_quota_percent=20),
       'fast':dict(name='加速',cpu_cores=1.0,cpu_quota_percent=100)}


def _trusted_helper():
    try:
        for path in (HELPER,*HELPER.parents):
            info=path.lstat()
            if info.st_uid!=0 or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):return False
        return HELPER.is_file() and os.access(HELPER,os.X_OK)
    except OSError:return False


class RuleLabResources:
    def __init__(self,*,run=None):
        self.run=run or subprocess.run

    def status(self):
        result=dict(available=False,mode=None,cpu_cores=None,cpu_quota_percent=None,
                    memory_max_bytes=None,memory_current_bytes=None,worker_pid=None,
                    scope='global',persistent=True,modes=[dict(id=k,**v) for k,v in MODES.items()],message='')
        try:
            completed=self.run(['/usr/bin/systemctl','show',UNIT,'--no-pager',
                '--property=ActiveState,SubState,MainPID,CPUQuotaPerSecUSec,MemoryMax,MemoryCurrent,ControlGroup'],
                capture_output=True,text=True,check=True,timeout=3,env={'PATH':'/usr/bin:/bin','LC_ALL':'C'})
            values=dict(line.split('=',1) for line in completed.stdout.splitlines() if '=' in line)
            if values.get('ActiveState')!='active' or values.get('SubState')!='running':
                result['message']='模拟后台未运行，暂不能切换速度';return result
            if values.get('ControlGroup')!='/system.slice/'+UNIT:
                result['message']='无法核对模拟后台资源限制';return result
            quota,period=CGROUP.joinpath('cpu.max').read_text().strip().split()
            ratio=int(quota)/int(period) if quota!='max' else None
            percent=round(ratio*100,3) if ratio is not None else None
            manager_quota={'200ms':20,'1s':100}.get(values.get('CPUQuotaPerSecUSec'))
            mode=next((key for key,value in MODES.items() if value['cpu_quota_percent']==percent),None)
            result.update(mode=mode,cpu_cores=ratio,cpu_quota_percent=percent,
                memory_max_bytes=int(values['MemoryMax']),memory_current_bytes=int(values['MemoryCurrent']),
                worker_pid=int(values['MainPID']))
            if not _trusted_helper():result['message']='速度切换尚未配置，当前资源上限仍可查看'
            elif percent!=manager_quota or mode is None:result['message']='实际CPU限额与可选档位不一致，请检查后台配置'
            elif result['memory_max_bytes']!=268435456:result['message']='模拟内存上限与预设不一致，请检查后台配置'
            else:result['available']=True
        except (OSError,ValueError,KeyError,ZeroDivisionError,subprocess.SubprocessError):
            result['message']='暂时无法读取实际资源限制，请刷新重试'
        return result

    def set_mode(self,mode):
        if not isinstance(mode,str) or mode not in MODES:
            raise RuleLabError('invalid_input','运行速度只能选择省资源或加速')
        before=self.status()
        if not before['available']:
            raise RuleLabError('performance_unavailable',before['message'],503)
        if before['mode']==mode:return before
        try:
            self.run(['/usr/bin/sudo','-n',str(HELPER),mode],capture_output=True,text=True,
                     check=True,timeout=11,env={'PATH':'/usr/bin:/bin','LC_ALL':'C'})
        except subprocess.TimeoutExpired as exc:
            raise RuleLabError('performance_unknown','速度切换响应超时，请刷新核对当前生效档位',503) from exc
        except (OSError,subprocess.SubprocessError) as exc:
            raise RuleLabError('performance_failed','速度切换未能确认，请刷新核对当前生效档位',503) from exc
        current=self.status()
        if not current['available'] or current['mode']!=mode:
            raise RuleLabError('performance_unconfirmed','尚未确认所选档位生效，请刷新核对',503)
        return current
