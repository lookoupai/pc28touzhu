#!/usr/bin/env python3
"""Preview or explicitly convert the independent experiment DB to incremental vacuum.

Stop the platform API, rule-lab worker and batch-queue services before --apply.
This command does not stop/start services and never opens the upstream source DB.
"""
import argparse
import json
import subprocess
from pathlib import Path

from pc28touzhu.services.rule_lab_storage import prepare_storage


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--backup',type=Path)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    if args.apply:
        for unit in ('pc28touzhu-platform.service','pc28touzhu-rule-lab.service','pc28touzhu-rule-lab-batch-queue.service'):
            try:
                result=subprocess.run(['systemctl','is-active',unit],capture_output=True,text=True,timeout=10)
            except (OSError,subprocess.TimeoutExpired):
                parser.error('无法确认实验写者已停止，服务状态查询失败：'+unit)
            state=result.stdout.strip()
            if state in ('active','activating','reloading','deactivating'):
                parser.error('请先停止实验写者：'+unit)
            # is-active uses LSB exit status 3 for inactive/failed units. An
            # empty/unknown response or a failed bus query cannot prove that
            # the writer stopped, even if the command exits non-zero.
            if state not in ('inactive','failed') or result.returncode!=3:
                parser.error('无法确认实验写者已停止，服务状态查询失败：'+unit)
        if args.backup is None:parser.error('--apply requires --backup outside the experiment directory')
    print(json.dumps(prepare_storage(args.database,apply=args.apply,backup_path=args.backup),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
