"""Read-only, credential-free snapshot for roll-trigger research."""
import argparse
import gzip
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def rows(conn, sql):
    return [dict(row) for row in conn.execute(sql)]


def extract(path):
    payload = {'extracted_at_utc': datetime.now(timezone.utc).isoformat()}
    upstream = sqlite3.connect('file:/www/wwwroot/AITradingSimulator/pc28_predictor.db?mode=ro', uri=True, timeout=30)
    upstream.row_factory = sqlite3.Row
    upstream.execute('BEGIN')
    payload['draws'] = rows(upstream, "SELECT issue_no,open_time,result_number,source_payload,created_at FROM lottery_draws WHERE lottery_type='pc28' ORDER BY CAST(issue_no AS INTEGER)")
    payload['predictors'] = rows(upstream, "SELECT id,name,engine_type,algorithm_key,enabled FROM predictors WHERE lottery_type='pc28'")
    payload['predictions'] = rows(upstream, "SELECT id,predictor_id,issue_no,prediction_big_small,prediction_odd_even,prediction_combo,hit_big_small,hit_odd_even,hit_combo,created_at,settled_at,status FROM predictions WHERE lottery_type='pc28' ORDER BY predictor_id,CAST(issue_no AS INTEGER),id")
    upstream.close()
    local = sqlite3.connect('file:/www/wwwroot/pc28touzhu/pc28touzhu.db?mode=ro', uri=True, timeout=30)
    local.row_factory = sqlite3.Row
    local.execute('BEGIN')
    payload['roll_events'] = rows(local, "SELECT id,subscription_id,signal_id,issue_no,leg_key,status,stake_amount,net_delta,resolved_result_type,settlement_rule_id,settlement_snapshot_json,result_context_json,auto_trigger_rule_id,auto_trigger_rule_run_id,auto_trigger_route_id,created_at,settled_at FROM subscription_progression_events WHERE leg_key IN ('main','aux') ORDER BY id")
    payload['roll_runs'] = rows(local, "SELECT id,rule_id,subscription_id,stat_date,window_id,started_issue_no,status,stop_reason,started_at,stopped_at FROM auto_trigger_rule_runs WHERE subscription_id IN (SELECT DISTINCT subscription_id FROM subscription_progression_events WHERE leg_key IN ('main','aux'))")
    payload['roll_jobs'] = rows(local, "SELECT id,progression_event_id,status,planned_message_text,stake_plan_json,send_with_job_id FROM execution_jobs WHERE leg_key IN ('main','aux') ORDER BY id")
    payload['roll_attempts'] = rows(local, "SELECT job_id,delivery_status,executed_at,remote_message_id FROM execution_attempts WHERE job_id IN (SELECT id FROM execution_jobs WHERE leg_key IN ('main','aux'))")
    payload['roll_rules'] = rows(local, "SELECT id,name,status,conditions_json,guard_groups_json,trigger_mode,schedule_json,cycle_policy_json,cooldown_issues FROM auto_trigger_rules WHERE id IN (9,10,11)")
    local.close()
    os.makedirs(Path(path).parent, mode=0o700, exist_ok=True)
    with gzip.open(path, 'wt', encoding='utf-8') as out:
        json.dump(payload,out,ensure_ascii=False)
    os.chmod(path,0o600)
    print(json.dumps({k:len(v) for k,v in payload.items() if isinstance(v,list)}))
    print(payload['extracted_at_utc'])


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output');args=parser.parse_args();extract(args.output)
