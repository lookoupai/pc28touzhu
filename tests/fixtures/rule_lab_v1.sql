-- Shipped rule lab v1 schema from 083d607; no runtime data.
CREATE TABLE lab_batches (
                id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, name TEXT NOT NULL,
                mode TEXT NOT NULL, predictor_id INTEGER NOT NULL, predictor_name TEXT NOT NULL,
                metric TEXT NOT NULL, template_pack TEXT NOT NULL, stake_cents INTEGER NOT NULL,
                start_date TEXT, end_date TEXT, status TEXT NOT NULL DEFAULT 'paused',
                phase TEXT NOT NULL DEFAULT 'idle', error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL, started_at TEXT, updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL DEFAULT '{}', revision INTEGER NOT NULL DEFAULT 0,
                processed_issues INTEGER NOT NULL DEFAULT 0, total_issues INTEGER,
                skip_counts_json TEXT NOT NULL DEFAULT '{}');

CREATE TABLE lab_daily (
                experiment_id INTEGER NOT NULL REFERENCES lab_experiments(id), stat_date TEXT NOT NULL,
                settled_count INTEGER NOT NULL DEFAULT 0, hit_count INTEGER NOT NULL DEFAULT 0,
                refund_count INTEGER NOT NULL DEFAULT 0, miss_count INTEGER NOT NULL DEFAULT 0,
                direction_hit_count INTEGER NOT NULL DEFAULT 0, stake_cents INTEGER NOT NULL DEFAULT 0,
                net_cents INTEGER NOT NULL DEFAULT 0, trigger_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(experiment_id,stat_date));

CREATE TABLE lab_decisions (
                id INTEGER PRIMARY KEY, experiment_id INTEGER NOT NULL REFERENCES lab_experiments(id),
                issue_no TEXT NOT NULL, issue_key INTEGER NOT NULL, bet_value TEXT NOT NULL,
                stake_cents INTEGER NOT NULL, decided_at TEXT NOT NULL, decision_mode TEXT NOT NULL,
                feature_issue_no TEXT, condition_value_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                skip_reason TEXT NOT NULL DEFAULT '', settled_at TEXT, finalized_at TEXT, result_type TEXT, net_cents INTEGER,
                refund_reason TEXT NOT NULL DEFAULT '', UNIQUE(experiment_id,issue_no));

CREATE TABLE lab_draws (
                issue_no TEXT PRIMARY KEY, issue_key INTEGER NOT NULL UNIQUE, source_id INTEGER,
                open_at TEXT NOT NULL, source_created_at TEXT NOT NULL, observed_at TEXT NOT NULL,
                triplet_json TEXT NOT NULL, input_hash TEXT NOT NULL, conflict INTEGER NOT NULL DEFAULT 0);

CREATE TABLE lab_experiments (
                id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL REFERENCES lab_batches(id),
                condition_key TEXT NOT NULL, name TEXT NOT NULL, condition_json TEXT NOT NULL,
                settlement_rule_id TEXT NOT NULL, settlement_snapshot_json TEXT NOT NULL,
                stats_json TEXT NOT NULL, last_settled_issue INTEGER NOT NULL DEFAULT 0,
                last_decided_issue INTEGER NOT NULL DEFAULT 0,
                UNIQUE(batch_id,condition_key,settlement_rule_id));

CREATE TABLE lab_predictions (
                predictor_id INTEGER NOT NULL, issue_no TEXT NOT NULL, issue_key INTEGER NOT NULL,
                source_id INTEGER, created_at TEXT NOT NULL, settled_at TEXT, observed_at TEXT NOT NULL,
                status TEXT NOT NULL, big_small TEXT, odd_even TEXT, combo TEXT, input_hash TEXT NOT NULL,
                conflict INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(predictor_id,issue_no));

CREATE TABLE lab_runtime (key TEXT PRIMARY KEY,value_json TEXT NOT NULL);

CREATE INDEX lab_batches_owner ON lab_batches(user_id,id);

CREATE INDEX lab_decisions_finished ON lab_decisions(status,finalized_at);

CREATE INDEX lab_decisions_pending ON lab_decisions(status,issue_key);

CREATE INDEX lab_predictions_issue ON lab_predictions(issue_key,predictor_id);
PRAGMA user_version=1;
