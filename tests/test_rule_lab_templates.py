"""模板库与迁移的可观察约束。"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests import test_rule_lab_api as fixtures
PREFIX=fixtures.PREFIX
from pc28touzhu.domain.rule_lab import build_conditions


class TemplateCatalogTests(unittest.TestCase):
    setUp=fixtures.RuleLabApiTests.setUp
    request=fixtures.RuleLabApiTests.request

    def test_builtin_catalog_exposes_exact_conditions_without_mutation_routes(self):
        result=self.request(PREFIX+'/templates',query='kind=condition&metric=combo')
        self.assertEqual(result['status_code'],200,result)
        items=result['json']['templates']
        pack=next(t for t in items if t['ref'].get('key')=='basic_v2')
        self.assertTrue(pack['builtin'])
        self.assertEqual(pack['definition']['conditions'],build_conditions('combo'))
        self.assertEqual(self.request(PREFIX+'/templates/builtin/edit',dict(request_id='no-builtin-edit'))['status_code'],404)

    def test_schema_is_seven_and_reinitialization_keeps_existing_rows(self):
        self.assertEqual(self.request(PREFIX+'/templates')['status_code'],200)
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],7)
            before={t:c.execute('SELECT * FROM '+t+' ORDER BY rowid').fetchall() for t, in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.store.initialize()
        with sqlite3.connect(self.store.db_path) as c:
            after={t:c.execute('SELECT * FROM '+t+' ORDER BY rowid').fetchall() for t, in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(before,after)
