import sqlite3,tempfile,unittest
from pathlib import Path
from pc28touzhu.services.rule_lab_store import RuleLabStore
class ResearchMigrationTests(unittest.TestCase):
    def test_schema8_adds_favorites_without_rewriting_original_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=RuleLabStore(Path(tmp)/'lab.db');store.initialize()
            with sqlite3.connect(store.db_path) as c:
                before={name:c.execute('SELECT * FROM '+name+' ORDER BY rowid').fetchall() for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'lab_%' AND name!='lab_favorites'")}
                self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],8)
                self.assertTrue(c.execute("SELECT 1 FROM sqlite_master WHERE name='lab_favorites'").fetchone())
                c.execute('PRAGMA user_version=7');c.execute('DROP TABLE lab_favorites')
            store.initialize();store.initialize()
            with sqlite3.connect(store.db_path) as c:
                self.assertEqual(before,{name:c.execute('SELECT * FROM '+name+' ORDER BY rowid').fetchall() for name in before})
                self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],8)
if __name__=='__main__':unittest.main()
