"""Resource control has no task/ledger writes and never runs production sudo in tests."""
import subprocess
import unittest
from pathlib import Path
from unittest.mock import Mock,patch

from pc28touzhu.services.rule_lab_resources import RuleLabResources,UNIT,HELPER
from pc28touzhu.services.rule_lab_store import RuleLabError


class FakeResources:
    def __init__(self):self.mode='economy';self.changes=[]
    def status(self):
        return dict(available=True,mode=self.mode,cpu_cores=.2 if self.mode=='economy' else 1,
                    cpu_quota_percent=20 if self.mode=='economy' else 100,
                    memory_max_bytes=268435456,memory_current_bytes=10485760,worker_pid=123,
                    scope='global',persistent=True,modes=[],message='')
    def set_mode(self,mode):
        self.changes.append(mode);self.mode=mode;return self.status()


class ResourceControllerTests(unittest.TestCase):
    def setUp(self):
        self.run=Mock();self.controller=RuleLabResources(run=self.run)
        self.trusted=patch('pc28touzhu.services.rule_lab_resources._trusted_helper',return_value=True)
        self.trusted.start();self.addCleanup(self.trusted.stop)
        self.cpu=patch.object(Path,'read_text',return_value='20000 100000\n')
        self.cpu_mock=self.cpu.start();self.addCleanup(self.cpu.stop)
        self.run.return_value=subprocess.CompletedProcess([],0,
            'ActiveState=active\nSubState=running\nMainPID=123\nCPUQuotaPerSecUSec=200ms\n'
            'MemoryMax=268435456\nMemoryCurrent=10485760\nControlGroup=/system.slice/'+UNIT+'\n','')

    def test_reads_real_cgroup_and_memory_without_sudo(self):
        result=self.controller.status()
        self.assertTrue(result['available']);self.assertEqual(result['mode'],'economy')
        self.assertEqual(result['cpu_cores'],.2);self.assertEqual(result['memory_current_bytes'],10485760)
        self.assertTrue(all('/usr/bin/sudo' not in call.args[0] for call in self.run.call_args_list))

    def test_cgroup_manager_mismatch_does_not_claim_requested_mode(self):
        self.cpu_mock.return_value='100000 100000\n'
        result=self.controller.status();self.assertFalse(result['available']);self.assertIn('不一致',result['message'])

    def test_missing_helper_or_unreadable_limits_disable_switch(self):
        with patch('pc28touzhu.services.rule_lab_resources._trusted_helper',return_value=False):
            self.assertFalse(self.controller.status()['available'])
        self.cpu_mock.return_value='max 100000'
        self.assertFalse(self.controller.status()['available'])
        self.run.side_effect=subprocess.TimeoutExpired('systemctl',3)
        result=self.controller.status();self.assertFalse(result['available']);self.assertNotIn('systemctl',result['message'])

    def test_set_uses_exact_allowed_command_and_confirms_effect(self):
        economy=FakeResources().status();fast={**economy,'mode':'fast','cpu_cores':1,'cpu_quota_percent':100}
        with patch.object(self.controller,'status',side_effect=[economy,fast]):
            result=self.controller.set_mode('fast')
        self.assertEqual(result['mode'],'fast')
        self.assertEqual(self.run.call_args.args[0],['/usr/bin/sudo','-n',str(HELPER),'fast'])
        self.assertEqual(self.run.call_args.kwargs['timeout'],11)
        self.assertNotIn('shell',self.run.call_args.kwargs)

    def test_same_mode_is_idempotent_and_invalid_input_never_invokes_command(self):
        with patch.object(self.controller,'status',return_value=FakeResources().status()):
            self.controller.set_mode('economy')
        self.run.assert_not_called()
        for value in ('fast;reboot','turbo',None,[],{},True):
            with self.subTest(value=value),self.assertRaises(RuleLabError):self.controller.set_mode(value)
        self.run.assert_not_called()

    def test_failed_or_unconfirmed_change_is_not_success_or_automatic_retry(self):
        for failure in (subprocess.TimeoutExpired('sudo',11),subprocess.CalledProcessError(1,'sudo',stderr='/private/path')):
            with patch.object(self.controller,'status',return_value=FakeResources().status()):
                self.run.side_effect=failure
                with self.assertRaises(RuleLabError) as cm:self.controller.set_mode('fast')
                self.assertEqual(cm.exception.status,503);self.assertNotIn('/private',cm.exception.message)
        self.run.reset_mock();self.run.side_effect=None
        with patch.object(self.controller,'status',return_value=FakeResources().status()):
            with self.assertRaises(RuleLabError) as cm:self.controller.set_mode('fast')
            self.assertEqual(cm.exception.code,'performance_unconfirmed')
        self.assertEqual(self.run.call_count,1)

    def test_helper_rejects_all_non_whitelisted_arguments_without_service_calls(self):
        helper=Path(__file__).resolve().parents[1]/'deploy/systemd/pc28-rule-lab-resource-mode'
        for args in ([],['fast','extra'],['reboot'],['fast;reboot'],['CPUQuota=999%']):
            result=subprocess.run(['/bin/sh',str(helper),*args],capture_output=True,timeout=2)
            self.assertEqual(result.returncode,64)
        with helper.open() as handle:content=handle.read()
        self.assertIn('/usr/bin/env -i',content);self.assertIn('CPUQuota=20%',content);self.assertIn('CPUQuota=100%',content)
        with helper.with_name('pc28-rule-lab-resource.sudoers').open() as handle:sudoers=handle.read()
        self.assertNotIn(' *',sudoers);self.assertNotIn('/usr/bin/systemctl',sudoers)


if __name__=='__main__':unittest.main()
