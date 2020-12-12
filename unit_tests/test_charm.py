import unittest

from unittest import mock

from ops.model import BlockedStatus
from ops.testing import Harness

import charm


class TestMssqlCharm(unittest.TestCase):

    def setUp(self):
        self.harness = Harness(charm.MSSQLCharm)
        self.addCleanup(self.harness.cleanup)

    @mock.patch.object(charm, 'apt_install')
    @mock.patch.object(charm, 'apt_update')
    @mock.patch.object(charm, 'add_source')
    @mock.patch.object(charm, 'urlopen')
    def test_on_install(self,
                        _urlopen, _add_source, _apt_update, _apt_install):
        gpg_key_url = charm.MSSQLCharm.GPG_KEY_URL
        apt_repo_url = charm.MSSQLCharm.APT_REPO_URL_MAP['2019']
        apt_packages = charm.MSSQLCharm.APT_PACKAGES
        test_gpg_key = 'test_gpg_key_url'
        test_apt_repo = 'test_apt_repo'

        gpg_key_mock = mock.MagicMock()
        gpg_key_mock.read.return_value = test_gpg_key.encode()
        apt_repo_mock = mock.MagicMock()
        apt_repo_mock.read.return_value = test_apt_repo.encode()
        _urlopen.side_effect = [gpg_key_mock, apt_repo_mock]

        self.harness.begin()
        self.harness.charm.on.install.emit()

        _urlopen.assert_has_calls([
            mock.call(gpg_key_url),
            mock.call(apt_repo_url)
        ])
        _add_source.assert_called_once_with(
            source=test_apt_repo,
            key=test_gpg_key,
            fail_invalid=True)
        _apt_update.assert_called_once_with(
            fatal=True)
        _apt_install.assert_called_once_with(
            packages=apt_packages,
            fatal=True)

    @mock.patch.object(charm, 'subprocess')
    @mock.patch.object(charm, 'service')
    def test_initialize_mssql_successfully(self, _service, _subprocess):
        self.harness.update_config({
            'accept-eula': True
        })
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        self.harness.update_relation_data(
            rel_id,
            'mssql',
            {'sa_password': 'test_sa_password'})

        self.harness.begin()
        self.harness.charm.cluster.on_initialized_unit = mock.MagicMock()
        self.harness.charm.cluster.on.ready_sa.emit()

        self.harness.charm.cluster.on_initialized_unit.assert_called_once()
        _service.assert_called_once_with(
            'stop', charm.MSSQLCharm.SERVICE_NAME)
        _subprocess.check_call.assert_called_once_with(
            args=['/opt/mssql/bin/mssql-conf', '-n', 'setup'],
            env={'ACCEPT_EULA': 'Y',
                 'MSSQL_PID': 'Developer',
                 'MSSQL_SA_PASSWORD': 'test_sa_password',
                 'MSSQL_ENABLE_HADR': '1'})
        self.assertTrue(self.harness.charm.state.initialized)
        self.assertEqual(self.harness.charm.unit.status,
                         charm.MSSQLCharm.UNIT_INITIALIZED_UNCLUSTERED_STATUS)

    def test_initialize_mssql_invalid_config(self):
        self.harness.update_config({
            'product-id': 'Invalid Product ID',
            'accept-eula': True
        })
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        self.harness.update_relation_data(
            rel_id,
            'mssql',
            {'sa_password': 'test_sa_password'})

        self.harness.begin()
        self.harness.charm.cluster.on.ready_sa.emit()

        self.assertFalse(self.harness.charm.state.initialized)

    def test_initialize_mssql_empty_sa_password(self):
        self.harness.update_config({'accept-eula': True})

        self.harness.begin()
        self.harness.charm.cluster.on.ready_sa.emit()

        self.assertFalse(self.harness.charm.state.initialized)

    def test_validate_product_id_successfully(self):
        self.harness.update_config({'product-id': 'Enterprise'})

        self.harness.disable_hooks()
        self.harness.begin()

        self.assertTrue(self.harness.charm._validate_product_id())

    def test_validate_product_id_valid_product_key(self):
        self.harness.update_config({
            'product-id': 'ABC00-ABC11-ABC22-ABC33-ABC44'
        })

        self.harness.disable_hooks()
        self.harness.begin()

        self.assertTrue(self.harness.charm._validate_product_id())

    def test_validate_product_id_invalid(self):
        self.harness.update_config({'product-id': 'Invalid Product ID'})

        self.harness.disable_hooks()
        self.harness.begin()

        self.assertFalse(self.harness.charm._validate_product_id())

    def test_validate_config_successfully(self):
        self.harness.update_config({'accept-eula': True})

        self.harness.disable_hooks()
        self.harness.begin()

        self.assertTrue(self.harness.charm._validate_config())

    def test_validate_config_missing_product_id(self):
        self.harness.update_config({'product-id': ''})

        self.harness.disable_hooks()
        self.harness.begin()

        self.assertFalse(self.harness.charm._validate_config())
        expected_status = BlockedStatus(
            'Missing configuration: {}'.format(['product-id']))
        self.assertEqual(self.harness.charm.unit.status, expected_status)

    def test_validate_config_eula_not_accepted(self):
        self.harness.disable_hooks()
        self.harness.begin()

        self.assertFalse(self.harness.charm._validate_config())
        self.assertEqual(self.harness.charm.unit.status,
                         BlockedStatus('The MSSQL EULA is not accepted'))

    def test_validate_config_eula_invalid_product_id(self):
        self.harness.update_config({
            'accept-eula': True,
            'product-id': 'Invalid Product ID'
        })

        self.harness.disable_hooks()
        self.harness.begin()

        self.assertFalse(self.harness.charm._validate_config())
        self.assertEqual(self.harness.charm.unit.status,
                         BlockedStatus('Invalid MSSQL product id'))
