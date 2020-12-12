import json
import unittest

from unittest import mock

from ops.testing import Harness
from ops.charm import CharmBase

from charmhelpers.contrib.openstack.ha.utils import VIP_GROUP_NAME

import interface_hacluster
import interface_mssql_cluster


class TestInterfaceHaCluster(unittest.TestCase):

    def setUp(self):
        self.harness = Harness(CharmBase, meta='''
            name: mssql
            peers:
              cluster:
                interface: mssql-cluster
            requires:
              ha:
                interface: hacluster
                scope: container
        ''')
        self.addCleanup(self.harness.cleanup)

    @mock.patch.object(interface_hacluster,
                       'update_hacluster_vip')
    @mock.patch.object(interface_hacluster.HaCluster,
                       'setup_pacemaker_mssql_login')
    @mock.patch.object(interface_hacluster,
                       'apt_install')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_on_joined(self, _is_ag_ready, _apt_install,
                       _setup_pacemaker_mssql_login, _update_hacluster_vip):
        _is_ag_ready.return_value = True
        self.harness.begin()
        self.harness.charm.cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        self.harness.charm.ha = interface_hacluster.HaCluster(
            self.harness.charm, 'ha')
        rel_id = self.harness.add_relation('ha', 'hacluster')
        self.harness.add_relation_unit(rel_id, 'hacluster/0')

        _apt_install.assert_called_once_with(
            packages=self.harness.charm.ha.APT_PACKAGES, fatal=True)
        _setup_pacemaker_mssql_login.assert_called_once_with()
        _update_hacluster_vip.assert_called_once()

        rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        expected_rel_data = {}
        keys = ['resources', 'resource_params', 'ms', 'colocations', 'orders']
        for key in keys:
            json_value = rel_data.get('json_{}'.format(key))
            self.assertIsNotNone(json_value)
            expected_rel_data.update({key: json.loads(json_value)})
        group_name = VIP_GROUP_NAME.format(service='mssql')
        self.assertDictEqual(
            expected_rel_data,
            {
                'resources': {
                    'ag_cluster': 'ocf:mssql:ag'
                },
                'resource_params': {
                    'ag_cluster':
                    'params ag_name="{}" '
                    'meta failure-timeout=60s '
                    'op start timeout=60s '
                    'op stop timeout=60s '
                    'op promote timeout=60s '
                    'op demote timeout=10s '
                    'op monitor timeout=60s interval=10s '
                    'op monitor timeout=60s interval=11s role="Master" '
                    'op monitor timeout=60s interval=12s role="Slave" '
                    'op notify timeout=60s'.format(
                        self.harness.charm.cluster.AG_NAME)
                },
                'ms': {
                    'ms-ag_cluster':
                    'ag_cluster meta '
                    'master-max="1" master-node-max="1" '
                    'clone-max="3" clone-node-max="1" notify="true"'
                },
                'colocations': {
                    'vip_on_master':
                    'inf: {} ms-ag_cluster:Master'.format(group_name)
                },
                'orders': {
                    'ag_first':
                    'inf: ms-ag_cluster:promote {}:start'.format(group_name)
                }
            })

    def test_on_changed(self):
        self.harness.begin()
        self.harness.charm.cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        self.harness.charm.ha = interface_hacluster.HaCluster(
            self.harness.charm, 'ha')
        rel_id = self.harness.add_relation('ha', 'hacluster')
        self.harness.add_relation_unit(rel_id, 'hacluster/0')
        self.harness.update_relation_data(
            rel_id, 'hacluster/0', {'clustered': 'yes'})

        self.assertEqual(self.harness.charm.unit.status,
                         self.harness.charm.ha.UNIT_ACTIVE_STATUS)
        self.assertTrue(self.harness.charm.ha.state.ha_cluster_ready)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_hacluster.HaCluster,
                       'setup_pacemaker_mssql_login')
    def test_on_created_ag(self, _setup_pacemaker_mssql_login,
                           _mssql_db_client):
        self.harness.begin()
        self.harness.charm.cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        self.harness.charm.ha = interface_hacluster.HaCluster(
            self.harness.charm, 'ha')
        self.harness.charm.cluster.on.created_ag.emit()

        _setup_pacemaker_mssql_login.assert_called_once_with()
        _mssql_db_client.assert_called_once_with()
        db_client_mock = _mssql_db_client.return_value
        db_client_mock.return_value.exec_t_sql.assert_called_once_with("""
        GRANT ALTER, CONTROL, VIEW DEFINITION
            ON AVAILABILITY GROUP::[{ag_name}] TO [{login_name}]
        GRANT VIEW SERVER STATE TO [{login_name}]
        """.format(ag_name=self.harness.charm.cluster.AG_NAME,
                   login_name=self.harness.charm.ha.PACEMAKER_LOGIN_NAME))

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch('charmhelpers.core.host.pwgen')
    @mock.patch('os.chown')
    @mock.patch('os.chmod')
    @mock.patch('builtins.open', new_callable=mock.mock_open)
    def test_setup_pacemaker_mssql_login(
            self, _open, _chmod, _chown, _pwgen, _mssql_db_client):

        _pwgen.return_value = 'test-password'
        self.harness.begin()
        self.harness.charm.cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        self.harness.charm.ha = interface_hacluster.HaCluster(
            self.harness.charm, 'ha')
        self.harness.charm.ha.setup_pacemaker_mssql_login()

        _pwgen.assert_called_once_with(32)
        _mssql_db_client.assert_called_once_with()
        db_client_mock = _mssql_db_client.return_value
        db_client_mock.create_login.assert_called_once_with(
            name=self.harness.charm.ha.PACEMAKER_LOGIN_NAME,
            password='test-password',
            server_roles=['sysadmin'])
        _open.assert_called_once_with(
            self.harness.charm.ha.PACEMAKER_LOGIN_CREDS_FILE, 'w')
        _open.return_value.write.assert_called_once_with(
            '{}\n{}\n'.format(self.harness.charm.ha.PACEMAKER_LOGIN_NAME,
                              'test-password'))
        _chmod.assert_called_once_with(
            self.harness.charm.ha.PACEMAKER_LOGIN_CREDS_FILE, 0o400)
        _chown.assert_called_once_with(
            self.harness.charm.ha.PACEMAKER_LOGIN_CREDS_FILE, 0, 0)
        self.assertTrue(self.harness.charm.ha.state.pacemaker_login_ready)
