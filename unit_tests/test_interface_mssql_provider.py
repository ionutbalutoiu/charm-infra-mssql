import unittest

from unittest import mock

from ops.testing import Harness
from ops.charm import CharmBase

import interface_hacluster
import interface_mssql_provider
import interface_mssql_cluster


class TestInterfaceMssqlDBProvider(unittest.TestCase):

    TEST_VIP_ADDRESS = '10.0.0.100'

    def setUp(self):
        self.harness = Harness(CharmBase, meta='''
            name: mssql
            provides:
              db:
                interface: mssql
            peers:
              cluster:
                interface: mssql-cluster
            requires:
              ha:
                interface: hacluster
                scope: container
        ''')
        self.harness.update_config({'vip': self.TEST_VIP_ADDRESS})
        self.addCleanup(self.harness.cleanup)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'set_unit_rel_nonce')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch('charmhelpers.core.host.pwgen')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_primary_replica',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_hacluster.HaCluster,
                       'is_ha_cluster_ready',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_on_changed(self, _is_ag_ready, _is_ha_cluster_ready,
                        _is_primary_replica, _pwgen, _mssql_db_client,
                        _set_unit_rel_nonce):
        _is_ag_ready.return_value = True
        _is_ha_cluster_ready.return_value = True
        _is_primary_replica.return_value = True
        _pwgen.return_value = 'test-password'
        self.harness.set_leader()
        self.harness.begin()
        self.harness.charm.cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        self.harness.charm.ha = interface_hacluster.HaCluster(
            self.harness.charm, 'ha')
        self.harness.charm.db_provider = \
            interface_mssql_provider.MssqlDBProvider(self.harness.charm, 'db')
        rel_id = self.harness.add_relation('db', 'mssqlconsumer')
        self.harness.add_relation_unit(rel_id, 'mssqlconsumer/0')
        self.harness.update_relation_data(
            rel_id,
            'mssqlconsumer/0',
            {
                'database': 'testdb',
                'username': 'testuser'
            })

        _pwgen.assert_called_once_with(32)
        _mssql_db_client.assert_called_once_with()
        db_client_mock = _mssql_db_client.return_value
        db_client_mock.create_database.assert_called_once_with(
            db_name='testdb',
            ag_name=self.harness.charm.cluster.AG_NAME)
        db_client_mock.create_login.assert_called_once_with(
            name='testuser',
            password='test-password')
        db_client_mock.grant_access.assert_called_once_with(
            db_name='testdb',
            db_user_name='testuser')
        _set_unit_rel_nonce.assert_called_once_with()

        rel_unit_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(rel_unit_data.get('db_host'),
                         self.harness.charm.ha.bind_address)
        self.assertEqual(rel_unit_data.get('password'), 'test-password')
        rel_app_data = self.harness.get_relation_data(rel_id, 'mssql')
        self.assertEqual(rel_app_data.get('db_host'),
                         self.harness.charm.ha.bind_address)
        self.assertEqual(rel_app_data.get('password'), 'test-password')
