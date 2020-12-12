import string
import unittest

from base64 import b64encode
from unittest import mock

from ops.testing import Harness
from ops.charm import CharmBase

import interface_mssql_cluster


class TestInterfaceMssqlCluster(unittest.TestCase):

    TEST_BIND_ADDRESS = '10.0.0.10'
    TEST_NODE_NAME = 'test-mssql-node'
    TEST_MASTER_CERT = {
        'master_key_password': 'test_key_password',
        'master_cert': b64encode('test_master_cert'.encode()).decode(),
        'master_cert_key': b64encode('test_master_cert_key'.encode()).decode(),
        'master_cert_key_password': 'test_cert_key_password',
    }
    TEST_PRIMARY_REPLICA_NAME = 'test-primary-name'
    TEST_PRIMARY_LOGINS = {
        'test-login-1': {
            'sid': 'sid1',
            'password_hash': 'test-password-hash1',
            'roles': ['test-role1']
        },
        'test-login-2': {
            'sid': 'sid2',
            'password_hash': 'test-password-hash2',
            'roles': ['test-role2']
        },
        'test-login-3': {
            'sid': 'sid3',
            'password_hash': 'test-password-hash3',
            'roles': ['test-role3']
        },
        'test-login-4': {
            'sid': 'sid4',
            'password_hash': 'test-password-hash4',
            'roles': ['test-role4']
        }
    }
    TEST_SECONDARY_LOGINS = {
        'test-login-1': {
            'sid': 'sid1',
            'password_hash': 'test-password-hash1',
            'roles': ['test-role1']
        },
        'test-login-2': {
            'sid': 'sid2',
            'password_hash': 'test-password-hash2',
            'roles': ['test-role2']
        }
    }

    def setUp(self):
        self.harness = Harness(CharmBase, meta='''
            name: mssql
            peers:
              cluster:
                interface: mssql-cluster
        ''')
        self.addCleanup(self.harness.cleanup)

        mocked_node_name = mock.patch.object(
            interface_mssql_cluster.MssqlCluster,
            'node_name',
            new_callable=mock.PropertyMock).start()
        mocked_node_name.return_value = self.TEST_NODE_NAME
        mocked_bind_address = mock.patch.object(
            interface_mssql_cluster.MssqlCluster,
            'bind_address',
            new_callable=mock.PropertyMock).start()
        mocked_bind_address.return_value = self.TEST_BIND_ADDRESS
        self.addCleanup(mock.patch.stopall)

    def test_on_joined(self):
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.initialized_nodes = {
            self.TEST_NODE_NAME: {
                'address': self.TEST_BIND_ADDRESS,
                'ready_to_cluster': 'true',
            }
        }
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')

        rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(rel_data.get('node_name'), self.TEST_NODE_NAME)
        self.assertEqual(rel_data.get('node_address'), self.TEST_BIND_ADDRESS)
        self.assertEqual(rel_data.get('ready_to_cluster'), 'true')
        self.assertIsNone(rel_data.get('clustered'))

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_cluster_node')
    @mock.patch.object(interface_mssql_cluster,
                       'append_hosts_entry')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'set_sa_password')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'master_cert',
                       new_callable=mock.PropertyMock)
    def test_on_changed(self, _master_cert, _set_sa_password,
                        _append_hosts_entry, _configure_cluster_node):
        _master_cert.return_value = self.TEST_MASTER_CERT
        self.harness.set_leader()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        self.harness.update_relation_data(
            rel_id,
            'mssql/1',
            {
                'node_name': self.TEST_NODE_NAME,
                'node_address': self.TEST_BIND_ADDRESS,
                'ready_to_cluster': 'true',
            })

        node_state = cluster.state.initialized_nodes.get(self.TEST_NODE_NAME)
        self.assertIsNotNone(node_state)
        self.assertEqual(node_state.get('address'), self.TEST_BIND_ADDRESS)
        self.assertTrue(node_state.get('ready_to_cluster'))
        self.assertIsNone(node_state.get('clustered'))
        _set_sa_password.assert_called_once_with()
        _append_hosts_entry.assert_called_once_with(
            self.TEST_BIND_ADDRESS, [self.TEST_NODE_NAME])
        _configure_cluster_node.assert_called_once_with()

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_cluster_node')
    @mock.patch.object(interface_mssql_cluster,
                       'append_hosts_entry')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'set_master_cert')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'master_cert',
                       new_callable=mock.PropertyMock)
    def test_on_initialized_unit(self, _master_cert, _set_master_cert,
                                 _append_hosts_entry, _configure_cluster_node):
        _master_cert.return_value = None
        self.harness.set_leader()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.on.initialized_unit.emit()

        node_state = cluster.state.initialized_nodes.get(self.TEST_NODE_NAME)
        self.assertIsNotNone(node_state)
        self.assertEqual(node_state.get('address'), self.TEST_BIND_ADDRESS)
        self.assertIsNone(node_state.get('ready_to_cluster'))
        self.assertIsNone(node_state.get('clustered'))

        rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(rel_data.get('node_name'), self.TEST_NODE_NAME)
        self.assertEqual(rel_data.get('node_address'), self.TEST_BIND_ADDRESS)
        self.assertIsNone(rel_data.get('ready_to_cluster'))
        self.assertIsNone(rel_data.get('clustered'))

        _append_hosts_entry.assert_called_once_with(
            self.TEST_BIND_ADDRESS, [self.TEST_NODE_NAME])
        _set_master_cert.assert_called_once_with()
        _configure_cluster_node.assert_not_called()

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'master_cert',
                       new_callable=mock.PropertyMock)
    def test_configure_master_cert(self, _master_cert, _mssql_db_client):
        _master_cert.return_value = self.TEST_MASTER_CERT
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.initialized_nodes = {
            self.TEST_NODE_NAME: {
                'address': self.TEST_BIND_ADDRESS,
                'ready_to_cluster': 'true',
            }
        }
        cluster.configure_master_cert()

        self.assertTrue(cluster.state.master_cert_configured)
        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.create_master_encryption_key.assert_called_once_with(
            'test_key_password')
        mock_ret_value.setup_master_cert.assert_called_once_with(
            'test_master_cert'.encode(),
            'test_master_cert_key'.encode(),
            'test_cert_key_password')

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_secondary_replica')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_primary_replica')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_primary_replica',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_master_cert')
    def test_configure_cluster_node_primary_replica(
            self, _configure_master_cert, _mssql_db_client,
            _is_primary_replica, _configure_primary_replica,
            _configure_secondary_replica):

        _is_primary_replica.return_value = True
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.initialized_nodes[self.TEST_NODE_NAME] = {
            'address': self.TEST_BIND_ADDRESS
        }
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.configure_cluster_node()

        _configure_master_cert.assert_called_once_with()
        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.setup_db_mirroring_endpoint.assert_called_once_with()
        self.assertTrue(
            cluster.state.initialized_nodes[
                self.TEST_NODE_NAME]['ready_to_cluster'])
        rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(rel_data.get('ready_to_cluster'), 'true')
        _configure_primary_replica.assert_called_once_with()
        _configure_secondary_replica.assert_not_called()

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_secondary_replica')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_primary_replica')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_primary_replica',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'configure_master_cert')
    def test_configure_cluster_node_secondary_replica(
            self, _configure_master_cert, _mssql_db_client,
            _is_primary_replica, _configure_primary_replica,
            _configure_secondary_replica):

        _is_primary_replica.return_value = False
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.initialized_nodes[self.TEST_NODE_NAME] = {
            'address': self.TEST_BIND_ADDRESS
        }
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.configure_cluster_node()

        _configure_master_cert.assert_called_once_with()
        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.setup_db_mirroring_endpoint.assert_called_once_with()
        self.assertTrue(
            cluster.state.initialized_nodes[
                self.TEST_NODE_NAME]['ready_to_cluster'])
        rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(rel_data.get('ready_to_cluster'), 'true')
        _configure_primary_replica.assert_not_called()
        _configure_secondary_replica.assert_called_once_with()

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'create_ag')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_configure_primary_replica_ag_not_ready(self, _is_ag_ready,
                                                    _create_ag):
        _is_ag_ready.return_value = False
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.configure_primary_replica()
        _create_ag.assert_called_once_with()

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'create_ag')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ag_replicas',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ready_nodes',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_configure_primary_replica_ag_ready(
            self, _is_ag_ready, _ready_nodes, _ag_replicas, _create_ag,
            _mssql_db_client):

        _is_ag_ready.return_value = True
        _ready_nodes.return_value = {
            'test-node-1': {'address': '10.0.0.11'},
            'test-node-2': {'address': '10.0.0.12'},
            'test-node-3': {'address': '10.0.0.13'}
        }
        _ag_replicas.return_value = ['test-node-1', 'test-node-2']
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.configure_primary_replica()

        _create_ag.assert_not_called()
        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.add_replicas.assert_called_once_with(
            cluster.AG_NAME, {'test-node-3': {'address': '10.0.0.13'}})
        rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertIsNotNone(rel_data.get('nonce'))

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'sync_logins_from_primary_replica')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'join_existing_ag')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ag_primary_replica',
                       new_callable=mock.PropertyMock)
    def test_configure_secondary_replica(
            self, _ag_primary_replica, _is_ag_ready, _mssql_db_client,
            _join_existing_ag, _sync_logins_from_primary_replica):

        _is_ag_ready.return_value = True
        _ag_primary_replica.return_value = self.TEST_PRIMARY_REPLICA_NAME
        _mssql_db_client.return_value.get_ag_replicas.return_value = \
            [self.TEST_PRIMARY_REPLICA_NAME, self.TEST_NODE_NAME]
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.configure_secondary_replica()

        _mssql_db_client.assert_called_once_with(
            self.TEST_PRIMARY_REPLICA_NAME)
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.get_ag_replicas.assert_called_once_with(cluster.AG_NAME)
        _join_existing_ag.assert_called_once_with()
        _sync_logins_from_primary_replica.assert_called_once_with()

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ag_primary_replica',
                       new_callable=mock.PropertyMock)
    def test_sync_logins_from_primary_replica(self, _ag_primary_replica,
                                              _mssql_db_client):
        _ag_primary_replica.return_value = self.TEST_PRIMARY_REPLICA_NAME
        mocked_primary_db_client = mock.MagicMock()
        mocked_primary_db_client.get_sql_logins.return_value = \
            self.TEST_PRIMARY_LOGINS
        mocked_this_db_client = mock.MagicMock()
        mocked_this_db_client.get_sql_logins.return_value = \
            self.TEST_SECONDARY_LOGINS
        _mssql_db_client.side_effect = [
            mocked_primary_db_client, mocked_this_db_client]
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.sync_logins_from_primary_replica()

        _mssql_db_client.assert_has_calls([
            mock.call(self.TEST_PRIMARY_REPLICA_NAME),
            mock.call()
        ])
        mocked_this_db_client.assert_has_calls([
            mock.call.get_sql_logins()
        ])
        mocked_this_db_client.assert_has_calls([
            mock.call.get_sql_logins(),
            mock.call.create_login(name='test-login-3',
                                   sid='sid3',
                                   password='test-password-hash3',
                                   is_hashed_password=True,
                                   server_roles=['test-role3']),
            mock.call.create_login(name='test-login-4',
                                   sid='sid4',
                                   password='test-password-hash4',
                                   is_hashed_password=True,
                                   server_roles=['test-role4'])
        ])

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ready_nodes',
                       new_callable=mock.PropertyMock)
    def test_create_ag(self, _ready_nodes, _mssql_db_client):
        _ready_nodes.return_value = ['node1', 'node2', 'node3']
        self.harness.set_leader()
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.create_ag()

        self.assertTrue(cluster.state.ag_configured)
        self.assertEqual(self.harness.charm.unit.status,
                         cluster.UNIT_ACTIVE_STATUS)
        _mssql_db_client.assert_called_once_with()
        _mssql_db_client.return_value.create_ag.assert_called_once_with(
            cluster.AG_NAME, ['node1', 'node2', 'node3'])
        unit_rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(unit_rel_data.get('clustered'), 'true')
        self.assertIsNotNone(unit_rel_data.get('nonce'))
        app_rel_data = self.harness.get_relation_data(rel_id, 'mssql')
        self.assertEqual(app_rel_data.get('ag_ready'), 'true')

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    def test_join_existing_ag(self, _mssql_db_client):
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.join_existing_ag()

        self.assertTrue(cluster.state.ag_configured)
        self.assertEqual(self.harness.charm.unit.status,
                         cluster.UNIT_ACTIVE_STATUS)
        _mssql_db_client.assert_called_once_with()
        _mssql_db_client.return_value.join_ag.assert_called_once_with(
            cluster.AG_NAME)
        unit_rel_data = self.harness.get_relation_data(rel_id, 'mssql/0')
        self.assertEqual(unit_rel_data.get('clustered'), 'true')

    @mock.patch.object(interface_mssql_cluster,
                       'append_hosts_entry')
    def test_add_to_initialized_nodes(self, _append_hosts_entry):
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.add_to_initialized_nodes(
            self.TEST_NODE_NAME, self.TEST_BIND_ADDRESS,
            ready_to_cluster=True, clustered=True)

        node_state = cluster.state.initialized_nodes.get(self.TEST_NODE_NAME)
        self.assertIsNotNone(node_state)
        self.assertEqual(node_state.get('address'), self.TEST_BIND_ADDRESS)
        self.assertTrue(node_state.get('ready_to_cluster'))
        self.assertTrue(node_state.get('clustered'))

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch('charmhelpers.core.host.pwgen')
    def test_set_master_cert(self, _pwgen, _mssql_db_client):
        _pwgen.side_effect = [
            'test-master-key-password', 'test-master-cert-key-password']
        _mssql_db_client.return_value.create_master_cert.return_value = \
            ('test-cert'.encode(), 'test-cert-key'.encode())
        self.harness.set_leader()
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.set_master_cert()

        self.assertTrue(cluster.state.master_cert_configured)
        _pwgen.assert_has_calls([
            mock.call(32),
            mock.call(32)
        ])
        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.create_master_encryption_key.assert_called_once_with(
            'test-master-key-password')
        mock_ret_value.create_master_cert.assert_called_once_with(
            'test-master-cert-key-password')
        app_rel_data = self.harness.get_relation_data(rel_id, 'mssql')
        self.assertEqual(app_rel_data.get('master_key_password'),
                         'test-master-key-password')
        self.assertEqual(app_rel_data.get('master_cert'),
                         b64encode('test-cert'.encode()).decode())
        self.assertEqual(app_rel_data.get('master_cert_key'),
                         b64encode('test-cert-key'.encode()).decode())
        self.assertEqual(app_rel_data.get('master_cert_key_password'),
                         'test-master-cert-key-password')

    @mock.patch.object(interface_mssql_cluster.secrets, 'choice')
    def test_set_sa_password(self, _choice):
        _choice.return_value = 'p'
        self.harness.set_leader()
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        cluster.set_sa_password()

        _choice_calls = []
        _choice_calls += [mock.call(string.ascii_lowercase)] * 8
        _choice_calls += [mock.call(string.ascii_uppercase)] * 8
        _choice_calls += [mock.call(string.digits)] * 8
        _choice_calls += [mock.call(string.punctuation)] * 8
        _choice.assert_has_calls(_choice_calls)
        app_rel_data = self.harness.get_relation_data(rel_id, 'mssql')
        self.assertEqual(app_rel_data.get('sa_password'), 'p' * 32)

    def test_clustered_nodes(self):
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.initialized_nodes['node-1'] = {
            'address': '10.0.0.11',
            'clustered': True}
        cluster.state.initialized_nodes['node-2'] = {
            'address': '10.0.0.12',
            'clustered': True}
        cluster.state.initialized_nodes['node-3'] = {
            'address': '10.0.0.13'}
        clustered_nodes = cluster.clustered_nodes

        self.assertEqual(
            clustered_nodes,
            {
                'node-1': {
                    'address': '10.0.0.11',
                    'clustered': True
                },
                'node-2': {
                    'address': '10.0.0.12',
                    'clustered': True
                }
            })

    def test_ready_nodes(self):
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.initialized_nodes['node-1'] = {
            'address': '10.0.0.11',
            'ready_to_cluster': True}
        cluster.state.initialized_nodes['node-2'] = {
            'address': '10.0.0.12',
            'ready_to_cluster': True,
            'clustered': True}
        cluster.state.initialized_nodes['node-3'] = {
            'address': '10.0.0.13'}
        ready_nodes = cluster.ready_nodes

        self.assertEqual(
            ready_nodes,
            {
                'node-1': {
                    'address': '10.0.0.11',
                    'ready_to_cluster': True
                },
                'node-2': {
                    'address': '10.0.0.12',
                    'ready_to_cluster': True,
                    'clustered': True
                }
            })

    def test_master_cert(self):
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        rel_id = self.harness.add_relation('cluster', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/1')
        self.harness.update_relation_data(
            rel_id,
            'mssql',
            {
                'master_key_password': 'test-key-pass',
                'master_cert': 'test-cert',
                'master_cert_key': 'test-cert-key',
                'master_cert_key_password': 'test-cert-key-pass',
            })
        master_cert = cluster.master_cert

        self.assertEqual(
            master_cert,
            {
                'master_key_password': 'test-key-pass',
                'master_cert': 'test-cert',
                'master_cert_key': 'test-cert-key',
                'master_cert_key_password': 'test-cert-key-pass',
            })

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_ag_primary_replica_ag_configured(
            self, _is_ag_ready, _mssql_db_client):

        _is_ag_ready.return_value = True
        _mssql_db_client.return_value.get_ag_primary_replica.return_value = \
            self.TEST_PRIMARY_REPLICA_NAME
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.ag_configured = True
        primary_replica = cluster.ag_primary_replica

        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.get_ag_primary_replica.assert_called_once_with(
            cluster.AG_NAME)
        self.assertEqual(primary_replica, self.TEST_PRIMARY_REPLICA_NAME)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_ag_primary_replica_no_clustered_nodes(self, _is_ag_ready,
                                                   _mssql_db_client):
        _is_ag_ready.return_value = True
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.ag_configured = False
        primary_replica = cluster.ag_primary_replica

        _mssql_db_client.assert_not_called()
        self.assertIsNone(primary_replica)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'clustered_nodes',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_ag_primary_replica_other_clustered_node(
            self, _is_ag_ready, _clustered_nodes, _mssql_db_client):

        _is_ag_ready.return_value = True
        _mssql_db_client.return_value.get_ag_primary_replica.return_value = \
            self.TEST_PRIMARY_REPLICA_NAME
        _clustered_nodes.return_value = {
            'node-1': {
                'address': '10.0.0.11',
                'clustered': True
            },
            'node-2': {
                'address': '10.0.0.12',
                'clustered': True
            }
        }
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.ag_configured = False
        primary_replica = cluster.ag_primary_replica

        _mssql_db_client.assert_called_once_with('node-2')
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.get_ag_primary_replica.assert_called_once_with(
            cluster.AG_NAME)
        self.assertEqual(primary_replica, self.TEST_PRIMARY_REPLICA_NAME)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ag_primary_replica',
                       new_callable=mock.PropertyMock)
    def test_is_primary_replica_current_node(self, _ag_primary_replica):
        _ag_primary_replica.return_value = self.TEST_NODE_NAME
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')

        self.assertTrue(cluster.is_primary_replica)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ag_primary_replica',
                       new_callable=mock.PropertyMock)
    def test_is_primary_replica_other_node(self, _ag_primary_replica):
        _ag_primary_replica.return_value = 'mssql-primary-replica'
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')

        self.assertFalse(cluster.is_primary_replica)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'ag_primary_replica',
                       new_callable=mock.PropertyMock)
    def test_is_primary_replica_leader_node(self, _ag_primary_replica):
        _ag_primary_replica.return_value = None
        self.harness.set_leader()
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')

        self.assertTrue(cluster.is_primary_replica)

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_ag_replicas_ag_configured(self, _is_ag_ready, _mssql_db_client):
        _is_ag_ready.return_value = True
        _mssql_db_client.return_value.get_ag_replicas.return_value = \
            ['node-1', 'node-2', 'node-3']
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.ag_configured = True
        ag_replicas = cluster.ag_replicas

        _mssql_db_client.assert_called_once_with()
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.get_ag_replicas.assert_called_once_with(
            cluster.AG_NAME)
        self.assertListEqual(ag_replicas, ['node-1', 'node-2', 'node-3'])

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_ag_replicas_no_clustered_nodes(self,
                                            _is_ag_ready, _mssql_db_client):
        _is_ag_ready.return_value = True
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        ag_replicas = cluster.ag_replicas

        _mssql_db_client.assert_not_called()
        self.assertListEqual(ag_replicas, [])

    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'mssql_db_client')
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'clustered_nodes',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(interface_mssql_cluster.MssqlCluster,
                       'is_ag_ready',
                       new_callable=mock.PropertyMock)
    def test_ag_replicas_other_clustered_node(
            self, _is_ag_ready, _clustered_nodes, _mssql_db_client):

        _is_ag_ready.return_value = True
        _mssql_db_client.return_value.get_ag_replicas.return_value = \
            ['node-1', 'node-2']
        _clustered_nodes.return_value = {
            'node-1': {
                'address': '10.0.0.11',
                'clustered': True
            },
            'node-2': {
                'address': '10.0.0.12',
                'clustered': True
            }
        }
        self.harness.disable_hooks()
        self.harness.begin()
        cluster = interface_mssql_cluster.MssqlCluster(
            self.harness.charm, 'cluster')
        cluster.state.ag_configured = False
        ag_replicas = cluster.ag_replicas

        _mssql_db_client.assert_called_once_with('node-2')
        mock_ret_value = _mssql_db_client.return_value
        mock_ret_value.get_ag_replicas.assert_called_once_with(
            cluster.AG_NAME)
        self.assertListEqual(ag_replicas, ['node-1', 'node-2'])
