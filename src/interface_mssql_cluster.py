"""
Implementation of the MSSQL charm cluster interface used with a peer relation.
"""

import logging
import secrets
import string
import math
import uuid

from base64 import b64encode, b64decode
from socket import gethostname as get_unit_hostname

from ops.framework import (
    EventBase,
    ObjectEvents,
    EventSource,
    Object,
    StoredState)
from ops.model import ActiveStatus
from charmhelpers.core import host

from mssql_db_client import MSSQLDatabaseClient
from utils import append_hosts_entry

logger = logging.getLogger(__name__)


class ReadySaEvent(EventBase):
    pass


class InitializedUnitEvent(EventBase):
    pass


class CreatedAvailabilityGroupEvent(EventBase):
    pass


class MssqlClusterEvents(ObjectEvents):
    ready_sa = EventSource(ReadySaEvent)
    initialized_unit = EventSource(InitializedUnitEvent)
    created_ag = EventSource(CreatedAvailabilityGroupEvent)


class MssqlCluster(Object):

    on = MssqlClusterEvents()
    state = StoredState()
    AG_NAME = 'juju-ag'
    UNIT_ACTIVE_STATUS = ActiveStatus('Unit is ready')

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.state.set_default(
            initialized_nodes={},
            master_cert_configured=False,
            ag_configured=False)
        self.relation_name = relation_name
        self.app = self.model.app
        self.unit = self.model.unit
        self.framework.observe(
            charm.on[relation_name].relation_joined,
            self.on_joined)
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)
        self.framework.observe(
            self.on.initialized_unit,
            self.on_initialized_unit)

    def on_joined(self, _):
        if self.node_name in self.state.initialized_nodes.keys():
            self.relation.data[self.unit]['node_name'] = self.node_name
            self.relation.data[self.unit]['node_address'] = self.bind_address
            for key in ['ready_to_cluster', 'clustered']:
                if self.state.initialized_nodes[self.node_name].get(key):
                    self.relation.data[self.unit][key] = 'true'

    def on_changed(self, event):
        rel_data = event.relation.data.get(event.unit)
        if rel_data:
            if rel_data.get('node_name') and rel_data.get('node_address'):
                self.add_to_initialized_nodes(
                    rel_data.get('node_name'),
                    rel_data.get('node_address'),
                    rel_data.get('ready_to_cluster'),
                    rel_data.get('clustered'))
        if not self.sa_password and self.unit.is_leader():
            self.set_sa_password()
        if self.sa_password:
            self.on.ready_sa.emit()
        if self.master_cert:
            self.configure_cluster_node()

    def on_initialized_unit(self, _):
        self.add_to_initialized_nodes(self.node_name, self.bind_address)
        self.relation.data[self.unit]['node_name'] = self.node_name
        self.relation.data[self.unit]['node_address'] = self.bind_address
        if not self.master_cert and self.unit.is_leader():
            self.set_master_cert()
        if self.master_cert:
            self.configure_cluster_node()

    def configure_master_cert(self):
        if self.node_name not in self.state.initialized_nodes.keys():
            logger.warning("Current unit is not initialized yet. Skipping "
                           "master cert setup.")
            return
        if self.state.master_cert_configured:
            logger.info("The master cert is already configured")
            return
        cert_info = self.master_cert
        db_client = self.mssql_db_client()
        db_client.create_master_encryption_key(
            cert_info['master_key_password'])
        db_client.setup_master_cert(
            b64decode(cert_info['master_cert'].encode()),
            b64decode(cert_info['master_cert_key'].encode()),
            cert_info['master_cert_key_password'])
        self.state.master_cert_configured = True

    def configure_cluster_node(self):
        self.configure_master_cert()
        self.mssql_db_client().setup_db_mirroring_endpoint()
        self.state.initialized_nodes[self.node_name]['ready_to_cluster'] = True
        self.relation.data[self.unit]['ready_to_cluster'] = 'true'
        if self.is_primary_replica:
            self.configure_primary_replica()
        else:
            self.configure_secondary_replica()

    def configure_primary_replica(self):
        if not self.is_ag_ready:
            self.create_ag()
            return
        ready_nodes = self.ready_nodes
        replicas = self.ag_replicas
        new_nodes = ready_nodes.keys() - replicas
        if len(new_nodes) == 0:
            return
        new_ready_nodes = {}
        for node in new_nodes:
            new_ready_nodes.update({
                node: ready_nodes[node]})
        self.mssql_db_client().add_replicas(self.AG_NAME, new_ready_nodes)
        self.set_unit_rel_nonce()

    def configure_secondary_replica(self):
        primary_replica = self.ag_primary_replica
        if not self.is_ag_ready or not primary_replica:
            return
        replicas = self.mssql_db_client(primary_replica).get_ag_replicas(
            self.AG_NAME)
        if self.node_name in replicas:
            self.join_existing_ag()
            self.sync_logins_from_primary_replica()

    def sync_logins_from_primary_replica(self):
        primary_db_client = self.mssql_db_client(self.ag_primary_replica)
        primary_logins = primary_db_client.get_sql_logins()
        this_db_client = self.mssql_db_client()
        this_logins = this_db_client.get_sql_logins()
        for login_name, login_info in primary_logins.items():
            if login_name in this_logins:
                continue
            logger.info(
                "Syncing login %s from the primary replica.", login_name)
            this_db_client.create_login(
                name=login_name,
                sid=login_info['sid'],
                password=login_info['password_hash'],
                is_hashed_password=True,
                server_roles=login_info['roles'])

    def create_ag(self):
        if self.state.ag_configured:
            logger.info("AG is already configured.")
            return
        ready_nodes = self.ready_nodes
        if len(ready_nodes) < 3:
            logger.warning(
                "We need at least 3 nodes ready to create the availability "
                "group. Current nodes ready: %s", len(ready_nodes))
            return
        self.mssql_db_client().create_ag(self.AG_NAME, ready_nodes)
        self.on.created_ag.emit()
        self.relation.data[self.unit]['clustered'] = 'true'
        self.set_app_rel_data({'ag_ready': 'true'})
        self.state.ag_configured = True
        self.set_unit_rel_nonce()
        self.set_unit_active_status()

    def join_existing_ag(self):
        if self.state.ag_configured:
            logger.info("AG is already configured.")
            return
        self.mssql_db_client().join_ag(self.AG_NAME)
        self.relation.data[self.unit]['clustered'] = 'true'
        self.state.ag_configured = True
        self.set_unit_active_status()

    def add_to_initialized_nodes(self, node_name, node_address,
                                 ready_to_cluster=None, clustered=None):
        self.state.initialized_nodes[node_name] = {'address': node_address}
        append_hosts_entry(node_address, [node_name])
        if ready_to_cluster:
            self.state.initialized_nodes[node_name]['ready_to_cluster'] = True
        if clustered:
            self.state.initialized_nodes[node_name]['clustered'] = True

    def set_master_cert(self):
        master_key_password = host.pwgen(32)
        master_cert_key_password = host.pwgen(32)
        db_client = self.mssql_db_client()
        db_client.create_master_encryption_key(master_key_password)
        cert, cert_key = db_client.create_master_cert(master_cert_key_password)
        self.set_app_rel_data({
            'master_key_password': master_key_password,
            'master_cert': b64encode(cert).decode(),
            'master_cert_key': b64encode(cert_key).decode(),
            'master_cert_key_password': master_cert_key_password,
        })
        self.state.master_cert_configured = True

    def set_sa_password(self, length=32):
        random_len = math.ceil(length/4)
        lower = ''.join(secrets.choice(string.ascii_lowercase)
                        for i in range(random_len))
        upper = ''.join(secrets.choice(string.ascii_uppercase)
                        for i in range(random_len))
        digits = ''.join(secrets.choice(string.digits)
                         for i in range(random_len))
        special = ''.join(secrets.choice(string.punctuation)
                          for i in range(random_len))
        sa_pass = lower + upper + digits + special
        self.set_app_rel_data({'sa_password': sa_pass})

    def set_unit_rel_nonce(self):
        self.relation.data[self.unit]['nonce'] = uuid.uuid4().hex

    def set_unit_active_status(self):
        logger.info("Unit is ready")
        self.unit.status = self.UNIT_ACTIVE_STATUS

    def set_app_rel_data(self, data={}, **kwargs):
        data.update(kwargs)
        rel = self.relation
        for key, value in data.items():
            rel.data[self.app][key] = value

    def get_app_rel_data(self, var_name):
        rel = self.relation
        if not rel:
            return None
        return rel.data[self.app].get(var_name)

    def mssql_db_client(self, db_host=None):
        mssql_host = db_host or self.bind_address
        return MSSQLDatabaseClient(
            host=mssql_host, user='SA', password=self.sa_password)

    @property
    def clustered_nodes(self):
        ready_nodes = {}
        for node_name, node_info in self.state.initialized_nodes.items():
            if node_info.get('clustered'):
                ready_nodes.update({node_name: node_info})
        return ready_nodes

    @property
    def ready_nodes(self):
        ready_nodes = {}
        for node_name, node_info in self.state.initialized_nodes.items():
            if node_info.get('ready_to_cluster'):
                ready_nodes.update({node_name: node_info})
        return ready_nodes

    @property
    def master_cert(self):
        master_key_password = self.get_app_rel_data('master_key_password')
        if not master_key_password:
            return None
        master_cert = self.get_app_rel_data('master_cert')
        if not master_cert:
            return None
        master_cert_key = self.get_app_rel_data('master_cert_key')
        if not master_cert_key:
            return None
        master_cert_key_password = self.get_app_rel_data(
            'master_cert_key_password')
        if not master_cert_key_password:
            return None
        return {
            'master_key_password': master_key_password,
            'master_cert': master_cert,
            'master_cert_key': master_cert_key,
            'master_cert_key_password': master_cert_key_password,
        }

    @property
    def is_ag_ready(self):
        return self.get_app_rel_data('ag_ready') == 'true'

    @property
    def ag_primary_replica(self):
        if not self.is_ag_ready:
            return None
        if self.state.ag_configured:
            return self.mssql_db_client().get_ag_primary_replica(self.AG_NAME)
        clustered_nodes = self.clustered_nodes
        if len(clustered_nodes) == 0:
            return None
        node = list(self.clustered_nodes.keys()).pop()
        return self.mssql_db_client(node).get_ag_primary_replica(self.AG_NAME)

    @property
    def is_primary_replica(self):
        primary_replica = self.ag_primary_replica
        if primary_replica:
            return self.node_name == self.ag_primary_replica
        return self.unit.is_leader()

    @property
    def ag_replicas(self):
        if not self.is_ag_ready:
            return []
        if self.state.ag_configured:
            return self.mssql_db_client().get_ag_replicas(self.AG_NAME)
        clustered_nodes = self.clustered_nodes
        if len(clustered_nodes) == 0:
            return []
        node = list(self.clustered_nodes.keys()).pop()
        return self.mssql_db_client(node).get_ag_replicas(self.AG_NAME)

    @property
    def node_name(self):
        return get_unit_hostname()

    @property
    def relation(self):
        return self.framework.model.get_relation(self.relation_name)

    @property
    def binding(self):
        return self.framework.model.get_binding(self.relation)

    @property
    def bind_address(self):
        return str(self.binding.network.bind_address)

    @property
    def sa_password(self):
        return self.get_app_rel_data('sa_password')
