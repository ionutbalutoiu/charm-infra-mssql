"""
Implementation of the MSSQL charm hacluster interface.
"""

import logging
import json
import os

from charmhelpers.core import host
from charmhelpers.fetch import apt_install
from charmhelpers.contrib.openstack.ha.utils import (
    VIP_GROUP_NAME,
    JSON_ENCODE_OPTIONS,
    update_hacluster_vip,
)
from ops.framework import Object, StoredState
from ops.model import ActiveStatus

from utils import retry_on_error

logger = logging.getLogger(__name__)


class HaCluster(Object):

    state = StoredState()
    PACEMAKER_LOGIN_NAME = 'MSSQLPacemaker'
    PACEMAKER_LOGIN_CREDS_FILE = '/var/opt/mssql/secrets/passwd'
    APT_PACKAGES = ['fence-agents', 'resource-agents', 'mssql-server-ha']
    UNIT_ACTIVE_STATUS = ActiveStatus('Unit is ready and clustered')

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.state.set_default(
            pacemaker_login_ready=False,
            ha_cluster_ready=False)
        self.relation_name = relation_name
        self.app = self.model.app
        self.unit = self.model.unit
        self.cluster = charm.cluster
        self.framework.observe(
            charm.on[relation_name].relation_joined,
            self.on_joined)
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)
        self.framework.observe(
            charm.cluster.on.created_ag,
            self.on_created_ag)

    def on_joined(self, event):
        if not self.cluster.is_ag_ready:
            logger.warning('The availability group is not ready. Defering '
                           'hacluster on_joined until AG is ready.')
            event.defer()
            return
        logger.info('Installing Microsoft SQL Server HA components')
        retry_on_error()(apt_install)(packages=self.APT_PACKAGES, fatal=True)
        self.setup_pacemaker_mssql_login()
        rel_data = {
            'resources': {
                'ag_cluster': 'ocf:mssql:ag'
            },
            'resource_params': {
                'ag_cluster':
                    'params ag_name="{ag_name}" '
                    'meta failure-timeout=60s '
                    'op start timeout=60s '
                    'op stop timeout=60s '
                    'op promote timeout=60s '
                    'op demote timeout=10s '
                    'op monitor timeout=60s interval=10s '
                    'op monitor timeout=60s interval=11s role="Master" '
                    'op monitor timeout=60s interval=12s role="Slave" '
                    'op notify timeout=60s'.format(
                        ag_name=self.cluster.AG_NAME)
            },
            'ms': {
                'ms-ag_cluster':
                    'ag_cluster meta '
                    'master-max="1" master-node-max="1" '
                    'clone-max="3" clone-node-max="1" notify="true"'
            }
        }
        update_hacluster_vip('mssql', rel_data)
        group_name = VIP_GROUP_NAME.format(service='mssql')
        rel_data.update({
            'colocations': {
                'vip_on_master':
                    'inf: {} ms-ag_cluster:Master'.format(group_name)
            },
            'orders': {
                'ag_first':
                    'inf: ms-ag_cluster:promote {}:start'.format(group_name)
            }
        })
        rel = self.model.get_relation(event.relation.name, event.relation.id)
        for k, v in rel_data.items():
            rel.data[self.unit]['json_{}'.format(k)] = json.dumps(
                v, **JSON_ENCODE_OPTIONS)

    def on_changed(self, event):
        rel_data = event.relation.data.get(event.unit)
        if rel_data.get('clustered'):
            logger.info('The hacluster relation is ready')
            self.unit.status = self.UNIT_ACTIVE_STATUS
            self.state.ha_cluster_ready = True

    def on_created_ag(self, _):
        self.setup_pacemaker_mssql_login()
        self.cluster.mssql_db_client().exec_t_sql("""
        GRANT ALTER, CONTROL, VIEW DEFINITION
            ON AVAILABILITY GROUP::[{ag_name}] TO [{login_name}]
        GRANT VIEW SERVER STATE TO [{login_name}]
        """.format(ag_name=self.cluster.AG_NAME,
                   login_name=self.PACEMAKER_LOGIN_NAME))

    def setup_pacemaker_mssql_login(self):
        if self.state.pacemaker_login_ready:
            logger.info('The pacemaker login is already configured.')
            return
        login_password = host.pwgen(32)
        self.cluster.mssql_db_client().create_login(
            name=self.PACEMAKER_LOGIN_NAME,
            password=login_password,
            server_roles=['sysadmin'])
        with open(self.PACEMAKER_LOGIN_CREDS_FILE, 'w') as f:
            f.write('{}\n{}\n'.format(self.PACEMAKER_LOGIN_NAME,
                                      login_password))
        os.chown(self.PACEMAKER_LOGIN_CREDS_FILE, 0, 0)
        os.chmod(self.PACEMAKER_LOGIN_CREDS_FILE, 0o400)
        self.state.pacemaker_login_ready = True

    @property
    def is_ha_cluster_ready(self):
        return self.state.ha_cluster_ready

    @property
    def bind_address(self):
        return self.model.config['vip']
