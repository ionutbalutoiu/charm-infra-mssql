"""
Implementation of the MSSQL charm database provider interface.
"""

import logging

from ops.framework import Object
from charmhelpers.core import host

logger = logging.getLogger(__name__)


class MssqlDBProvider(Object):

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.db_rel_name = relation_name
        self.app = self.model.app
        self.unit = self.model.unit
        self.cluster = charm.cluster
        self.ha = charm.ha
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)
        self.framework.observe(
            charm.on[relation_name].relation_departed,
            self.on_departed)

    def on_changed(self, event):
        if not self.cluster.is_ag_ready or not self.ha.is_ha_cluster_ready:
            logger.warning('Defering DB on_changed() until the AG and '
                           'the HA cluster are ready.')
            event.defer()
            return
        if not self.cluster.is_primary_replica:
            logger.warning('Unit is not the SQL Server primary replica. '
                           'Skipping DB on_changed().')
            return
        rel_data = self.db_rel_data(event)
        if not rel_data:
            logging.info("The db relation data is not available yet.")
            return

        logging.info("Handling db request.")
        db_user_password = host.pwgen(32)
        db_client = self.cluster.mssql_db_client()
        db_client.create_database(db_name=rel_data['database'],
                                  ag_name=self.cluster.AG_NAME)
        db_client.create_login(name=rel_data['username'],
                               password=db_user_password)
        db_client.grant_access(db_name=rel_data['database'],
                               db_user_name=rel_data['username'])
        # Notify the secondary replicas, so they can sync the new SQL logins
        # from the primary replica.
        self.cluster.set_unit_rel_nonce()

        rel = self.model.get_relation(
            event.relation.name,
            event.relation.id)
        # advertise on app
        rel.data[self.app]['db_host'] = self.ha.bind_address
        rel.data[self.app]['password'] = db_user_password
        # advertise on unit
        rel.data[self.unit]['db_host'] = self.ha.bind_address
        rel.data[self.unit]['password'] = db_user_password

    def on_departed(self, event):
        rel_data = self.db_rel_data(event)
        if not rel_data:
            logger.info('No relation data. Skipping DB on_departed().')
            return
        db_client = self.cluster.mssql_db_client()
        db_client.remove_login(rel_data['username'])
        if self.cluster.is_ag_ready and self.cluster.is_primary_replica:
            db_client.revoke_access(db_name=rel_data['database'],
                                    db_user_name=rel_data['username'])

    def db_rel_data(self, event):
        rel_data = event.relation.data.get(event.unit)
        if not rel_data:
            return {}
        database = rel_data.get('database')
        username = rel_data.get('username')
        if not database or not username:
            return {}
        return {
            'database': database,
            'username': username,
        }
