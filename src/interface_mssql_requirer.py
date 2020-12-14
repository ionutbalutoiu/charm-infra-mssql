"""
Implementation of the MSSQL charm database requirer interface.
"""

import logging

from ops.framework import (
    EventBase,
    ObjectEvents,
    EventSource,
    Object,
    StoredState)

logger = logging.getLogger(__name__)


class ReadyDBEvent(EventBase):
    pass


class MssqlDBRequirerEvents(ObjectEvents):
    ready_db = EventSource(ReadyDBEvent)


class MssqlDBRequirer(Object):

    on = MssqlDBRequirerEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.state.set_default(
            database_host=None,
            database_user_password=None)
        self.app = self.model.app
        self.unit = self.model.unit
        self.framework.observe(
            charm.on[relation_name].relation_joined,
            self.on_joined)
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)

    def on_joined(self, event):
        database_name = self.model.config.get('database-name')
        if not database_name:
            database_name = self.model.app.name
        database_user_name = self.model.config.get('database-user-name')
        if not database_user_name:
            database_user_name = self.model.app.name
        rel = self.model.get_relation(event.relation.name, event.relation.id)
        rel.data[self.unit]['database'] = database_name
        rel.data[self.unit]['username'] = database_user_name

    def on_changed(self, event):
        rel_data = event.relation.data.get(event.unit)
        if not rel_data:
            return
        self.state.database_host = rel_data.get('db_host')
        self.state.database_user_password = rel_data.get('password')
        if self.state.database_host and self.state.database_user_password:
            self.on.ready_db.emit()
