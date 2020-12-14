import unittest

from ops.testing import Harness
from ops.charm import CharmBase

from interface_mssql_requirer import MssqlDBRequirer


class TestInterfaceMssqlDBRequirer(unittest.TestCase):

    def setUp(self):
        self.harness = Harness(CharmBase, meta='''
            name: wordpress
            provides:
              db:
                interface: mssql
        ''')
        self.addCleanup(self.harness.cleanup)

    def test_on_joined_default_rel_vars(self):
        self.harness.begin()
        self.harness.charm.db = MssqlDBRequirer(self.harness.charm, 'db')
        rel_id = self.harness.add_relation('db', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/0')

        rel_data = self.harness.get_relation_data(
            rel_id, self.harness.charm.unit.name)
        self.assertEqual(rel_data.get('database'), self.harness.charm.app.name)
        self.assertEqual(rel_data.get('username'), self.harness.charm.app.name)

    def test_on_joined_config_rel_vars(self):
        self.harness.update_config({
            'database-name': 'test-db',
            'database-user-name': 'test-db-user'
        })
        self.harness.begin()
        self.harness.charm.db = MssqlDBRequirer(self.harness.charm, 'db')
        rel_id = self.harness.add_relation('db', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/0')

        rel_data = self.harness.get_relation_data(
            rel_id, self.harness.charm.unit.name)
        self.assertEqual(rel_data.get('database'), 'test-db')
        self.assertEqual(rel_data.get('username'), 'test-db-user')

    def test_on_changed(self):
        self.harness.begin()
        self.harness.charm.db = MssqlDBRequirer(self.harness.charm, 'db')
        rel_id = self.harness.add_relation('db', 'mssql')
        self.harness.add_relation_unit(rel_id, 'mssql/0')
        self.harness.update_relation_data(
            rel_id,
            self.harness.charm.unit.name,
            {
                'db_host': '10.0.0.100',
                'password': 'test-db-password',
            })

        rel_data = self.harness.get_relation_data(
            rel_id, self.harness.charm.unit.name)
        self.assertEqual(rel_data.get('db_host'), '10.0.0.100')
        self.assertEqual(rel_data.get('password'), 'test-db-password')


if __name__ == '__main__':
    unittest.main()
