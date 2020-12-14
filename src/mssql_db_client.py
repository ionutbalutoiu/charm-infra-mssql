"""
DB client helpers for the MSSQL charm.
"""

import logging
import pwd
import grp
import os
import time

from charmhelpers.fetch.python.packages import pip_install

from utils import retry_on_error

try:
    from pymssql import connect  # NOQA:F401
except ImportError:
    # We install 'pymssql' package from the deployment machine instead
    # of using the charm 'requirements.txt', because the package is installed
    # with a pre-compiled cpython library, which is pre-compiled for every
    # supported Python version.
    # So, when we build the charm via 'charmcraft build', the built charm
    # will contain the cpython library corresponding to the Python version
    # used to build the charm. The Python version from the deployment machine
    # might not be the same, and the charm will fail to import 'pymssql'.
    # For example: we build Ubuntu Focal with Python 3.8, and we deploy on
    # Ubuntu Bionic with Python 3.6.
    retry_on_error()(pip_install)(package='pymssql', fatal=True)
    from pymssql import connect  # NOQA:F401

logger = logging.getLogger(__name__)


class MSSQLDatabaseClient(object):

    MSSQL_DATA_DIR = '/var/opt/mssql/data'
    MSSQL_USER = 'mssql'
    MSSQL_GROUP = 'mssql'

    def __init__(self, user, password, host="localhost", port=1433):
        self._user = user
        self._password = password
        self._host = host
        self._port = port

    def _connection(self, timeout=300):
        sleep_time = 5
        start = time.time()
        while True:
            elapsed = time.time() - start
            if elapsed > timeout:
                raise Exception(
                    "Couldn't connect to SQL Server %s:%s within %.2f "
                    "minutes" % (self._host, self._port, timeout / 60))
            try:
                _conn = connect(
                    server=self._host, port=self._port,
                    user=self._user, password=self._password)
                _conn.autocommit(True)
                return _conn
            except Exception:
                time.sleep(sleep_time)

    def exec_t_sql(self, t_sql):
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute(t_sql)
        conn.close()

    def create_database(self, db_name, ag_name=None):
        logger.info("Creating database %s.", db_name)
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}')
        BEGIN
            CREATE DATABASE [{db_name}]
        END
        """.format(db_name=db_name))
        logger.info("Created the database.")
        if ag_name:
            logger.info("Adding database %s to AG %s.", db_name, ag_name)
            cursor.execute("""
            ALTER DATABASE [{db_name}] SET RECOVERY FULL
            BACKUP DATABASE [{db_name}] TO DISK = N'{data_dir}/{db_name}.bak'
            IF NOT EXISTS(
                SELECT db.name FROM
                    sys.dm_hadr_database_replica_states rs
                    JOIN
                    sys.databases db
                    ON rs.database_id = db.database_id
                WHERE db.name = '{db_name}')
            BEGIN
                ALTER AVAILABILITY GROUP [{ag_name}] ADD DATABASE [{db_name}]
            END
            """.format(ag_name=ag_name,
                       db_name=db_name,
                       data_dir=self.MSSQL_DATA_DIR))
            logger.info("Database added to AG.")
        conn.close()

    def create_login(self, name, password, is_hashed_password=False,
                     sid=None, server_roles=[]):
        logger.info("Creating SQL login %s.", name)
        conn = self._connection()
        cursor = conn.cursor()
        login_params = []
        if is_hashed_password:
            login_params.append("PASSWORD = 0x{0} HASHED".format(password))
        else:
            login_params.append("PASSWORD = '{0}'".format(password))
        cursor.execute("""
        SELECT * FROM sys.syslogins WHERE name = '{0}'
        """.format(name))
        if cursor.fetchone():
            operation = "ALTER"
        else:
            operation = "CREATE"
            if sid:
                login_params.append("SID = 0x{0}".format(sid))
        login_params += ["CHECK_POLICY = OFF", "CHECK_EXPIRATION = OFF"]
        cursor.execute("""
        {operation} LOGIN [{login_name}] WITH {login_params}
        """.format(operation=operation,
                   login_name=name,
                   login_params=", ".join(login_params)))
        for role in server_roles:
            cursor.execute("""
            ALTER SERVER ROLE [{role}] ADD MEMBER [{login_name}]
            """.format(role=role, login_name=name))
        conn.close()
        logger.info("Created the SQL login.")

    def remove_login(self, name):
        logger.info("Removing SQL login %s, if it exists.", name)
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        IF EXISTS (SELECT * FROM sys.syslogins WHERE name = '{0}')
        BEGIN
            DROP LOGIN [{0}]
        END
        """.format(name))
        conn.close()
        logger.info("SQL login removed.")

    def grant_access(self, db_name, db_user_name, login_name=None):
        if not login_name:
            login_name = db_user_name
        logger.info("Granting access for user %s to database %s.",
                    db_user_name, db_name)
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        USE [{db_name}]
        IF NOT EXISTS(SELECT * FROM sys.sysusers WHERE name = '{db_user_name}')
        BEGIN
            CREATE USER [{db_user_name}] FOR LOGIN [{login_name}]
        END
        ALTER ROLE db_owner ADD MEMBER [{db_user_name}]
        """.format(db_name=db_name,
                   db_user_name=db_user_name,
                   login_name=login_name))
        conn.close()
        logger.info("Database access granted.")

    def revoke_access(self, db_name, db_user_name):
        logger.info("Revoking access for user %s to database %s.",
                    db_user_name, db_name)
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        USE [{db_name}]
        DROP USER IF EXISTS [{db_user_name}]
        """.format(db_name=db_name, db_user_name=db_user_name))
        conn.close()
        logger.info("Database access revoked.")

    def create_master_encryption_key(self, master_key_password):
        logger.info("Creating the master encryption key.")
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        USE [master]
        IF NOT EXISTS(SELECT * FROM sys.symmetric_keys
                      WHERE name = '##MS_DatabaseMasterKey##')
        BEGIN
            CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{0}'
        END
        ELSE
        BEGIN
            ALTER MASTER KEY REGENERATE WITH ENCRYPTION BY PASSWORD = '{0}'
        END
        """.format(master_key_password))
        conn.close()
        logger.info("Master encryption key created.")

    def create_master_cert(self, master_cert_key_password):
        logger.info("Creating the master certificate.")
        cert_file = os.path.join(
            self.MSSQL_DATA_DIR, 'dbm_certificate.cer')
        cert_key_file = os.path.join(
            self.MSSQL_DATA_DIR, 'dbm_certificate.pvk')
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        USE [master]
        IF NOT EXISTS(SELECT * FROM sys.certificates
                      WHERE name = 'dbm_certificate')
        BEGIN
            CREATE CERTIFICATE dbm_certificate WITH SUBJECT = 'dbm'
        END
        BACKUP CERTIFICATE dbm_certificate
            TO FILE = '{cert_file}'
            WITH PRIVATE KEY (
                FILE = '{cert_key_file}',
                ENCRYPTION BY PASSWORD = '{master_cert_key_password}'
            )
        """.format(cert_file=cert_file,
                   cert_key_file=cert_key_file,
                   master_cert_key_password=master_cert_key_password))
        conn.close()
        with open(cert_file, 'rb') as f:
            cert = f.read()
        with open(cert_key_file, 'rb') as f:
            cert_key = f.read()
        logger.info("Created the master certificate.")
        return cert, cert_key

    def setup_master_cert(self, master_cert,
                          master_cert_key, master_cert_key_password):
        logger.info("Setting up existing master certificate.")
        uid = pwd.getpwnam(self.MSSQL_USER).pw_uid
        gid = grp.getgrnam(self.MSSQL_GROUP).gr_gid

        cert_file = os.path.join(
            self.MSSQL_DATA_DIR, 'dbm_certificate.cer')
        with open(cert_file, 'wb') as f:
            f.write(master_cert)
        os.chown(cert_file, uid, gid)

        cert_key_file = os.path.join(
            self.MSSQL_DATA_DIR, 'dbm_certificate.pvk')
        with open(cert_key_file, 'wb') as f:
            f.write(master_cert_key)
        os.chown(cert_key_file, uid, gid)

        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        USE [master]
        IF NOT EXISTS(SELECT * FROM sys.certificates
                      WHERE name = 'dbm_certificate')
        BEGIN
            CREATE CERTIFICATE dbm_certificate
                FROM FILE = '{cert_file}'
                WITH PRIVATE KEY (
                    FILE = '{cert_key_file}',
                    DECRYPTION BY PASSWORD = '{master_cert_key_password}'
                )
        END
        """.format(cert_file=cert_file,
                   cert_key_file=cert_key_file,
                   master_cert_key_password=master_cert_key_password))
        conn.close()
        logger.info("Restored the master certificate.")

    def setup_db_mirroring_endpoint(self):
        logger.info("Creating the DB mirroring endpoint")
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS(SELECT * FROM sys.endpoints WHERE name = 'Hadr_endpoint')
        BEGIN
            CREATE ENDPOINT [Hadr_endpoint]
                AS TCP (LISTENER_PORT = 5022)
                FOR DATABASE_MIRRORING (
                    ROLE = ALL,
                    AUTHENTICATION = CERTIFICATE dbm_certificate,
                    ENCRYPTION = REQUIRED ALGORITHM AES
                    )
        END
        ALTER ENDPOINT [Hadr_endpoint] STATE = STARTED
        """)
        conn.close()
        logger.info("Created the DB mirroring endpoint")

    def create_ag(self, ag_name, ready_nodes):
        logger.info("Creating the availability group %s.", ag_name)
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT * FROM sys.availability_groups WHERE name = '{0}'
        """.format(ag_name))
        if cursor.fetchone():
            logger.info("Availability group already exist.")
            conn.close()
            return
        t_sql_replica_nodes = []
        for node_name, node_info in ready_nodes.items():
            t_sql_replica_nodes.append("""
                N'{node_name}'
                WITH (
                    ENDPOINT_URL = N'tcp://{node_address}:5022',
                    AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
                    FAILOVER_MODE = EXTERNAL,
                    SEEDING_MODE = AUTOMATIC
                    )""".format(node_name=node_name,
                                node_address=node_info['address']))
        cursor.execute("""
        CREATE AVAILABILITY GROUP [{ag_name}]
            WITH (DB_FAILOVER = ON, CLUSTER_TYPE = EXTERNAL)
            FOR REPLICA ON {replica_nodes}
        ALTER AVAILABILITY GROUP [{ag_name}] GRANT CREATE ANY DATABASE
        """.format(ag_name=ag_name,
                   replica_nodes=",".join(t_sql_replica_nodes)))
        conn.close()
        logger.info("Created availability group.")

    def add_replicas(self, ag_name, ready_nodes):
        conn = self._connection()
        cursor = conn.cursor()
        for node_name, node_info in ready_nodes.items():
            logger.info("Adding node %s as SQL Server replica.", node_name)
            cursor.execute("""
            SELECT * FROM sys.dm_hadr_availability_replica_cluster_nodes
            WHERE group_name = '{ag_name}' and node_name = '{node_name}'
            """.format(ag_name=ag_name,
                       node_name=node_name))
            if cursor.fetchone():
                logger.info("Node is already a SQL Server replica.")
                continue
            cursor.execute("""
            ALTER AVAILABILITY GROUP [{ag_name}] ADD REPLICA ON '{node_name}'
                WITH (
                    ENDPOINT_URL = 'TCP://{node_address}:5022',
                    AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
                    FAILOVER_MODE = EXTERNAL,
                    SEEDING_MODE = AUTOMATIC
                    )""".format(ag_name=ag_name,
                                node_name=node_name,
                                node_address=node_info['address']))
        conn.close()
        logger.info("Replicas added.")

    def join_ag(self, ag_name):
        logger.info("Joining availability group %s.", ag_name)
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS(SELECT * FROM sys.availability_groups WHERE name = '{0}')
        BEGIN
            ALTER AVAILABILITY GROUP [{0}] JOIN WITH (CLUSTER_TYPE = EXTERNAL)
        END
        ALTER AVAILABILITY GROUP [{0}] GRANT CREATE ANY DATABASE
        """.format(ag_name))
        conn.close()
        logger.info("Availability group joined.")

    def get_ag_primary_replica(self, ag_name):
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT primary_replica FROM
            sys.dm_hadr_availability_group_states States
            INNER JOIN
            sys.availability_groups Groups
            ON States.group_id = Groups.group_id
        WHERE Groups.Name = '{0}'
        """.format(ag_name))
        row = cursor.fetchone()
        conn.close()
        return row[0]

    def get_ag_replicas(self, ag_name):
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT replica_server_name FROM
            sys.availability_replicas Replicas
            INNER JOIN
            sys.availability_groups Groups
            ON Replicas.group_id = Groups.group_id
        WHERE Groups.Name = '{0}'
        """.format(ag_name))
        replicas = []
        for row in cursor:
            replicas.append(row[0])
        conn.close()
        return replicas

    def get_sql_login_roles(self, login_name):
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT r.name FROM
        sys.server_role_members rm
        INNER JOIN
        sys.server_principals r ON (r.principal_id = rm.role_principal_id AND
                                    r.type = 'R')
        INNER JOIN
        sys.server_principals m ON m.principal_id = rm.member_principal_id
        WHERE m.name = '{login_name}'
        """.format(login_name=login_name))
        roles = []
        for row in cursor:
            roles.append(row[0])
        conn.close()
        return roles

    def get_sql_logins(self):
        conn = self._connection()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT name, sid, password_hash FROM sys.sql_logins
        """)
        sql_logins = {}
        for row in cursor:
            sql_logins.update({
                row[0]: {
                    'sid': row[1].hex(),
                    'password_hash': row[2].hex(),
                    'roles': self.get_sql_login_roles(login_name=row[0])
                }
            })
        conn.close()
        return sql_logins
