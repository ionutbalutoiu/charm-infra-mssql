#!/usr/bin/env python3

import logging
import subprocess
import re

from urllib.request import urlopen

from charmhelpers.fetch import add_source, apt_update, apt_install
from charmhelpers.core.host import lsb_release, service

from ops.framework import StoredState
from ops.charm import CharmBase
from ops.model import BlockedStatus, MaintenanceStatus
from ops.main import main

from interface_mssql_cluster import MssqlCluster
from interface_hacluster import HaCluster
from interface_mssql import MssqlDBProvides
from utils import retry_on_error

logger = logging.getLogger(__name__)


class MSSQLCharm(CharmBase):

    state = StoredState()
    UNIT_INITIALIZED_UNCLUSTERED_STATUS = MaintenanceStatus(
        'SQL Server is initialized. Waiting to get clustered.')
    SERVICE_NAME = 'mssql-server'
    MSSQL_PRODUCT_IDS = [
        'evaluation',
        'developer',
        'express',
        'web',
        'standard',
        'enterprise'
    ]
    GPG_KEY_URL = 'https://packages.microsoft.com/keys/microsoft.asc'
    APT_REPO_URL_MAP = {
        '2019': ('https://packages.microsoft.com/config/ubuntu/{}/'
                 'mssql-server-2019.list').format(
                     lsb_release()['DISTRIB_RELEASE'])
    }
    APT_PACKAGES = ['mssql-server']

    def __init__(self, *args):
        super().__init__(*args)
        self.state.set_default(initialized=False)
        self.cluster = MssqlCluster(self, 'cluster')
        self.ha = HaCluster(self, 'ha')
        self.db_provider = MssqlDBProvides(self, 'db')
        self.framework.observe(
            self.on.install,
            self.on_install)
        self.framework.observe(
            self.on.config_changed,
            self.initialize_mssql)
        self.framework.observe(
            self.cluster.on.ready_sa,
            self.initialize_mssql)
        self.framework.observe(
            self.on.get_sa_password_action,
            self.on_get_sa_password_action)

    @retry_on_error()
    def on_install(self, _):
        logger.info('Setting up Microsoft APT repository')
        gpg_key = urlopen(self.GPG_KEY_URL).read().decode()
        apt_repo = urlopen(self.APT_REPO_URL_MAP['2019']).read().decode()
        add_source(source=apt_repo, key=gpg_key, fail_invalid=True)
        logger.info('Installing Microsoft SQL Server')
        apt_update(fatal=True)
        apt_install(packages=self.APT_PACKAGES, fatal=True)

    def initialize_mssql(self, _):
        if self.state.initialized:
            logger.info('SQL Server is already initialized')
            return
        if not self._validate_config():
            logger.warning('Charm config is not valid')
            return
        if not self.cluster.sa_password:
            logger.warning('The SA password is not set yet')
            return
        logger.info('Initializing SQL Server')
        service('stop', self.SERVICE_NAME)
        subprocess.check_call(
            args=['/opt/mssql/bin/mssql-conf', '-n', 'setup'],
            env={'ACCEPT_EULA': self.accept_eula,
                 'MSSQL_PID': self.model.config['product-id'],
                 'MSSQL_SA_PASSWORD': self.cluster.sa_password,
                 'MSSQL_ENABLE_HADR': '1'})
        self.state.initialized = True
        self.cluster.on.initialized_unit.emit()
        self.unit.status = self.UNIT_INITIALIZED_UNCLUSTERED_STATUS

    def on_get_sa_password_action(self, event):
        event.set_results({'sa-password': self.cluster.sa_password})

    def _is_product_key(self, key):
        regex = re.compile(r"^([A-Z]|[0-9]){5}(-([A-Z]|[0-9]){5}){4}$")
        if regex.match(key.upper()):
            return True
        return False

    def _validate_product_id(self):
        product_id = self.model.config['product-id']
        if product_id.lower() in self.MSSQL_PRODUCT_IDS:
            return True
        logger.info("The product id is not a standard MSSQL product id. "
                    "Checking if it's a product key.")
        if not self._is_product_key(product_id):
            logger.warning("Product id %s is not a valid product key",
                           product_id)
            return False
        return True

    def _validate_config(self):
        """Validates the charm config

        :returns: boolean representing whether the config is valid or not.
        """
        logger.info('Validating charm config')
        config = self.model.config
        required = ['product-id']
        missing = []
        for name in required:
            if not config.get(name):
                missing.append(name)
        if missing:
            msg = 'Missing configuration: {}'.format(missing)
            logger.warning(msg)
            self.unit.status = BlockedStatus(msg)
            return False
        if not self.model.config['accept-eula']:
            msg = 'The MSSQL EULA is not accepted'
            logger.warning(msg)
            self.unit.status = BlockedStatus(msg)
            return False
        if not self._validate_product_id():
            msg = 'Invalid MSSQL product id'
            logger.warning(msg)
            self.unit.status = BlockedStatus(msg)
            return False
        return True

    @property
    def accept_eula(self):
        if self.model.config['accept-eula']:
            return 'Y'
        return 'N'


if __name__ == "__main__":
    main(MSSQLCharm)
