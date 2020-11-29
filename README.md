# Overview

This is an infrastructure Juju charm for Microsoft SQL server for Linux. It
uses the operator framework, and supports High Availability (HA) via the
hacluster subordinate charm.

# Deployment

The Microsoft SQL Server EULA must be explicitly accepted via the `accept-eula`
charm config before deploying the charm. We also need set the `vip` used by
the pacemaker cluster for HA:
```
juju deploy ./mssql.charm --num-units 3 \
    --config accept-eula=true \
    --config vip="<VIP_ADDRESS>"

juju deploy cs:hacluster mssql-hacluster \
    --config failure_timeout=60 \
    --config cluster_recheck_interval=120

juju add-relation mssql mssql-hacluster
```

## Scale-out

At any point in time, you can add more SQL Server instances via:
```
juju add-unit mssql -n 2
```

The existing replicated databases will be synchronized on the new nodes, once
they join the cluster.

# Microsoft SQL Server Utility

To communicate with the database, the `sqlcmd` utility comes handy.
Instructions to install it on Ubuntu are available [here](https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-ver15#ubuntu).

An example of command to connect to the database would be:
```
sqlcmd -S 10.13.114.200 -U "<DB_USER_NAME>" -P "<DB_USER_PASSWORD>"
```

To find out the `SA` password, use the `get-sa-password` Juju action:
```
juju run-action --wait mssql/leader get-sa-password
```
