"""
Utilities needed for the charm implementation
"""

import logging
import functools
import traceback
import time

from python_hosts import Hosts, HostsEntry

logger = logging.getLogger(__name__)


def _get_exception_details():
    return traceback.format_exc()


def retry_on_error(max_attempts=30, sleep_seconds=5, terminal_exceptions=[]):
    def _retry_on_error(func):
        @functools.wraps(func)
        def _exec_retry(*args, **kwargs):
            i = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except KeyboardInterrupt as ex:
                    logger.warning("Got a KeyboardInterrupt, skip retrying")
                    logger.error(ex)
                    raise
                except Exception as ex:
                    if any([isinstance(ex, tex)
                            for tex in terminal_exceptions]):
                        raise
                    i += 1
                    if i < max_attempts:
                        logger.warning(
                            "Exception occurred, retrying (%d/%d):\n%s",
                            i, max_attempts, _get_exception_details())
                        time.sleep(sleep_seconds)
                    else:
                        raise
        return _exec_retry
    return _retry_on_error


def append_hosts_entry(address, names):
    new_entry = HostsEntry(entry_type='ipv4', address=address, names=names)
    my_hosts = Hosts()
    my_hosts.add([new_entry])
    my_hosts.write()
