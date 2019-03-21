# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.remoteflow -- TODO
===================================

.. module:: remoteflow
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
import os

import mahler.core
import mahler.core.utils.config

from ._version import get_versions
from .resources import RemoteFlowResources

VERSIONS = get_versions()
del get_versions

__descr__ = 'TODO'
__version__ = VERSIONS['version']
__license__ = 'GNU GPLv3'
__author__ = u'Xavier Bouthillier'
__author_short__ = u'Xavier Bouthillier'
__author_email__ = 'xavier.bouthillier@umontreal.ca'
__copyright__ = u'2018, Xavier Bouthillier'
__url__ = 'https://github.com/bouthilx/mahler.scheduler.remoteflow'

DEF_CONFIG_FILES_PATHS = [
    os.path.join(mahler.core.DIRS.site_data_dir, 'scheduler', 'remoteflow', 'config.yaml.example'),
    os.path.join(mahler.core.DIRS.site_config_dir, 'scheduler', 'remoteflow', 'config.yaml'),
    os.path.join(mahler.core.DIRS.user_config_dir, 'scheduler', 'remoteflow', 'config.yaml')
    ]


def build(max_workers, user, hosts, submission_root, init_only, **kwargs):
    return RemoteFlowResources(max_workers=max_workers, user=user, hosts=hosts,
                               submission_root=submission_root, init_only=init_only)


def build_parser(parser):
    """Return the parser that needs to be used for this command"""
    remoteflow_parser = parser.add_parser('remoteflow', help='remoteflow help')

    remoteflow_parser.add_argument(
        '--user', type=str, default=None,
        help='username on the host. For security reason, the password cannot be passed in '
             'commandline. Authentication is only supported with key pairs.')

    remoteflow_parser.add_argument(
        '--hosts', type=str, nargs='*', default=None,
        help='hostnames where to deploy tasks. Can only set host names using the commandline '
             'arguments. Use config file to define host configs.')

    remoteflow_parser.add_argument(
        '--submission-root', type=str, default=None,
        help='Submission root on hosts to save sbatch scripts. Use config file to define '
             'submission roots specific to each host.')

    remoteflow_parser.add_argument(
        '--max-workers', type=int, default=10,
        help='Number of concurrent workers to submit on each host (not total over all host). '
             'Use config file to define max_workers specific to each host.')

    # TODO: Turn into a generic command in mahler.cli.scheduler.
    remoteflow_parser.add_argument(
        '--init-only', action='store_true',
        help='Setup clusters without launching any jobs.')


def define_config():
    config = mahler.core.utils.config.Configuration()

    config.add_option(
        'user', type=str, default=getpass.getuser(),
        env_var='MAHLER_SCHEDULER_REMOTEFLOW_USER')

    config.add_option(
        'hosts', type=dict, default={}, env_var=None)

    config.add_option(
        'submission_root', type=str, default=None,
        env_var='MAHLER_SCHEDULER_REMOTEFLOW_SUBMISSION_ROOT')

    config.add_option(
        'prolog', type=str, default='',
        env_var='MAHLER_SCHEDULER_REMOTEFLOW_PROLOG')

    config.add_option(
        'max_workers', type=int, default=1, env_var='MAHLER_SCHEDULER_REMOTEFLOW_MAX_WORKERS')

    return config


def parse_config_files(config):
    mahler.core.utils.config.parse_config_files(
        config, mahler.core.DEF_CONFIG_FILES_PATHS,
        base='scheduler.remoteflow')

    mahler.core.utils.config.parse_config_files(
        config, DEF_CONFIG_FILES_PATHS)
