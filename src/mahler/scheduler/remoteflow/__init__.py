# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.remoteflow -- TODO
===================================

.. module:: remoteflow
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
import getpass
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


def build(max_workers, user, hosts, host_include, host_exclude, rc, submission_root, **kwargs):
    if host_include or host_exclude:
        filtered_hosts = dict()
        for host in hosts.keys():
            if host_include and host in host_include:
                filtered_hosts[host] = hosts[host]
            elif host_exclude and host not in host_exclude:
                filtered_hosts[host] = hosts[host]
        hosts = filtered_hosts

    return RemoteFlowResources(max_workers=max_workers, user=user, hosts=hosts,
                               rc=rc, submission_root=submission_root)


def build_init_parser(parser):
    """Return the parser that needs to be used for this command"""
    init_parser = parser.add_parser('remoteflow', help='remoteflow help')
    build_parser(init_parser)


def build_info_parser(parser):
    """Return the parser that needs to be used for this command"""
    info_parser = parser.add_parser('remoteflow', help='remoteflow help')

    info_parser.add_argument(
        '--user', type=str, default=None,
        help='username on the host. For security reason, the password cannot be passed in '
             'commandline. Authentication is only supported with key pairs.')

    info_parser.add_argument(
        '--host-include', type=str, nargs='*', default=[],
        help='hostnames to select from the config. The others are ignored')

    info_parser.add_argument(
        '--host-exclude', type=str, nargs='*', default=[],
        help='hostnames to exclude from the config.')


def build_run_parser(parser):
    """Return the parser that needs to be used for this command"""
    run_parser = parser.add_parser('remoteflow', help='remoteflow help')

    run_parser.add_argument(
        '--user', type=str, default=None,
        help='username on the host. For security reason, the password cannot be passed in '
             'commandline. Authentication is only supported with key pairs.')

    run_parser.add_argument(
        '--host-include', type=str, nargs='*', default=[],
        help='hostnames to select from the config. The others are ignored')

    run_parser.add_argument(
        '--host-exclude', type=str, nargs='*', default=[],
        help='hostnames to exclude from the config.')


    return run_parser


def build_submit_parser(parser):
    """Return the parser that needs to be used for this command"""
    submit_parser = parser.add_parser('remoteflow', help='remoteflow help')
    build_parser(submit_parser)

    return submit_parser


def build_parser(parser):
    """Generic arguments for all parsers"""

    parser.add_argument(
        '--user', type=str, default=None,
        help='username on the host. For security reason, the password cannot be passed in '
             'commandline. Authentication is only supported with key pairs.')

    parser.add_argument(
        '--host-include', type=str, nargs='*', default=[],
        help='hostnames to select from the config. The others are ignored')

    parser.add_argument(
        '--host-exclude', type=str, nargs='*', default=[],
        help='hostnames to exclude from the config.')

    parser.add_argument(
        '--submission-root', type=str, default=None,
        help='Submission root on hosts to save sbatch scripts. Use config file to define '
             'submission roots specific to each host.')

    parser.add_argument(
        '--max-workers', type=int, default=None,
        help='Number of concurrent workers to submit on each host (not total over all host). '
             'Use config file to define max_workers specific to each host.')

    parser.add_argument(
        '--force-num-tasks', type=int, default=None,
        help='Force submit more jobs. Usefull if first jobs are creating more.')

    parser.add_argument(
        '--rc', type=str, default=None,
        help='RC file used to setup all hosts. '
             'Use config file to define `rc` specific to each host.')


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
        'rc', type=str, default=None,
        env_var='MAHLER_SCHEDULER_REMOTEFLOW_RC')

    config.add_option(
        'max_workers', type=int, default=1, env_var='MAHLER_SCHEDULER_REMOTEFLOW_MAX_WORKERS')

    return config


def parse_config_files(config):
    mahler.core.utils.config.parse_config_files(
        config, mahler.core.DEF_CONFIG_FILES_PATHS,
        base='scheduler.remoteflow')

    mahler.core.utils.config.parse_config_files(
        config, DEF_CONFIG_FILES_PATHS)
