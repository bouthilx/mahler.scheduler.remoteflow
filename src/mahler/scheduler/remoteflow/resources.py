# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.remoteflow.resources -- TODO
===================================

.. module:: resources
    :platform: Unix
    :synopsis: TODO

TODO: Write long description

"""
import logging
import os
import subprocess
import warnings

import invoke.exceptions

import paramiko.ssh_exception

import patchwork.files
import patchwork.transfers
import patchwork.environment

from fabric import ThreadingGroup as Group

from mahler.core.resources import Resources

# Until this is fixed: https://github.com/paramiko/paramiko/issues/1386
warnings.filterwarnings(action='ignore', module='.*paramiko.*')

logger = logging.getLogger(__name__)


FLOW_OPTIONS_TEMPLATE = "{array}time=2:59:00;job-name={job_name}"

FLOW_TEMPLATE = "flow-submit {container} --root {root_dir} --prolog '{prolog}' --options '{options}'"

COMMAND_TEMPLATE = "mahler execute{container}{tags}{options}"

SUBMIT_COMMANDLINE_TEMPLATE = "{flow} launch {command}"


config = """
# Singularity specific
export SINGULARITY_DIR=$SCRATCH/singularity
export SREGISTRY_CACHEDIR=$SINGULARITY_DIR
export SINGULARITY_CACHEDIR=$SINGULARITY_DIR/cache
export SREGISTRY_DATABASE=$SINGULARITY_DIR

# Flow specific
export CONTAINER_DATA=$SCRATCH/data
export CERTIFICATE_FOLDER=$HOME/certs
export CONTAINER_HOME=$SCRATCH/home
export CONTAINER_CONFIG=$HOME/.config

export FLOW_SUBMISSION_DIR=$SCRATCH/submit

export MAHLER_REGISTRY_MONGODB_PORT=$(python -c 'from socket import socket; s = socket(); s.bind(("", 0)); print(s.getsockname()[1])')
"""


IGNORE_RESOURCES = ['usage']


class RemoteFlowResources(Resources):
    """
    """

    def __init__(self, user, hosts, max_workers=100, submission_root=None, prolog="",
                 rc=None):
        """
        """
        if not isinstance(hosts, dict):
            hosts = {host: {} for host in hosts}

        self.user = user
        self.max_workers = max_workers
        self.prolog = prolog
        self.hosts = hosts
        self.submission_root = submission_root
        self.rc = rc
        self.connections = Group(*list(sorted(hosts.keys())), user=user)
        for connection in self.connections:
            connection.connect_timeout = 360

    def init(self):

        for host_name, host_connection in zip(sorted(self.hosts.keys()), self.connections):
            print('Adding source remoteflowrc in {}:bashrc'.format(host_name))
            try:
                patchwork.files.append(host_connection, '.bashrc', 'source $HOME/.remoteflowrc')
            except paramiko.ssh_exception.AuthenticationException as e:
                print(e)
                continue

            rc_file_path = self.hosts.get('rc', self.rc)

            if rc_file_path is None:
                raise ValueError(
                    "`rc` is not defined for host {} nor globally.".format(host_name))

            try:
                out = host_connection.run('cat .remoteflowrc', hide=True)
                # [:-2] is a dirty hack to drop the line `export CLUSTERNAME={}`
                if '\n'.join(out.stdout.split('\n')[:-2]) != open(rc_file_path, 'r').read():
                    update_rc = True
            except invoke.exceptions.UnexpectedExit:
                update_rc = True

            if update_rc:
                print('Updating {}:.remoteflowrc'.format(host_name))
                host_connection.put(rc_file_path)
                patchwork.files.append(
                    host_connection, '.remoteflowrc', 'export CLUSTERNAME={}'.format(host_name))

            print('Updating {}:.config/mahler'.format(host_name))
            patchwork.transfers.rsync(
                    host_connection, '{}/.remoteconfig/mahler'.format(os.environ['HOME']), '.config')

            # if not patchwork.environment.have_program(host_connection, 'flow-submit'):
            print('Installing flow on {}'.format(host_name))
            host_connection.run(
                'pip install --upgrade --user git+https://github.com/bouthilx/flow.git', hide=True)

            # if not patchwork.environment.have_program(host_connection, 'sregistry'):
            print('Installing sregistry on {}'.format(host_name))
            host_connection.run('pip install --upgrade --user sregistry[all]', hide=True)

    def available(self, squash=True):
        """
        """
        command = 'squeue -r -o %t -u {user}'.format(user=self.user)
        jobs = {}
        max_workers = 0
        result = self.connections.run(command, hide=True, warn=True)
        for host_name in sorted(self.hosts.keys()):
            logger.debug('squeue on {}'.format(host_name))
            max_workers += self.hosts[host_name].get('max_workers', self.max_workers)
            out = result[self.connections[list(sorted(self.hosts.keys())).index(host_name)]]
            out = out.stdout
            states = dict()
            for line in out.split("\n")[1:]:  # ignore `ST` header
                line = line.strip()
                if not line:
                    continue

                if line not in states:
                    states[line] = 0

                states[line] += 1

            logger.debug('Nodes availability')
            for state, number in sorted(states.items()):
                logging.debug('{}: {}'.format(state, number))

            jobs[host_name] = sum(number for name, number in states.items() if name != 'CG')
            logging.debug('total: {}'.format(jobs[host_name]))

        if squash:
            return max(max_workers - sum(jobs.values()), 0)
        else:
            return {host_name: host_config.get('max_workers', self.max_workers) - jobs[host_name]
                    for host_name, host_config in self.hosts.items()}

    def run(self, commandline):
        commandline = " ".join(commandline)
        print('executing:\n' + commandline)
        result = self.connections.run(commandline, hide=True, warn=True)
        status = dict()
        for host_name in sorted(self.hosts.keys()):
            print("  {}  ".format(host_name))
            print('-' * (len(host_name) + 4))
            out = result[self.connections[list(sorted(self.hosts.keys())).index(host_name)]]
            print('\nstdout')
            print('------')
            print(out.stdout)
            print('\nstderr')
            print('------')
            print(out.stderr)

    def info(self):
        command = 'squeue -r -o %t -u {user}'.format(user=self.user)
        jobs = {}
        max_workers = 0
        result = self.connections.run(command, hide=True)

        status = dict()
        for host_name in self.hosts.keys():
            logger.debug('squeue on {}'.format(host_name))
            max_workers += self.hosts[host_name].get('max_workers', self.max_workers)
            out = result[self.connections[list(sorted(self.hosts.keys())).index(host_name)]]
            out = out.stdout
            states = dict()
            for line in out.split("\n")[1:]:  # ignore `ST` header
                line = line.strip()
                if not line:
                    continue

                if line not in states:
                    states[line] = 0

                states[line] += 1

            status[host_name] = states

            logger.debug('Nodes availability')
            for state, number in sorted(states.items()):
                logging.debug('{}: {}'.format(state, number))

            jobs[host_name] = sum(number for name, number in states.items() if name != 'CG')
            logging.debug('total: {}'.format(jobs[host_name]))

        nodes_available = {host_name: host_config.get('max_workers', self.max_workers) - jobs[host_name]
                           for host_name, host_config in self.hosts.items()}
        total_nodes_available = sum(nodes_available.values())
        lines = ['{} nodes available'.format(total_nodes_available)]
        for host_name, host_nodes_available in sorted(nodes_available.items()):
            lines.append('  {:<10}: {} nodes available'.format(host_name, max(host_nodes_available, 0)))

        lines += ['', "Status:"]
        for host_name in sorted(nodes_available.keys()):
            lines.append("  " + host_name)
            for state_name, state_number in sorted(status[host_name].items()):
                lines.append("    {}: {}".format(state_name, state_number))
            lines.append("")

        return '\n'.join(lines)

    def submit(self, tasks, container=None, tags=tuple(), working_dir=None, num_workers=None,
               force_num_tasks=None):
        """
        """
        nodes_available = self.available(squash=False)
        total_nodes_available = sum(nodes_available.values())
        print('{} nodes available'.format(total_nodes_available))
        for host_name, host_nodes_available in nodes_available.items():
            print('{:>20}: {} nodes available'.format(host_name, host_nodes_available))

        if not total_nodes_available:
            return

        print('Pulling container {} on logging node of hosts {}'.format(
            container, list(self.hosts.keys())))
        result = self.connections.run('sregistry pull {}'.format(container), hide=True, warn=True)
        print("\nCommand output")
        for host_name, out in zip(sorted(self.hosts.keys()), result.values()):
            print(host_name)
            print(out.stdout)
            print(out.stderr)

        for i, host_name in enumerate(sorted(self.hosts.keys())):
            if not nodes_available[host_name]:
                continue

            # filter tasks if they have host attached which != host_name

            # self.submit_single_host(filtered_tasks)
            self.submit_single_host(host_name, self.connections[i], tasks,
                                    nodes_available[host_name], tags, container, working_dir,
                                    num_workers, force_num_tasks)

        # TODO: make submission separately for each host because they have different number of
        # available nodes. Also, add a duplication ratio, so that tasks are submitted to multiple
        # hosts and the faster wins the race.

    def submit_single_host(self, host_name, connection, tasks, nodes_available, tags, container,
                           working_dir, num_workers, force_num_tasks):

        if force_num_tasks:
            n_tasks = force_num_tasks
        else:
            n_tasks = len(tasks)

        array_option = 'array=1-{};'.format(min(n_tasks, nodes_available))
        flow_options = FLOW_OPTIONS_TEMPLATE.format(
            array=array_option, job_name=".".join(sorted(tags)))

        resources = []
        for name, value in tasks[0]['facility']['resources'].items():
            if name == 'cpu':
                resources.append('cpus-per-task={}'.format(value))
            elif name == 'gpu':
                resources.append('gres=gpu:{}'.format(value))
            elif name == 'mem':
                resources.append('mem={}'.format(value))
            elif name not in IGNORE_RESOURCES:
                raise ValueError('Unknown option: {}'.format(name))

        flow_options += ";" + ";".join(resources)

        submission_root = self.hosts[host_name].get('submission_root', self.submission_root)
        if submission_root is None:
            raise ValueError(
                "submission_root is not defined for host {} nor globally.".format(host_name))
        submission_dir = os.path.join(submission_root, container)
        # TODO: Run mkdirs -p with connection.run instead of python's `os`.
        #       this folder should be created in _ensure_remote_setup.
        if not os.path.isdir(submission_dir):
            connection.run('mkdir -p {}'.format(submission_dir))

        prolog = self.hosts[host_name].get('prolog', self.prolog)

        flow_command = FLOW_TEMPLATE.format(
            container=container, root_dir=submission_dir, prolog=prolog, options=flow_options)

        options = {}
        if working_dir:
            options['working-dir'] = working_dir
        if num_workers:
            options['num-workers'] = num_workers

        if options:
            options = ' ' + ' '.join('--{}={}'.format(k, v) for k, v in options.items())
        else:
            options = ''

        command = COMMAND_TEMPLATE.format(
            container=" --container " + container if container else "",
            tags=" --tags " + " ".join(tags) if tags else "",
            options=options)

        submit_command = SUBMIT_COMMANDLINE_TEMPLATE.format(flow=flow_command, command=command)

        print("Executing on {}:".format(host_name))
        print(submit_command)
        out = connection.run(submit_command, hide=True, warn=True)
        print("\nCommand output")
        print("------")
        print(out.stdout)
        print(out.stderr)
