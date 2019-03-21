# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.remoteflow.resources -- TODO
===================================

.. module:: resources
    :platform: Unix
    :synopsis: TODO

TODO: Write long description

"""
import getpass
import logging
import os
import subprocess

import invoke.exceptions.UnexpectedExit

from fabric import ThreadingGroup as Group

from mahler.core.resources import Resources


logger = logging.getLogger(__name__)


SUBMISSION_ROOT = os.environ['FLOW_SUBMISSION_DIR']

FLOW_OPTIONS_TEMPLATE = "{array}time=2:59:00;job-name={job_name}"

FLOW_TEMPLATE = "flow-submit {container} --root {root_dir} --prolog {prolog} --options {options}"

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
"""


class RemoteFlowResources(Resources):
    """
    """

    def __init__(self, hosts, max_workers=100, init_only=False, submission_root=None, prolog=""):
        """
        """
        if not isinstance(hosts, dict):
            hosts = {host: {'max_workers': max_workers} for host in hosts}

        self.max_workers = max_workers
        self.prolog = prolog
        self.hosts = hosts
        self.submission_root = submission_root
        self.connections = Group(hosts.keys())
        self.init_only = init_only

    def available(self, squash=True):
        """
        """
        command = 'squeue -r -o %t -u {user}'.format(user=getpass.getuser())
        jobs = {}
        max_workers = 0
        result = self.connections.run(command, hide=True)
        for host in result.keys():
            logger.debug('squeue on {}'.format(host))
            max_workers += self.hosts[host].get('max_workers', self.max_workers)
            out = str(result[host], encoding='utf-8')
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

            jobs[host] = sum(number for name, number in states.items() if name != 'CG')
            logging.debug('total: {}'.format(jobs[host]))

        if squash:
            return max(max_workers - sum(jobs.values()), 0)
        else:
            return {host_name: host_config['max_workers'] - jobs[host_name]
                    for host_name, host_config in self.hosts.items()}

    def _ensure_remote_setup():

        import pdb
        pdb.set_trace()

        for host_name, host_connection in zip(self.hosts.keys(), self.connection):
            print('Adding source remoteflowrc in {}:bashrc'.format(host_name))
            patchwork.files.append(host_connection, '.bashrc', 'source $HOME/.remoteflowrc')

            rc_file_path = self.hosts.get('rc', self.rc)
            try:
                out = host_connection.run('cat .remoteflowrc')
                if '\n'.join(out.split('\n')[:-1]) != open(rc_file_path, 'r').read():
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
                    host_connection, '{}/.config/mahler'.format(os.environ['HOME']), '.config')

            if not patchwork.environment.have_program(host_connection, 'flow-submit'):
                print('Installing flow on {}'.format(host_name))
                host_connection.run('pip install --user git+https://github.com/bouthilx/flow.git')

            if not patchwork.environment.have_program(host_connection, 'sregistry'):
                print('Installing sregistry on {}'.format(host_name))
                host_connection.run('pip install --user sregistry')

    def submit(self, tasks, container=None, tags=tuple(), working_dir=None):
        """
        """
        self._ensure_remote_setup()
        if self.init_only:
            return

        nodes_available = self.available(squash=False)
        total_nodes_available = sum(nodes_available.values())
        print('{} nodes available'.format(total_nodes_available))
        for host_name, host_nodes_available in nodes_available.items():
            print('{:>20}: {} nodes available'.format(host_name, host_nodes_available))

        if not total_nodes_available:
            return

        print('Pulling container {} on logging node of hosts {}'.format(
            container, list(self.hosts.keys())))
        out = self.connections.run('sregistry pull {}'.format(container), hide=True)
        print("\nCommand output")
        print("------")
        print(str(out, encoding='utf-8'))

        for host_name in self.hosts.keys():
            if not nodes_available[host_name]:
                continue

            # filter tasks if they have host attached which != host_name

            self.submit_single_host(filtered_tasks)

        # TODO: make submission separately for each host because they have different number of
        # available nodes. Also, add a duplication ratio, so that tasks are submitted to multiple
        # hosts and the faster wins the race.

    def submit_single_host(self, host_name, connection, tasks, nodes_available, container):

        array_option = 'array=1-{};'.format(min(len(tasks), nodes_available))
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
            else:
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

        command = COMMAND_TEMPLATE.format(
            container=" --container " + container if container else "",
            tags=" --tags " + " ".join(tags) if tags else "",
            options=" --working-dir={}".format(working_dir) if working_dir else "")

        submit_command = SUBMIT_COMMANDLINE_TEMPLATE.format(flow=flow_command, command=command)

        print("Executing:")
        print(submit_command)
        out = self.connections.run(submit_command, hide=True)
        print("\nCommand output")
        print("------")
        print(str(out, encoding='utf-8'))
