import json
import os
import sys
import tempfile
import threading
import time

from contextlib import contextmanager
from subprocess import call
from cloudify.celery import celery
from cloudify_rest_client.client import CloudifyClient


_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
_THREAD_COUNT = 10


def _check_alive(name, version):
    if version.startswith('3.1'):
        sep = '.'
    else:
        sep = '@'
    try:
        worker_name = 'celery{0}{1}'.format(sep, name)
        tasks = celery.control.inspect([worker_name]).registered() or {}
        worker_tasks = set(tasks.get(worker_name, {}))
        return 'script_runner.tasks.run' in worker_tasks
    except:
        return False


def _worker_name(name, version):
    if version.startswith('3.1'):
        sep = '.'
    else:
        sep = '@'
    return 'celery{0}{1}'.format(sep, name)


def celery_service_operation(name, operation):
    if operation is None:
        return
    cmd = ['sudo', 'service', 'celeryd-{0}'.format(name), operation]
    status = call(cmd)
    if status:
        raise RuntimeError('Could not perform operation {0}'.format(cmd))


def celery_wait_for_started(name, version, time_to_wait=30):
    worker_name = _worker_name(name, version)
    inspect = celery.control.inspect(destination=[worker_name])
    time.sleep(2)
    timeout = time.time() + time_to_wait
    while time.time() < timeout:
        print 'Waiting for worker {0}'.format(worker_name)
        stats = (inspect.stats() or {}).get(worker_name)
        if stats:
            print 'Worker {0} is up'.format(worker_name)
            return
        time.sleep(4)
    raise RuntimeError('Could not start worker {0}'.format(name))


def is_transient_workers_mode(client=None):
    if client is None:
        client = CloudifyClient()
    transient_config = client.manager.get_context()['context']['cloudify'].get(
        'transient_deployment_workers_mode', {})
    return transient_config.get('enabled', False)


@contextmanager
def with_deployment_env(deployment_id, version, force_restart,
                        restart_init=None, restart_cleanup=None,
                        raise_on_wait_failure=True):
    is_transient_worker = is_transient_workers_mode()
    workflows_worker = '{0}_workflows'.format(deployment_id)
    if force_restart:
        start_operation = 'restart'
    elif is_transient_worker:
        start_operation = 'start'
    else:
        start_operation = None
    if is_transient_worker:
        stop_operation = 'stop'
    elif force_restart:
        stop_operation = 'restart'
    else:
        stop_operation = None
    celery_service_operation(deployment_id, start_operation)
    try:
        if restart_init is not None:
            restart_init()
        celery_service_operation(workflows_worker, start_operation)
        try:
            try:
                celery_wait_for_started(deployment_id, version)
                celery_wait_for_started(workflows_worker, version)
            except Exception as e:
                if raise_on_wait_failure:
                    raise e
            yield
        finally:
            if restart_cleanup is not None:
                restart_cleanup()
            celery_service_operation(workflows_worker, stop_operation)
    finally:
        celery_service_operation(deployment_id, stop_operation)


def check_agents_alive(deployment_id, deployment, version):
   try:
       with with_deployment_env(deployment_id, version, force_restart=False,
                                raise_on_wait_failure=False):
           dep_res = {}
           dep_res['workflows_worker_alive'] = _check_alive(
               '{0}_workflows'.format(deployment_id), version)
           dep_res['operations_worker_alive'] = _check_alive(
               deployment_id, version)
           dep_res['agents_alive'] = {}
           dep_alive = dep_res['workflows_worker_alive'] and dep_res[
               'operations_worker_alive']
           agents = deployment['agents'].keys()
           for agent in agents:
               agent_alive = _check_alive(agent, version)
               dep_res['agents_alive'][agent] = agent_alive
               dep_alive = dep_alive and agent_alive
           return dep_res, dep_alive
   except RuntimeError as err:
       dep_res = {
           'error': str(type(err)) + ': ' + str(err),
       }
       return dep_res, False


def check_vm_access(deployment_id, deployment, test_vm_script):
    dep_res = {}
    dep_python = '~/cloudify.{0}/env/bin/python'.format(deployment_id)
    _, input_path = tempfile.mkstemp()
    _, output_path = tempfile.mkstemp()
    try:
        with open(input_path, 'w') as f:
            f.write(json.dumps(deployment['agents']))
        status = call(['timeout', '20', os.path.expanduser(dep_python),
                       test_vm_script, input_path, output_path])
        if status:
            dep_res['agents_remote_access_error'] = True
        else:
            with open(output_path) as f:
                dep_res['agents_remote_access'] = json.loads(f.read())
        return dep_res
    finally:
        os.remove(input_path)
        os.remove(output_path)


def _get_openstack_data(deployment_id, deployment, vms_data_script):
    dep_res = {}
    dep_python = '~/cloudify.{0}/env/bin/python'.format(deployment_id)
    _, input_path = tempfile.mkstemp()
    _, output_path = tempfile.mkstemp()
    try:
        with open(input_path, 'w') as f:
            f.write(json.dumps(deployment['agents']))
        status = call(['timeout', '20', os.path.expanduser(dep_python),
                       vms_data_script, input_path, output_path])
        if status:
            dep_res['openstack_data_error'] = True
        else:
            with open(output_path) as f:
                dep_res['openstack_data'] = json.loads(f.read())
        return dep_res
    finally:
        os.remove(input_path)
        os.remove(output_path)


def _insert_deployment_result(dep_res, deployment_id, deployment, version, test_vm_access, test_vm_script, vms_data_script):
    print 'On manager: deployment {0}'.format(deployment_id)
    result, dep_alive = check_agents_alive(deployment_id, deployment, version)
    dep_res.update(result)
    openstack_data = _get_openstack_data(
        deployment_id, deployment, vms_data_script)
    dep_res.update(openstack_data)
    if not dep_alive or not test_vm_access:
        return
    vm_access = check_vm_access(deployment_id, deployment, test_vm_script)
    dep_res.update(vm_access)


def _prepare_deployment_results(queue):
    for res, dep_id, agents, version, test_vm_access, test_vm_script, vms_data_script in queue:
        _insert_deployment_result(
            res, dep_id, agents, version, test_vm_access, test_vm_script, vms_data_script)


def main(args):
    path = sys.argv[1]
    output = sys.argv[2]
    version = sys.argv[3]
    test_vm_access = sys.argv[4] != '_'
    if test_vm_access:
        test_vm_script = sys.argv[4]
    else:
        test_vm_script = None
    vms_data_script = sys.argv[5]
    with open(path) as f:
        deployments = json.loads(f.read())

    res = {}
    threads = []
    queues = [[] for _ in range(_THREAD_COUNT)]
    i = 0
    for deployment_id, agents in deployments.iteritems():
        dep_res = {}
        res[deployment_id] = dep_res
        thread_id = i % _THREAD_COUNT
        queues[thread_id].append((dep_res, deployment_id, agents,
                                  version, test_vm_access, test_vm_script, vms_data_script))
        i = i + 1

    for queue in queues:
        thread = threading.Thread(target=_prepare_deployment_results,
                                  kwargs={'queue': queue})
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    with open(output, 'w') as f:
        f.write(json.dumps(res))


if __name__ == '__main__':
    main(sys.argv)
