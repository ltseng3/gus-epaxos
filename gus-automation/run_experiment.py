import concurrent
import os

import time

from command_util import *
from remote_util import *
from setup_network_delay import get_server_name_to_internal_ip_map


def run_experiment(config, timestamp, executor):
    kill_machines(config, executor)

    master_thread = start_master(config, timestamp)
    server_names_to_internal_ips = get_server_name_to_internal_ip_map(config) # for easy binary execution
    server_threads = start_servers(config, timestamp, server_names_to_internal_ips)
    client_thread = start_clients(config, timestamp, server_names_to_internal_ips)

    client_thread.wait()
    for server_thread in server_threads:
        server_thread.terminate()
    master_thread.terminate()

    path_to_client_data = collect_exp_data(config, timestamp, executor)
    calculate_exp_data(config, path_to_client_data)

def start_master(config, timestamp):
    master_command = get_master_cmd(config, timestamp)
    # The first server listed in the 'server_names' config is the master server.
    master_url = get_machine_url(config, config['server_names'][0])

    return run_remote_command_async(master_command, master_url)

def start_servers(config, timestamp, server_names_to_internal_ips):
    server_threads = []

    for server_name in config['server_names']:
        server_url = get_machine_url(config, server_name)
        server_command = get_server_cmd(config, timestamp, server_names_to_internal_ips, server_name)
        server_threads.append(run_remote_command_async(server_command, server_url))

    # I assume there is no way we can detect when the servers are initialized.
    time.sleep(2)
    return server_threads

def start_clients(config, timestamp, server_names_to_internal_ips):
    client_url = get_machine_url(config, 'client')
    client_command = get_client_cmd(config, timestamp, server_names_to_internal_ips)
    return run_remote_command_async(client_command, client_url)

def kill_machines(config, executor):
    futures = []

    master_url = get_machine_url(config, config['server_names'][0])
    futures.append(executor.submit(run_remote_command_sync(['killall', '-15', 'master'], master_url)))

    for server_name in config['server_names']:
        server_url = get_machine_url(config, server_name)
        futures.append(executor.submit(run_remote_command_sync(['killall', '-15', 'server'], server_url)))

    concurrent.futures.wait(futures)

def collect_exp_data(config, timestamp, executor):
    download_futures = []
    control_exp_directory = os.path.join(config['base_control_experiment_directory'], timestamp)
    remote_exp_directory = os.path.join(config['base_remote_experiment_directory'], timestamp)

    # Master machine data is in the logs of the first server.
    for server_name in config['server_names']:
        server_url = get_machine_url(config, server_name)
        download_futures.append(executor.submit(copy_remote_directory_to_local, os.path.join(control_exp_directory, 'server-%s' % server_name), server_url, remote_exp_directory))

    client_url = get_machine_url(config, 'client')
    path_to_client_data = os.path.join(control_exp_directory, 'client')
    download_futures.append(executor.submit(copy_remote_directory_to_local, os.path.join(control_exp_directory, 'client'), client_url, remote_exp_directory))

    concurrent.futures.wait(download_futures)

    return path_to_client_data

def calculate_exp_data(config, path_to_client_data):
    client_cdf_analysis_script = os.path.join(config['control_src_directory'], "client_metrics.py")
    subprocess.call(["python3.8", client_cdf_analysis_script], cwd=path_to_client_data)