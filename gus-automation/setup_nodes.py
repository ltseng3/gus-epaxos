import concurrent
import os
import subprocess
import time

from remote_util import *


def setup_nodes(config, executor):
    make_binaries(config)
    exp_directory = prepare_local_exp_directory(config)
    prepare_remote_exp_directories(config, exp_directory, executor)
    copy_binaries_to_machines(config, executor)


def make_binaries(config):
    print("making binaries")

    control_src_directory = config['control_src_directory']

    # Add temporary environment variables to be used when compiling binaries.
    e = os.environ.copy()
    e["GOPATH"] = control_src_directory # the bin directory will be located inside the repo

    # Make binaries in bin directory (located at <control_src_directory>/bin).
    subprocess.call(["go", "install", "master"], cwd=control_src_directory, env=e)
    subprocess.call(["go", "install", "server"], cwd=control_src_directory, env=e)
    subprocess.call(["go", "install", "client"], cwd=control_src_directory, env=e)


def prepare_local_exp_directory(config, config_file=None):
    print("making local experiment directory")

    exp_directory = get_timestamped_exp_dir(config)
    os.makedirs(exp_directory)
    # shutil.copy(config_file, os.path.join(exp_directory, os.path.basename(config_file)))
    return exp_directory


# TODO include type of experiment (latency, throughput, etc.) here
def get_timestamped_exp_dir(config):
    now_string = time.strftime('%Y-%m-%d-%H-%M-%S',
                               time.localtime())
    return os.path.join(config['base_control_experiment_directory'], now_string)


def prepare_remote_exp_directories(config, local_exp_directory, executor):
    print("preparing remote directories")

    # Prepare remote out directory with timestamped experiment directory folder
    remote_directory = os.path.join(config['base_remote_experiment_directory'], os.path.basename(local_exp_directory))
    remote_out_directory = os.path.join(remote_directory, 'out')

    futures = []
    for server_name in config['server_names']:
        futures.append(executor.submit(prepare_remote_exp_directory, config, server_name, remote_out_directory))

    futures.append(executor.submit(prepare_remote_exp_directory, config, 'client', local_exp_directory, remote_out_directory))

    concurrent.futures.wait(futures)
    return remote_directory


def prepare_remote_exp_directory(config, machine_name, remote_out_directory):
    machine_url = get_machine_url(config, machine_name)
    run_remote_command_sync('mkdir -p %s' % remote_out_directory, machine_url)


def copy_binaries_to_machines(config, executor):
    print("copying binaries")

    control_binary_directory = os.path.join(config['control_src_directory'], 'bin')
    remote_binary_directory = config['remote_bin_directory']

    futures = []
    for server_name in config['server_names']:
        server_url = get_machine_url(config, server_name)
        futures.append(executor.submit(copy_path_to_remote_host,
                                       control_binary_directory, server_url, remote_binary_directory))

    client_url = get_machine_url(config, 'client')
    futures.append(executor.submit(copy_path_to_remote_host,
                                   control_binary_directory, client_url, remote_binary_directory))

    concurrent.futures.wait(futures)