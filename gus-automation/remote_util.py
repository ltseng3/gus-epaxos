import subprocess


def get_machine_url(config, server_name):
    return "%s@%s" % (config['cloudlab_user'],
                      (config['host_format_str'] % (server_name, config['experiment_name'], config['project_name'])))


def run_remote_command_sync(command, remote_url):
    print(command)
    return subprocess.run(ssh_args(command, remote_url),
                          stdout=subprocess.PIPE, universal_newlines=True).stdout


def ssh_args(command, remote_url):
    return ["ssh", '-o', 'StrictHostKeyChecking=no',  # can connect with machines that are not in the known host list
            '-o', 'ControlMaster=auto',  # multiplex ssh connections with a single tcp connection
            '-o', 'ControlPersist=2m',  # after the first connection is closed, use tcp connection for up to 2 minutes
            '-o', 'ControlPath=~/.ssh/cm-%r@%h:%p',  # location of the control socket
            remote_url, command]
