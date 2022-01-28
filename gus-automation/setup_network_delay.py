import subprocess
import concurrent
import sys


def setup_delays(config, executor):
    # Note: trading off performance so I can have simplicity. each helper function takes a copy of the config
    # dictionary instead of not copying the config dictionary and passing the necessary config fields in manually.
    # Especially since delays are being setup on each server in parallel, this might increase RAM usage a bit.
    futures = []

    server_names_to_ips = get_server_name_to_ip_map(config)
    print("server_names_to_ips:", server_names_to_ips)

    for server_name in config['server_names']:
        server_host = get_server_host(config, server_name)
        print("setting up delay for", server_host)

        server_ips_to_delays = get_ip_to_delay(config, server_names_to_ips, server_name)
        print("ips to delays:", server_ips_to_delays)

        server_interface = get_exp_net_interface(config, server_host)
        if config['emulate_wan_latency']:
            futures.append(executor.submit(add_delays_for_ips, config, server_ips_to_delays,
                                           server_interface, server_host))
        # Get rid of network delay if using lan latency.
        else:
            futures.append(executor.submit(run_remote_command_sync,
                                           'sudo tc qdisc del dev %s root' % server_interface,
                                           config['cloudlab_user'], server_host))

    # Note: client machine has lan latency to each server

    concurrent.futures.wait(futures)


def get_server_name_to_ip_map(config):
    # Note: We get ips through the first server in the server names list just in case the control machine is not in the
    # cloudlab cluster.
    name_to_ip = {}

    for server_name in config['server_names']:
        ip = get_ip_for_server_name_from_remote_machine(server_name, config['cloudlab_user'], get_server_host(config, config['server_names'][0]))
        name_to_ip[server_name] = ip

    ip = get_ip_for_server_name_from_remote_machine('client', config['cloudlab_user'], get_server_host(config, config['server_names'][0]))
    name_to_ip['client'] = ip

    return name_to_ip

def get_ip_for_server_name_from_remote_machine(server_name, remote_user, remote_host):
    return run_remote_command_sync('getent hosts %s | awk \'{ print $1 }\'' % server_name, remote_user, remote_host).rstrip()


def get_server_host(config, server_name):
    return config['host_format_str'] % (server_name, config['experiment_name'], config['project_name'])


def get_ip_to_delay(config, name_to_ip, server_name):
    ip_to_delay = {}
    name_to_delay = config['server_ping_latencies'][server_name]

    for name, ip in name_to_ip.items():
        if name not in name_to_delay:
            print("ERROR: in the config file, server_names does not match with server_ping_latencies, "
                  "specifically key %s in server_ping_latencies" % server_name)
            sys.exit()
        ip_to_delay[ip] = name_to_delay[name]

    return ip_to_delay


def get_exp_net_interface(config, remote_host):
    return run_remote_command_sync('cat /var/emulab/boot/ifmap | awk \'{ print $1 }\'', config['cloudlab_user'], remote_host).rstrip()


def add_delays_for_ips(config, ip_to_delay, interface, remote_host):
    max_bandwidth = config['max_bandwidth']
    remote_user = config['cloudlab_user']

    command = 'sudo tc qdisc del dev %s root; ' % interface
    command += 'sudo tc qdisc add dev %s root handle 1: htb; ' % interface
    command += 'sudo tc class add dev %s parent 1: classid 1:1 htb rate %s; ' % (interface, max_bandwidth) # we want unlimited bandwidth
    idx = 2
    for ip, delay in ip_to_delay.items():
        command += 'sudo tc class add dev %s parent 1:1 classid 1:%d htb rate %s; ' % (interface, idx, max_bandwidth)
        command += 'sudo tc qdisc add dev %s handle %d: parent 1:%d netem delay %dms; ' % (interface, idx, idx, delay / 2)
        command += 'sudo tc filter add dev %s pref %d protocol ip u32 match ip dst %s flowid 1:%d; ' % (interface, idx, ip, idx)
        idx += 1
    run_remote_command_sync(command, remote_user, remote_host)


def run_remote_command_sync(command, remote_user, remote_host):
    print(command)
    return subprocess.run(ssh_args(command, remote_user, remote_host),
                          stdout=subprocess.PIPE, universal_newlines=True).stdout

def ssh_args(command, remote_user, remote_host):
    return ["ssh", '-o', 'StrictHostKeyChecking=no',  # can connect with machines that are not in the known host list
            '-o', 'ControlMaster=auto',  # multiplex ssh connections with a single tcp connection
            '-o', 'ControlPersist=2m',  # after the first connection is closed, use tcp connection for up to 2 minutes
            '-o', 'ControlPath=~/.ssh/cm-%r@%h:%p',  # location of the control socket
            '%s@%s' % (remote_user, remote_host), command]