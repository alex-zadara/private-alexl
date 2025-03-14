import subprocess
import os.path
import sys
import threading
import argparse
import time

LOG_SETS = {
    'nova'    : ('/var/log/nova/nova-api.log',
                 '/var/log/nova/nova-network.log',
                 '/var/log/nova/nova-compute.log',
                 '/var/log/nova/nova-scheduler.log',
                 '/var/log/nova/nova-objectstore.log',
                 '/var/log/nova/nova-volume.log',
                 '/var/log/nova/nova-vsa.log',
                 '/var/log/nova/nova-manage.log',
                 '/var/log/nova/nova-install.log'),
    'sys'      :('/var/log/syslog', '/var/log/kern.log', '/var/log/auth.log'),
    'vac'      :('/var/log/zadara/zadara_vac.log',),
    'vam'      :('/var/log/zadara/zadara_vam.log',),
    'lsa'      :('/var/log/zadara/zadara_lsa.log',),
    'vc'       :('/var/log/zadara/zadara_vac.log', '/var/log/zadara/zadara_vam.log', '/var/log/zadara/zadara_lsa.log', '/var/log/zadara/zadara_cfg.py.log'),
    'zios'     :('/var/log/zadara/zadara_osm.log','/var/log/zadara/zadara_vac.log','/var/log/zadara/zadara_zoc.log','/var/log/zadara/zadara_zom_master.log','/var/log/zadara/zadara_zom_slave.log'),
    'vccfg'    :('/var/log/zadara/zadara_vccfg.log',),
    'docker'   :('/var/log/upstart/docker.log',),
    'sn'       :('/var/log/zadara/zadara_snmonitor.log', '/var/log/zadara/zadara_sncfg.log')
}

def run_plink_with_tail_logs(plink_path, ip, port, username, password, remote_log_file, local_logs_dir, pull_existing_log):
    # Build the command to run
    cmd = '';
    if pull_existing_log:
        cmd = 'echo ------ Existing log: `date`; cat ' + remote_log_file + '; '
    cmd = cmd + 'date; echo ------ Tailing: `date`; tail -F ' + remote_log_file; 
    args = [plink_path, '-auto-store-sshkey', '-P', str(port), '-ssh', '-pw', password, username + '@' + ip, cmd]

    local_log_file = os.path.join(local_logs_dir, ip + '.' + os.path.basename(remote_log_file))
    local_log_file_obj = open(local_log_file, 'a') # TODO ask the user whether to truncate or append

    while True:
        subpr_obj = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=local_log_file_obj, stderr=local_log_file_obj)
        subpr_obj.communicate(b'y') # The input argument must be a byte string
        print('plink pulling from [{0}] terminated, restarting'.format(remote_log_file))
    
def main():
    parser = argparse.ArgumentParser(description='Connects to a Linux machine, performs "tail -F " on selected set of logs and save the logs in local files')
    parser.add_argument('ip', nargs='+', help='The IP address(es) of machine(s) to connect to')
    parser.add_argument('-P', '--port', default=22, type=int, help='The port to use when connecting, default is 22')
    parser.add_argument('-u', '--user', default='root', help='The username to use when connecting, default is "root"')
    parser.add_argument('-p', '--password', default='root', help='The password to use when connecting, default is "root"')
    log_choices = tuple(LOG_SETS.keys())
    parser.add_argument('-l', '--logs', action='append', choices=log_choices, required=True, help='The set of log files to monitor')
    parser.add_argument('--plink_path', default='C:\PortablePrograms\kitty\klink.exe', help='The path to the plink program, default is "C:\Programs\plink\plink.exe"')
    parser.add_argument('--local_logs_dir', default='C:\Work\Logs', help='The local directory to store the log files, default is "C:\Work\Logs"')
    parser.add_argument('-e', '--pull_existing_logs', action='store_true', default=False, help='Whether to pull existing content from the log file, before "tail -F", default is False');
    opts = parser.parse_args()

    threads = []
    
    remote_log_files = []
    for log_set_name in opts.logs:
        remote_log_files.extend(LOG_SETS[log_set_name])

    for ip_address in opts.ip:
        for remote_log_file in remote_log_files:
            print('Starting thread for [{0}] on [{1}]'.format(remote_log_file, ip_address))
            thr = threading.Thread(name='Thread for [{0}] on [{1}]'.format(remote_log_file, ip_address),
                                   target=run_plink_with_tail_logs,
                                   args=(opts.plink_path, ip_address, opts.port, opts.user, opts.password, remote_log_file, opts.local_logs_dir, opts.pull_existing_logs))
            threads.append(thr)
            thr.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        os.abort()

main()
