#! /usr/bin/env python3.6

import subprocess, os.path, paramiko, sys
from datetime import datetime
from paramiko import SSHClient

def main():

    # Check for presence of lock file
    #createClone("backup/backup-ipsec","5_inc","incremental")
    (stdout, stderr, exit_code) = initiateClient("backup-ipsec", "root")
    # Rsync files
    # End backup at client
    
    if stdout:
        print(stdout)
    if stderr:
        print (stderr)
    sys.exit(exit_code)


def createClone(main_dataset_name, sub_dataset_name, backup_type):
    time_now = datetime.today().strftime('%Y-%m-%d--%H-%M-%S')
    #Take snapshot
    subprocess.run(['zfs', 'snapshot', main_dataset_name + '/' + sub_dataset_name +'@' + time_now])
    #Make clone
    subprocess.run(['zfs', 'clone', main_dataset_name + '/' + sub_dataset_name +'@' + time_now, main_dataset_name + '/' + backup_type + '_' + time_now ])

def initiateClient(client, username):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(client, username = username)
    (ssh_stdin, ssh_stdout, ssh_stderr) = ssh.exec_command('/root/zfsync_test/rsync-project/client_backup.py --initiate-backup')
    stdout = ssh_stdout.readlines()
    stderr = ssh_stderr.readlines()
    exit_code = ssh_stdout.channel.recv_exit_status()
    ssh.close()
    return (stdout, stderr, exit_code)

def endClient(client, username):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(client, username = username)
    (ssh_stdin, ssh_stdout, ssh_stderr) = ssh.exec_command('/root/zfsync_test/rsync-project/client_backup.py --end-backup')
    stdout = ssh_stdout.readlines()
    stderr = ssh_stderr.readlines()
    exit_code = ssh_stdout.channel.recv_exit_status()
    ssh.close()
    return (stdout, stderr, exit_code)


if __name__ == "__main__":
    main()
