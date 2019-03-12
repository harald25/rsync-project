#! /usr/bin/env python3.6

import subprocess, os.path, paramiko
from datetime import datetime
from paramiko import SSHClient

def main():

    #createClone("backup/backup-ipsec","5_inc","incremental")
    initiateClient("backup-ipsec", "root")


def createClone(main_dataset_name, sub_dataset_name, backup_type):
    time_now = datetime.today().strftime('%Y-%m-%d--%H-%M-%S')
    #Take snapshot
    subprocess.run(['zfs', 'snapshot', main_dataset_name + '/' + sub_dataset_name +'@' + time_now])
    #Make clone
    subprocess.run(['zfs', 'clone', main_dataset_name + '/' + sub_dataset_name +'@' + time_now, main_dataset_name + '/' + backup_type + '_' + time_now ])

def initiateClient(client, username):
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(username + '@' + client + '/root/zfsync_test/rsync-project')
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('./client_backup.py')
    print(ssh_stdout)


if __name__ == "__main__":
    main()
