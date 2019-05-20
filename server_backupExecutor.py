#! /usr/bin/env python3.6

import subprocess, os.path, paramiko, sys
from datetime import datetime
from paramiko import SSHClient
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

def main():

    # Parameters:
    #   - Client (hostname of client),
    #   - Dataset path (path to the root folder of where the backupjob is stored)
    #   - Backup type (Full, differential, incremental)

    # Check status of last run
        # If last run failed, do cleanup
    # Check for presence of lock file
    #createDataset("backup/backup-ipsec","5_inc","incremental")
    (stdout, stderr, exit_code) = initiateClient("backup-ipsec", "root")
    # Rsync files
    # End backup at client

    if stdout:
        print(stdout)
    if stderr:
        print (stderr)
    sys.exit(exit_code)


def createDataset(root_dataset_name, backup_type):

    """
    This function creates a ZFS dataset.
    The function takes two parameters: root_dataset_name and backup_type

    Parameters
    ----------
    root_dataset_name : Should be the ZFS name for the root dataset of the backup job.
                        The new dataset for this backup will be created here

    backup_type :   is either 'full', 'diff', or 'inc'
                    If backup type is full, a new empty dataset is created under the specified root_dataset_name
                    If backup_type = diff, a clone is made from the last successful full backup

    """

    time_now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    dataset_list = subprocess.run(['zfs', 'list', '-t', 'filesystem', '-o', 'name', '-H', '-r', root_dataset_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    dataset_list = dataset_list.stdout.splitlines().sort()

    if dataset_list.stderr:
        if "dataset does not exist" in dataset_list.stderr:
            writeToLog("critical", dataset_list.stderr, root_dataset_name)
            sys.exit(EXIT_CRITICAL)
        else:
            writeToLog("critical", dataset_list.stderr, root_dataset_name)
            sys.exit(EXIT_CRITICAL)

    else:

        if backup_type == "full":
            new_dataset_name = root_dataset_name +'/' + time_now + "_full"
            new_dataset = subprocess.run(['zfs', 'create', new_dataset_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if new_dataset.stderr:
                writeToLog("critical", new_dataset.stderr, root_dataset_name)
                sys.exit(EXIT_CRITICAL)
            else:
                writeToLog("info", "New dataset created: " + new_dataset_name, root_dataset_name)
                return new_dataset.stdout

        if backup_type == "diff":
            #Make a new list with only the full backups
            dataset_list_full = []
            for dataset in dataset_list:
                bkp_type = dataset.split("_")
                if bkp_type[-1] == "full":
                    dataset_list_full.append(dataset)

            dataset_list_full.sort()
            last_full_backup = dataset_list_full[-1]
            snapAndCloneDataset(last_full_backup,"diff")

        if backup_type == "inc":
            last_backup = dataset_list[-1]
            snapAndCloneDataset(last_backup,"inc")

def snapAndCloneDataset(dataset_name,suffix):
    """
    This function creates a ZFS snapshot of specified dataset and makes a clone
    of that snapshot.
    The function takes two parameters: dataset_name and suffix

    Parameters
    ----------
    dataset_name :  Should be the ZFS name for the dataset to be snapshotted and
                    cloned
    suffix :        Should be either 'diff' or 'inc' to mark what type of backup
                    the dataset contains


    """
    root_dataset_name = dataset_name.rsplit("/",1)[0]
    snapshot_name = dataset_name +'@' + time_now + "_snap"
    clone_name = root_dataset_name + '/' + time_now + "_" +suffix
    time_now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    #Take snapshot
    snap = subprocess.run(['zfs', 'snapshot', snapshot_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if snap.stderr:
        writeToLog("critical", snap.stderr, root_dataset_name)
        sys.exit(EXIT_CRITICAL)
    else:
        writeToLog("info", "Snapshot created:" +snapshot_name, root_dataset_name)
    #Make clone
    clone = subprocess.run(['zfs', 'clone', snapshot_name, clone_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if clone.stderr:
        writeToLog("critical", clone.stderr, root_dataset_name)
        sys.exit(EXIT_CRITICAL)
    else:
        writeToLog("info", "Clone created:" +clone_name, root_dataset_name)


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

def writeToLog(level, message, root_dataset_name):
    """
    Writes message and its verbosity level to log file. Timestamp is automatically added
    Log files are stored at the root dataset for the running backup job.

    Parameters
    ----------
    level :         Verbosity level: info, warning, critical
    message :       The message to log
    root_dataset_name :  The name of the current dataset. Used to determine log file location

    """
    date_now = datetime.today().strftime('%Y-%m-%d')
    datetime_now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    with open("/"+root_dataset_name+"/"+time_now+".log", "a+", encoding="utf-8") as log:
        log.write(datetime_now + " - " + level + " - " + message)
    log.closed()



if __name__ == "__main__":
    main()
