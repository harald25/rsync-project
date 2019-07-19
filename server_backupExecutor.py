#! /usr/bin/env python3.6

import subprocess, os.path, paramiko, sys, argparse
from datetime import datetime
from paramiko import SSHClient
from shared_functions import *
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

arg_parser = argparse.ArgumentParser(description='Server side script that does the main execution of the backup job.')
arg_parser.add_argument("volumes", nargs='*',help="Full path of all the logical volumes to back up")
arg_parser.add_argument('-c','--client', help='DNS solvable hostname/FQDN, or IP address of the client', required=True)
arg_parser.add_argument('-p','--dataset-name', help='Name of the root dataset where the backupjob is stored', required=True)
arg_parser.add_argument('-t','--backup-type', help='Type of backup to perform',choices=['full','diff','inc'], required=True)

arguments = arg_parser.parse_args()

time_now = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
lv_suffix = "_rsyncbackup_"+time_now
lock_file = "/"+arguments.dataset_name+"/lock"
log_file = "/"+arguments.dataset_name+"/"+time_now+".log"

def main():
    print(arguments)

    if check_lockfile(lock_file):
        print("Lock file is present. Exiting!")
        sys.exit(EXIT_CRITICAL)
    else:
        try:
            create_lockfile(lock_file)
        except Exception as e:
            print(e)
            sys.exit(EXIT_CRITICAL)

    #Creating new dataset for the backup job
    returncode = create_dataset(arguments.dataset_name,arguments.backup_type)
    if returncode:
        print("Critical! Exiting because of an error while creating ZFS dataset. See log file for details")
        delete_lockfile(lock_file)
        sys.exit(EXIT_CRITICAL)
    else:
        print("It's wooorking!")

    #(stdout, stderr, exit_code) = initiate_client("backup-ipsec", "root")
    # Rsync files
    # End backup at client

    # if stdout:
    #     print(stdout)
    # if stderr:
    #     print (stderr)
    # sys.exit(exit_code)

    delete_lockfile(lock_file)



def create_dataset(root_dataset_name, backup_type):

    """
    This function creates a ZFS dataset.
    The function takes two parameters: root_dataset_name, and backup_type

    Parameters
    ----------
    root_dataset_name : Should be the ZFS name for the root dataset of the backup job.
                        The new dataset for this backup will be created here

    backup_type :   is either 'full', 'diff', or 'inc'
                    If backup type == full, a new empty dataset is created under the specified root_dataset_name
                    If backup_type == diff, a clone is made from the last successful full backup
                    If backup_type == 'inc' a clone from the previous successful backup is made, regardless of type

    """

    #Get a list of all existing datasets under the specified root dataset
    datasets = subprocess.run(['zfs', 'list', '-t', 'filesystem', '-o', 'name', '-H', '-r', root_dataset_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #If root dataset does not exist. Set backup type to 'full' and continue
    if "dataset does not exist" in datasets.stderr:
        write_to_log("info", "Root dataset "+root_dataset_name" did not already exist. Backup type forced to 'full'. A new root dataset will be created", log_file)
        backup_type = "full"
    #If error from listing dataset (but not because the dataset does not exist)
    elif datasets.stderr and ("dataset does not exist" not in datasets.stderr):
        write_to_log("critical", datasets.stderr, log_file)
        return 1

    if backup_type == "full":
        new_dataset_name = root_dataset_name +'/' + time_now + "_full"
        new_dataset = subprocess.run(['zfs', 'create','-p', new_dataset_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if new_dataset.stderr:
            write_to_log("critical",new_dataset.stderr, log_file)
            return 1
        else:
            write_to_log("info", "New dataset created successfuly: " + new_dataset_name, log_file)
            return 0

    else:
        dataset_list = datasets.stdout.splitlines()[1:] #Remove 1st item from list, since it is the root backupset for the backup job
        if backup_type == "diff":
            #Make a new list with only the full backups
            dataset_list_full = []
            for dataset in dataset_list:
                bkp_type = dataset.split("_")
                if bkp_type[-1] == "full":
                    dataset_list_full.append(dataset)

            dataset_list_full.sort()
            last_full_backup = dataset_list_full[-1]
            returncode = snap_and_clone_dataset(last_full_backup,backup_type)
            if returncode:
                write_to_log("critical", "Error while snapshoting and cloning dataset", log_file)
                return returncode
            else:
                write_to_log("info", "Snaphot and clone successful", log_file)
                return returncode

        elif backup_type == "inc":
            last_backup = dataset_list[-1]
            returncode = snap_and_clone_dataset(last_backup,backup_type)
            if returncode:
                write_to_log("critical", "Error while snapshoting and cloning dataset", log_file)
                return returncode
            else:
                write_to_log("info", "Snaphot and clone successful", log_file)
                return returncode

        else:
            print ("You should not have been able to get here")
            sys.exit(EXIT_CRITICAL)

def snap_and_clone_dataset(dataset_name,backup_type):
    """
    This function creates a ZFS snapshot of specified dataset and makes a clone
    of that snapshot.
    The function takes two parameters: dataset_name and backup_type

    Parameters
    ----------
    dataset_name :  Should be the ZFS name for the dataset to be snapshotted and
                    cloned. Will be on the form "backup/jobname/datetime_backuptype"
    backup_type :   Should be either 'diff' or 'inc'. Will be appended to the name
                    of the new dataset to mark what type of backup it contains

    """

    root_dataset_name = dataset_name.split("/")[0]
    root_backup_dataset_name = dataset_name.split("/")[0]+"/"+dataset_name.split("/")[1]

    snapshot_name = dataset_name +'@' + time_now + "_snap"
    clone_name = root_backup_dataset_name + '/' + time_now + "_" +backup_type
    time_now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    #Take snapshot
    snap = subprocess.run(['zfs', 'snapshot', snapshot_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if snap.stderr:
        write_to_log("critical", snap.stderr, log_file)
        return 1
    else:
        write_to_log("info", "Snapshot created:" +snapshot_name, log_file)
    #Make clone
    clone = subprocess.run(['zfs', 'clone', snapshot_name, clone_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if clone.stderr:
        write_to_log("critical", clone.stderr, log_file)
        return 1
    else:
        write_to_log("info", "Clone created:" +clone_name, log_file)
        return 0


def initiate_client(client, username):
    """
    This function initiates a backup on a client by calling 'client_backup.py' on
    the client.
    The function takes two parameters: client and username

    Parameters
    ----------
    client :        Hostname or IP address of the client where we want to initiate
                    a backup
    username :      The username that we will use to connect to the client.

    """

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(client, username = username)
    (ssh_stdin, ssh_stdout, ssh_stderr) = ssh.exec_command('/root/rsync-project/client_backup.py --initiate-backup')
    stdout = ssh_stdout.readlines()
    stderr = ssh_stderr.readlines()
    exit_code = ssh_stdout.channel.recv_exit_status()
    ssh.close()
    return (stdout, stderr, exit_code)

def end_client(client, username):

    """
    This function ends a backup on a client by calling 'client_backup.py' on
    the client.
    The function takes two parameters: client and username

    Parameters
    ----------
    client :        Hostname or IP address of the client where we want to initiate
                    a backup
    username :      The username that we will use to connect to the client.

    """

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(client, username = username)
    (ssh_stdin, ssh_stdout, ssh_stderr) = ssh.exec_command('/root/zfsync_test/rsync-project/client_backup.py --end-backup')
    stdout = ssh_stdout.readlines()
    stderr = ssh_stderr.readlines()
    exit_code = ssh_stdout.channel.recv_exit_status()
    ssh.close()
    return (stdout, stderr, exit_code)


def check_last_backup_status(root_dataset_name):
    """
    Checks the status from the last run of the bacup. Will return status and date of last
    backup, date of last successful backup. If there are no last backups, or no successful
    backups, the return value will be None
    The status is checked by parsing /backup/jobname/status.txt

    Parameters
    ----------
    root_dataset_name :         Name of the ZFS root dataset for backupjob to check
                                Example: 'backup/job1'

    """

    #status_history = []
    last_successful_date = None
    last_backup_date = None
    last_backup_status = None

    datetime_now = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    with open("/"+root_dataset_name+"/"+status+".txt", "r", encoding="utf-8") as status:
        for line in status:
            last_backup_status = status.split(",")[0]
            last_backup_date = status.split(",")[1]
            if status.split(",")[0] == "successful":
                last_successful_date = status.split(",")[1]

    status.closed()
    return last_successful_date, last_backup_date, last_backup_status



if __name__ == "__main__":
    main()
