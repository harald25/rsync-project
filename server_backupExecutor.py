#! /usr/bin/env python3.6

import subprocess, os.path, paramiko, sys, argparse, time
from datetime import datetime
from paramiko import SSHClient
from shared_functions import *
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

arg_parser = argparse.ArgumentParser(description='Server side script that does the main execution of the backup job.')
arg_parser.add_argument("volumes", nargs='*',help="Full path of all the logical volumes to back up")
arg_parser.add_argument('-c','--client', help='DNS solvable hostname/FQDN, or IP address of the client', required=True)
arg_parser.add_argument('-p','--dataset-name', help='Name of the root dataset where the backupjob is stored', required=True)
arg_parser.add_argument('-t','--backup-type', help='Type of backup to perform',choices=['full','diff','inc'], required=True)
arg_parser.add_argument('-v','--verbosity-level',nargs='?', const=3, type=int, default=3, help='Level of verbosity for logging and printing. 0 = no logging/printing, 1 = errors, 2 = warning + error, 3 = warning+error+info')

arguments = arg_parser.parse_args()

time_now = datetime.today().strftime('%Y-%m-%dT%H-%M-%S')
lv_suffix = "_rsyncbackup_"+time_now
lock_file = "/"+arguments.dataset_name+"/lock"
backupjob_log_file = "/"+arguments.dataset_name+"/"+time_now+"_"+arguments.backup_type+".log"
main_log_file = "/backup/backupexecutor.log"
client_snapshot_mount_path = "/mnt/rsyncbackup"
ssh_user = "root"

def main():
    log_and_print(arguments.verbosity_level,"info", "Starting backupjob", main_log_file)
    log_and_print(arguments.verbosity_level,"info", str(arguments), main_log_file)

    #Check that the root dataset for the backup job exists.
    if not os.path.isdir("/"+arguments.dataset_name):
        log_and_print(arguments.verbosity_level,"critical", "Root dataset for backup job, "+arguments.dataset_name+" does not exist. Exiting!", main_log_file)
        sys.exit(EXIT_CRITICAL)

    #Root dataset exists. Let's continue!
    else:
        #Check if there is a lock file present
        if check_lockfile(lock_file):
            log_and_print(arguments.verbosity_level,"warning", "Lock file is present. Exiting!", backupjob_log_file)
            sys.exit(EXIT_WARNING)
        else:
            create_lockfile(lock_file)

        #Creating new dataset for the running backup job
        returncode,current_dataset = create_dataset(arguments.dataset_name,arguments.backup_type)
        if returncode:
            log_and_print(arguments.verbosity_level,"critical", "Error while creating dataset", backupjob_log_file)
            delete_lockfile(lock_file)
            sys.exit(EXIT_CRITICAL)
        else:
            log_and_print(arguments.verbosity_level,"info", "Dataset created", backupjob_log_file)

        #For each logical volume specified, initiate client and run rsync
        for volume in arguments.volumes:
            (ic_stdout, ic_stderr, ic_exit_code) = initiate_client(arguments.client, ssh_user, volume,lv_suffix)
            if ic_exit_code:

                log_and_print(arguments.verbosity_level,"critical", "Error while initiating client", backupjob_log_file)
                delete_lockfile(lock_file)
                sys.exit(EXIT_CRITICAL)
            else:


                log_and_print(arguments.verbosity_level,"info", "Client initiated successfully", backupjob_log_file)
                log_and_print(arguments.verbosity_level,"info", str(ic_stdout), backupjob_log_file)
                # Rsync files
                rsync_status = rsync_files(arguments.client, volume, lv_suffix, current_dataset)
                if rsync_status:

                    log_and_print(arguments.verbosity_level,"critical","Rsync failed for volume: "+volume+lv_suffix,backupjob_log_file)
                    (ec_stdout, ec_stderr, ec_exit_code) = end_client(arguments.client, ssh_user, volume,lv_suffix)
                    if ec_exit_code:


                        log_and_print(arguments.verbosity_level,"critical","Unable to end_client for volume: "+volume+lv_suffix,backupjob_log_file)
                        log_and_print(arguments.verbosity_level,"critical",str(ec_stderr),backupjob_log_file)
                    else:


                        log_and_print(arguments.verbosity_level,"info","end_client successful for volume: "+volume+lv_suffix,backupjob_log_file)
                        log_and_print(arguments.verbosity_level,"info",str(ec_stdout),backupjob_log_file)
                    delete_lockfile(lock_file)
                    sys.exit(EXIT_CRITICAL)
                else:

                    log_and_print(arguments.verbosity_level,"info","Rsync succeeded for volume: "+volume+lv_suffix,backupjob_log_file)
                    (ec_stdout, ec_stderr, ec_exit_code) = end_client(arguments.client, ssh_user, volume,lv_suffix)
                    if ec_exit_code:


                        log_and_print(arguments.verbosity_level,"critical","Unable to end_client for volume: "+volume+lv_suffix,backupjob_log_file)
                        log_and_print(arguments.verbosity_level,"critical",str(ec_stderr),backupjob_log_file)
                    else:


                        log_and_print(arguments.verbosity_level,"info","end_client successful for volume: "+volume+lv_suffix,backupjob_log_file)
                        log_and_print(arguments.verbosity_level,"info",str(ec_stdout),backupjob_log_file)




        log_and_print(arguments.verbosity_level,"info","BackupExecutor has run successfully! Exiting.",backupjob_log_file)
        log_and_print(arguments.verbosity_level,"info","BackupExecutor has run successfully! Exiting.",main_log_file)
        delete_lockfile(lock_file)
        sys.exit(EXIT_OK)


def rsync_files(client, volume, lv_suffix, dataset):
    log_and_print(arguments.verbosity_level,"info", "rsync_files function invoked with parameters:", backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "client = "+client, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "volume = "+volume, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "lv_suffix = "+lv_suffix, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "dataset = "+dataset, backupjob_log_file)

    rsync_returncode = None
    def run_rsync_and_yield(cmd):
        rsync_process = subprocess.Popen(cmd,encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for stdout_line in iter(rsync_process.stdout.readline, ""):
            yield stdout_line
        rsync_process.stdout.close()
        rsync_returncode = rsync_process.wait()

    try:
        lv_name = volume.split("/")[3]
        lv_mount_path = client_snapshot_mount_path+"/"+lv_name+lv_suffix+"/" #We add a trailing slash to copy contents and not the directory itself
        lv_snapshot_name = volume+lv_suffix
        backup_dest_dir = "/"+dataset+"/"+lv_name
        rsync_command = ['rsync', '--progress', '--stats', '-aAX', '--delete', '-e', 'ssh', 'root@'+client+':'+lv_mount_path, backup_dest_dir]
        new_dir = subprocess.run(['mkdir','-p',backup_dest_dir ])

        if new_dir.stderr:
            log_and_print(arguments.verbosity_level,"critical","Unable to create directory: "+backup_dest_dir, backupjob_log_file)
            log_and_print(arguments.verbosity_level,"critical",str(new_dir.stderr),backupjob_log_file)
            return 1 # 1 = error
        else:

            log_and_print(arguments.verbosity_level,"info","New directory created successfully: "+backup_dest_dir, backupjob_log_file)
            log_and_print(arguments.verbosity_level,"info","Starting rsync", backupjob_log_file)
            rsync_start_time = time.time()
            for output in run_rsync_and_yield(rsync_command):
                print(output, end="")
            rsync_execution_time = time.time() - rsync_start_time
            log_and_print(arguments.verbosity_level,"info","Rsync finished executing in: "+str(rsync_execution_time)+" seconds", backupjob_log_file)

            if rsync_returncode == None:
                log_and_print(arguments.verbosity_level,"critical",str("Unhandled error!"),backupjob_log_file)
                return 1 # 1 = error
            elif rsync_returncode == 0:
                log_and_print(arguments.verbosity_level,"info",str("Rsync finished successfully"),backupjob_log_file)
                return 0 # 0 = OK
            else:
                log_and_print(arguments.verbosity_level,"critical","Rsync exited with an error",backupjob_log_file)
                return 1 # 1 = error

    except subprocess.SubprocessError as e:
        print(e.cmd)

    except Exception as e:

        log_and_print(arguments.verbosity_level,"critical", str(e), backupjob_log_file)
        end_client(client,ssh_user,volume,lv_suffix)
        delete_lockfile(lock_file)
        sys.exit(EXIT_CRITICAL)


def create_dataset(root_dataset_name, backup_type):

    """
    This function creates a ZFS dataset.
    The function takes two parameters: root_dataset_name, and backup_type
    The root dataset for the backup job must already exist, or else this function will fail and exit.
    The function returns a return code and the name of the new dataset.
    Return code = 0 = OK
    Return code = 1 = Fail

    Parameters
    ----------
    root_dataset_name : Should be the ZFS name for the root dataset of the backup job.
                        The new dataset for this backup will be created here

    backup_type :   is either 'full', 'diff', or 'inc'
                    If backup type == 'full', a new empty dataset is created under the specified root_dataset_name
                    If backup_type == 'diff', a clone is made from the last successful full backup
                    If backup_type == 'inc' a clone from the previous successful backup is made, regardless of type

    """

    log_and_print(arguments.verbosity_level,"info", "create_dataset function invoked with parameters:", backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "root_dataset_name = "+root_dataset_name, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "backup_type = "+backup_type, backupjob_log_file)

    #Get a list of all existing datasets under the specified root dataset
    datasets = subprocess.run(['zfs', 'list', '-t', 'filesystem', '-o', 'name', '-H', '-r', root_dataset_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if "dataset does not exist" in datasets.stderr:
        log_and_print(arguments.verbosity_level,"critical", datasets.stderr, main_log_file)
        return 1,""
    elif datasets.stderr and ("dataset does not exist" not in datasets.stderr):
        log_and_print(arguments.verbosity_level,"critical", datasets.stderr, backupjob_log_file)
        return 1,""
    else:
        dataset_list = datasets.stdout.splitlines()[1:] #Remove 1st item from list, since it is the root backupset for the backup job
        if not dataset_list:
            #If dataset_list is empty there are no previous backups, and we will force a full backup

            log_and_print(arguments.verbosity_level,"info","No previous backups. backup_type forced to 'full'", backupjob_log_file)
            backup_type = "full"

        if backup_type == "full":
            new_dataset_name = root_dataset_name +'/' + time_now + "_full"
            new_dataset = subprocess.run(['zfs', 'create','-p', new_dataset_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if new_dataset.stderr:
                log_and_print(arguments.verbosity_level,"critical",new_dataset.stderr, backupjob_log_file)
                return 1,""
            else:
                log_and_print(arguments.verbosity_level,"info", "New dataset created successfuly: " + new_dataset_name, backupjob_log_file)
                return 0,new_dataset_name

        elif backup_type == "diff":
            #Make a new list with only the full backups
            dataset_list_full = []
            for dataset in dataset_list:
                bkp_type = dataset.split("_")
                if bkp_type[-1] == "full":
                    dataset_list_full.append(dataset)

            dataset_list_full.sort()
            last_full_backup = dataset_list_full[-1]
            returncode, new_dataset_name = snap_and_clone_dataset(last_full_backup,backup_type)
            if returncode:
                log_and_print(arguments.verbosity_level,"critical", "Error while snapshoting and cloning dataset", backupjob_log_file)
                return returncode,""
            else:
                log_and_print(arguments.verbosity_level,"info", "Snaphot and clone successful", backupjob_log_file)
                return returncode,new_dataset_name

        elif backup_type == "inc":
            last_backup = dataset_list[-1]
            returncode,new_dataset_name = snap_and_clone_dataset(last_backup,backup_type)
            if returncode:
                log_and_print(arguments.verbosity_level,"critical", "Error while snapshoting and cloning dataset", backupjob_log_file)
                return returncode,""
            else:
                log_and_print(arguments.verbosity_level,"info", "Snaphot and clone successful", backupjob_log_file)
                return returncode,new_dataset_name

        else:
            print ("You should not have been able to get here")
            log_and_print(arguments.verbosity_level,"critical", "Backup type does not have a valid value", backupjob_log_file)
            sys.exit(EXIT_CRITICAL)

def snap_and_clone_dataset(dataset_name,backup_type):
    """
    This function creates a ZFS snapshot of specified dataset and makes a clone
    of that snapshot. The function returns a return code and the name of the new dataset.
    Return code = 0 = OK
    Return code = 1 = Fail
    The function takes two parameters: dataset_name and backup_type

    Parameters
    ----------
    dataset_name :  Should be the ZFS name for the dataset to be snapshotted and
                    cloned. Will be on the form "backup/jobname/datetime_backuptype"
    backup_type :   Should be either 'diff' or 'inc'. Will be appended to the name
                    of the new dataset to mark what type of backup it contains

    """

    log_and_print(arguments.verbosity_level,"info", "snap_and_clone_dataset function invoked with parameters:", backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "dataset_name = "+dataset_name, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "backup_type = "+backup_type, backupjob_log_file)

    root_dataset_name = dataset_name.split("/")[0]
    root_backup_dataset_name = dataset_name.split("/")[0]+"/"+dataset_name.split("/")[1]

    snapshot_name = dataset_name +'@' + time_now + "_snap"
    clone_name = root_backup_dataset_name + '/' + time_now + "_" +backup_type

    #Take snapshot
    snap = subprocess.run(['zfs', 'snapshot', snapshot_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if snap.stderr:
        log_and_print(arguments.verbosity_level,"critical", snap.stderr, backupjob_log_file)
        return 1,""
    else:
        log_and_print(arguments.verbosity_level,"info", "Snapshot created: " +snapshot_name, backupjob_log_file)
    #Make clone
    clone = subprocess.run(['zfs', 'clone', snapshot_name, clone_name],encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if clone.stderr:
        log_and_print(arguments.verbosity_level,"critical", clone.stderr, backupjob_log_file)
        return 1,""
    else:
        log_and_print(arguments.verbosity_level,"info", "Clone created: " +clone_name, backupjob_log_file)
        return 0,clone_name


def initiate_client(client, username,lv_path,lv_suffix):
    """
    This function initiates a backup on a client by calling 'client_backup.py' on
    the client.
    The function takes four parameters: client, username, lv_path, and lv_suffix

    Parameters
    ----------
    client :        Hostname or IP address of the client where we want to initiate
                    a backup
    username :      The username that we will be used to connect to the client.
    lv_path:             The path to the logical volume to be snapshotted
    lv_suffix:      Suffix to add to the snapshot name

    """

    log_and_print(arguments.verbosity_level,"info", "initiate_client function invoked with parameters:", backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "client= "+client, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "username = "+username, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "lv_path = "+lv_path, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "lv_suffix = "+lv_suffix, backupjob_log_file)

    try:
        ssh = SSHClient()
        ssh.load_system_host_keys()
        log_and_print(arguments.verbosity_level,"info", "Connecting to '"+client+"' via SSH as user '"+username+"'",backupjob_log_file)
        ssh.connect(client, username = username)
    except Exception as e:

        log_and_print(arguments.verbosity_level,"critical", "Unable to connect to client '"+client+"' via SSH",backupjob_log_file)
        log_and_print(arguments.verbosity_level,"critical", str(e),backupjob_log_file)
        delete_lockfile(lock_file)
        ssh.close()
        sys.exit(EXIT_CRITICAL)

    try:
        (ssh_stdin, ssh_stdout, ssh_stderr) = ssh.exec_command("/opt/zfsync/client_backup.py initiate-backup -l " + lv_path + " -s "+lv_suffix +" -v "+str(arguments.verbosity_level))
        stdout = ssh_stdout.readlines()
        stderr = ssh_stderr.readlines()
        exit_code = ssh_stdout.channel.recv_exit_status()
        if exit_code:


            log_and_print(arguments.verbosity_level,"critical","Stderr output:" +str(stderr), backupjob_log_file)
            log_and_print(arguments.verbosity_level,"critical","Stdout output:" +str(stdout), backupjob_log_file)
        else:
            log_and_print(arguments.verbosity_level,"info",str(stdout),backupjob_log_file)
        ssh.close()
        return (stdout, stderr, exit_code)

    except Exception as e:

        log_and_print(arguments.verbosity_level,"Critical","Unable to initiate client", backupjob_log_file)
        log_and_print(arguments.verbosity_level,"critical", str(e), backupjob_log_file)
        ssh.close()
        delete_lockfile(lock_file)
        sys.exit(EXIT_CRITICAL)


def end_client(client, username,lv_path,lv_suffix):

    """
    This function ends a backup on a client by calling 'client_backup.py' on
    the client. 'client_backup.py' then unmounts the specified snapshot and deletes it on the client
    The function takes four parameters: client, username, lv_path, and lv_suffix

    Parameters
    ----------
    client :        Hostname or IP address of the client where we want to initiate
                    a backup
    username :      The username that we will be used to connect to the client.
    lv_path:        The path to the logical volume snapshot
    lv_suffix:      Suffix of the snapshot

    """

    log_and_print(arguments.verbosity_level,"info", "end_client function invoked with parameters:", backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "client= "+client, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "username = "+username, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "lv_path = "+lv_path, backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "lv_suffix = "+lv_suffix, backupjob_log_file)

    try:
        ssh = SSHClient()
        ssh.load_system_host_keys()
        log_and_print(arguments.verbosity_level,"info", "Connecting to '"+client+"' via SSH as user '"+username+"'",backupjob_log_file)
        ssh.connect(client, username = username)
    except Exception as e:

        log_and_print(arguments.verbosity_level,"critical", "Unable to connect to client '"+client+"' via SSH",backupjob_log_file)
        log_and_print(arguments.verbosity_level,"critical", str(e),backupjob_log_file)
        delete_lockfile(lock_file)
        ssh.close()
        sys.exit(EXIT_CRITICAL)

    try:
        (ssh_stdin, ssh_stdout, ssh_stderr) = ssh.exec_command("/opt/zfsync/client_backup.py end-backup -l " + lv_path + " -s "+lv_suffix+" -v "+str(arguments.verbosity_level))
        stdout = ssh_stdout.readlines()
        stderr = ssh_stderr.readlines()
        exit_code = ssh_stdout.channel.recv_exit_status()
        if exit_code:

            log_and_print(arguments.verbosity_level,"critical", str(stderr), backupjob_log_file)
        else:
            log_and_print(arguments.verbosity_level,"info",str(stdout),backupjob_log_file)
        ssh.close()
        return (stdout, stderr, exit_code)

    except Exepction as e:

        log_and_print(arguments.verbosity_level,"critical", str(e), backupjob_log_file)
        ssh.close()
        delete_lockfile(lock_file)
        sys.exit(EXIT_CRITICAL)


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

    log_and_print(arguments.verbosity_level,"info", "check_last_backup_status function invoked with parameters:", backupjob_log_file)
    log_and_print(arguments.verbosity_level,"info", "root_dataset_name= "+root_dataset_name, backupjob_log_file)

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
