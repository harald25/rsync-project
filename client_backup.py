#! /usr/bin/env python3.6

import subprocess, sys, argparse, os, stat
from datetime import datetime
from shared_functions import *

SNAPSHOT_SIZE = 512 #In megabytes
#This name must be provided by the backupserver, so that it is the same
#both when initializing and ending the backup.
SNAPSHOT_MOUNT_PATH = "/mnt/rsyncbackup"#DONT use a trailing slash
LOCK_FILE_PATH = "/etc/zfsync/lock" #Full path to lock file
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

arg_parser = argparse.ArgumentParser(description='Client side script for initializing and ending backups.')
arg_parser.add_argument("action", choices=['initiate-backup', 'end-backup'], help="Specify weather to initiate or end backup")
arg_parser.add_argument('-l','--lv-path', help='Path to the logical volume', required=True)
arg_parser.add_argument('-s','--snap-suffix', help='The name suffix of snaphot to be created (if initiating), or deleted (if ending)', required=True)
arguments = arg_parser.parse_args()

def main():
    if check_lockfile(LOCK_FILE_PATH):
        print("Client lock file is already present. Exiting!")
        sys.exit(EXIT_WARNING)
    else:
        create_lockfile(LOCK_FILE_PATH)
        if arguments.action == "initiate-backup":
            create_lvm_snapshot(arguments.lv_path, arguments.snap_suffix)
            delete_lockfile(LOCK_FILE_PATH)
            sys.exit(EXIT_OK)
        elif arguments.action == "end-backup":
            delete_lv_snapshot(arguments.lv_path, arguments.snap_suffix)
            delete_lockfile(LOCK_FILE_PATH)
            sys.exit(EXIT_OK)
        else:
            print("CRITICAL! You are not supposed to be able to end up here")
            delete_lockfile(LOCK_FILE_PATH)
            sys.exit(EXIT_CRITICAL)



# Need to add a check of the path to see that it is valid
# Need to add a check to see if there is enough available space in volume group for snapshot
def create_lvm_snapshot(lv_path,snap_suffix):
    """
    This function creates an LVM snapshot and mounts it. The snapshot is mounted
    in a subfolder of SNAPSHOT_MOUNT_PATH. Before a snapshot is created the path
    is checked to see that it points to an existing block device.
    The function takes two parameters: lv_path and snap_suffix

    Parameters
    ----------
    lv_path :      The path to the logical volume to make snapshot of
    snap_suffix :   A suffix that will be appended to the name of the snapshot.
                    This will need to be generated on the backup server, and
                    provided as a parameter when the script is called

    """

    #Check that provided path is valid and pointing to a block device
    if not verify_lv_path(lv_path):
        delete_lockfile(LOCK_FILE_PATH)
        print("Logical volume path is not valid")
        sys.exit(EXIT_CRITICAL)

    #Extract VG and LV portion of lv_path
    vg_name = lv_path.split("/")[2]
    lv_name = lv_path.split("/")[3]

    #Run the command 'vgs %volume group name%'. Check for errors. Extract free space portion of returned output
    space_in_vg = subprocess.run(['vgs', vg_name, '--noheadings', '--units', 'm', '--nosuffix'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if space_in_vg.stderr:
        print(space_in_vg.stderr)
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)
    space_in_vg = space_in_vg.stdout.split()[6]
    space_in_vg = float(space_in_vg)

    if SNAPSHOT_SIZE >= space_in_vg:
        print("CRITICAL! Not enough free space in volume group. A snapshot will not be created")
        #Add logging
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)

    create_snap = subprocess.run(['lvcreate', '-L'+str(SNAPSHOT_SIZE)+'M', '-s', '-n', lv_name+snap_suffix, lv_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if create_snap.returncode:
        print("Error while creating snapshot")
        print(create_snap.stderr)
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)
    else:
        print(create_snap.stdout)

    make_mnt_dir = subprocess.run(['mkdir','-p',SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix],encoding='utf-8', stderr=subprocess.PIPE)
    if make_mnt_dir.stderr:
        print("CRITICAL! Unable to create directory to mount snapshot")
        sys.exit(EXIT_CRITICAL)

    mount_snap = subprocess.run(['mount','/dev/'+vg_name+'/'+lv_name+snap_suffix,SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if mount_snap.returncode:
        print(mount_snap.stderr)
        #Add delete_lockfile(file_name). Or maybe not, since a crash here will leave an uncleaned snapshot?
        sys.exit(EXIT_CRITICAL)
    else:
        print(mount_snap.stdout)
        print("Snapshot mounted successfully!")

# Need to add a check of the path to see that it is valid
def delete_lv_snapshot(lv_path,snap_suffix):
    """
    This function unmounts a logical volume snapshot, and deletes the snapshot.
    lv_path and snap_suffix are used to find the right mount path and path to
    snapshot. Before a snapshot is delted the path is checked to see that it
    points to an existing block device.
    The function takes two parameters: lv_path and snap_suffix

    Parameters
    ----------
    lv_path :       The path to the logical volume that was earlier taken a
                    snapshot of
    snap_suffix :   The suffix that was appended to the name of the snapshot.
                    This was generated on the backup server when the backup was
                    initialized, and provided as a parameter when the script was
                    called
    """

    # Verify specified path to snapshot
    if not verify_lv_path(lv_path+snap_suffix):
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)

    #Extract VG and LV portion of lv_path
    snapshot_path = lv_path+snap_suffix
    vg_name = lv_path.split("/")[2]
    lv_name = lv_path.split("/")[3]

    #Check that mount path exists and is a directory
    if not os.path.isdir(SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix):
        print("CRITICAL! The mount path generated based on lv_path and snap_suffix does not exist. Exiting!")
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)

    proc = subprocess.run(['umount', SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_CRITICAL)
    else:
        print(proc.stdout)
        print("Snapshot unmounted successfully!")

    proc = subprocess.run(['lvremove', '-y', snapshot_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_CRITICAL)
    else:
        print(proc.stdout)

def verify_lv_path(lv_path):
    """
    Returns true if path exists and is pointing to a block device. Or else returns
    false.
    The function takes one parameter: lv_path

    Parameters
    ----------
    lv_path :     The path to be verified

    """

    if os.path.exists(lv_path):
        mode = os.stat(lv_path).st_mode
        if stat.S_ISBLK(mode):
            return True
        else:
            print("The path is not pointing to a logical volume")
            return False
    else:
        print("The path to logical volume does not exist:")
        print(lv_path)
        return False


if __name__ == "__main__":
    main()
