#! /usr/bin/env python3.6

import subprocess, sys, argparse
from pathlib import Path
from datetime import datetime

SNAPSHOT_SIZE = 512 #In megabytes
#This name must be provided by the backupserver, so that it is the same
#both when initializing and ending the backup.
SNAPSHOT_MOUNT_PATH = "/mnt/rsyncbackup"
LOCK_FILE_NAME = "lock"
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

arg_parser = argparse.ArgumentParser(description='Client side script for initializing and ending backups.')
arg_parser.add_argument("action", choices=['initiate-backup', 'end-backup'], help="Specify weather to initiate or end backup",group="Action")
arg_parser.add_argument('-l','--lv-path', help='Path to the logical volume', required=True, group="Required arguments")
arg_parser.add_argument('-s','--snap-suffix', help='The name suffix of snaphot to be created (if initiating), or deleted (if ending)', required=True, group="Required arguments")
arguments = arg_parser.parse_args()

def main():
    print(arguments)
    if checkLockFile(LOCK_FILE_NAME):
        print("Client lock file is already present. Exiting!")
        sys.exit(EXIT_WARNING)
    else:
        createLockfile(LOCK_FILE_NAME)
        if arguments.action == "initiate-backup":
            createLvmSnapshot(arguments.lv_path, arguments.snap_suffix)
            deleteLockfile(LOCK_FILE_NAME)
            sys.exit(EXIT_OK)
        elif arguments.action == "end-backup":
            deleteLvmSnapshot(arguments.lv_path, arguments.snap_suffix)
            deleteLockfile(LOCK_FILE_NAME)
            sys.exit(EXIT_OK)
        else:
            print("You are not supposed to be able to end up here")
            deleteLockfile(LOCK_FILE_NAME)
            sys.exit(EXIT_CRITICAL)



# Need to add a check of the path to see that it is valid
# Need to add a check to see if there is enough available space in volume group for snapshot
def createLvmSnapshot(lvm_path,snap_suffix):
    """
    This function creates an LVM snapshot and mounts it. The snapshot is mounted
    in a subfolder of SNAPSHOT_MOUNT_PATH.
    The function takes two parameters: lvm_path and snap_suffix

    Parameters
    ----------
    lvm_path :      The path to the logical volume to make snapshot of
    snap_suffix :   A suffix that will be appended to the name of the snapshot.
                    This will need to be generated on the backup server, and
                    provided as a parameter when the script is called

    """

    #Check that the lvm_path starts with a /
    if lvm_path[0] != "/":
        print("Logical volume path is invalid. Does not start with a /")
        deleteLockfile(LOCK_FILE_NAME)
        sys.exit(EXIT_CRITICAL)
    #Extract VG and LV portion of lvm_path
    vg_name = lvm_path.split("/")[2]
    lv_name = lvm_path.split("/")[3]

    #Run the command 'vgs %volume group name%'. Check for errors. Extract free space portion of returned output
    space_in_vg = subprocess.run(['vgs', vg_name, '--noheadings', '--units', 'm', '--nosuffix'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if space_in_vg.stderr:
        print(space_in_vg.stderr)
        deleteLockfile(LOCK_FILE_NAME)
        sys.exit(EXIT_CRITICAL)
    space_in_vg = space_in_vg.stdout.split()[6]
    space_in_vg = float(space_in_vg)

    if SNAPSHOT_SIZE >= space_in_vg:
        print("Critical: Not enough free space in volume group. A snapshot will not be created")
        #Add logging
        deleteLockfile(LOCK_FILE_NAME)
        sys.exit(EXIT_CRITICAL)

    create_snap = subprocess.run(['lvcreate', '-L'+str(SNAPSHOT_SIZE)+'M', '-s', '-n', lv_name+snap_suffix, lvm_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if create_snap.returncode:
        print(create_snap.stderr)
        deleteLockfile(LOCK_FILE_NAME)
        sys.exit(EXIT_CRITICAL)
    else:
        print(create_snap.stdout)

    make_mnt_dir = subprocess.run(['mkdir','-p',SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix],encoding='utf-8', stderr=subprocess.PIPE)
    if make_mnt_dir.stderr:
        print("Unable to create directory to mount snapshot")
        sys.exit(EXIT_CRITICAL)

    mount_snap = subprocess.run(['mount','/dev/'+vg_name+'/'+lv_name+snap_suffix,SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if mount_snap.returncode:
        print(mount_snap.stderr)
        #Add deleteLockfile(file_name). Or maybe not, since a crash here will leave an uncleaned snapshot?
        sys.exit(EXIT_CRITICAL)
    else:
        print(mount_snap.stdout)
        print("Snapshot mounted successfully!")

# Need to add a check of the path to see that it is valid
def deleteLvmSnapshot(lvm_path,snap_suffix):
    """
    This function unmounts a logical volume snapshot, and deletes the snapshot.
    lvm_path and snap_suffix are used to find the right mount path and path to
    snapshot.
    The function takes two parameters: lvm_path and snap_suffix

    Parameters
    ----------
    lvm_path :      The path to the logical volume that was earlier taken a
                    snapshot of
    snap_suffix :   The suffix that was appended to the name of the snapshot.
                    This was generated on the backup server when the backup was
                    initialized, and provided as a parameter when the script was
                    called
    """

    #Is this sufficient validation?
    if snapshot_path[0] != "/":
        print("Snapshot path is invalid. Does not start with a /")
        sys.exit(EXIT_CRITICAL)
    #Extract VG and LV portion of lvm_path
    vg_name = lvm_path.split("/")[2]
    lv_name = lvm_path.split("/")[3]

    proc = subprocess.run(['umount', '/mnt/snap'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

def deleteLockfile(file_name):
    proc = subprocess.run(['rm', '-f', '/etc/zfsync/'+file_name], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_UNKNOWN)
    else:
        print("Lock file deleted")

def createLockfile(file_name):
    proc = subprocess.run(['touch', '/etc/zfsync/'+file_name], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_WARNING)
    else:
        print("Lock file created")

def checkLockFile(file_name):
    lockfile = Path("/etc/zfsync/" + file_name)
    if lockfile.exists():
        lock_exists = True
    else:
        lock_exists = False
    return lock_exists


if __name__ == "__main__":
    main()
