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
LOG_FILE_PATH = "/etc/zfsync/client_backup.log"

arg_parser = argparse.ArgumentParser(description='Client side script for initializing and ending backups.')
arg_parser.add_argument("action", choices=['initiate-backup', 'end-backup'], help="Specify weather to initiate or end backup")
arg_parser.add_argument('-l','--lv-path', help='Path to the logical volume', required=True)
arg_parser.add_argument('-s','--snap-suffix', help='The name suffix of snaphot to be created (if initiating), or deleted (if ending)', required=True)
arguments = arg_parser.parse_args()

def main():
    if check_lockfile(LOCK_FILE_PATH):
        write_to_log("critical","Client lock file is already present. Exiting!",LOG_FILE_PATH)
        print("Client lock file is already present. Exiting!")
        sys.exit(EXIT_WARNING)
    else:
        create_lockfile(LOCK_FILE_PATH)
        if arguments.action == "initiate-backup":
            create_lvm_snapshot(arguments.lv_path, arguments.snap_suffix)
            delete_lockfile(LOCK_FILE_PATH)
            sys.exit(EXIT_OK)
        elif arguments.action == "end-backup":
            if delete_lv_snapshot(arguments.lv_path, arguments.snap_suffix):
                delete_lockfile(LOCK_FILE_PATH)
                sys.exit(EXIT_WARNING)
            else:
                delete_lockfile(LOCK_FILE_PATH)
                sys.exit(EXIT_OK)
        else:
            print("CRITICAL! You are not supposed to be able to end up here")
            write_to_log("critical","This should not be possible!",LOG_FILE_PATH)
            delete_lockfile(LOCK_FILE_PATH)
            sys.exit(EXIT_CRITICAL)


# Need to add a check of the path to see that it is valid
# Need to add a check to see if there is enough available space in volume group for snapshot
def create_lvm_snapshot(lv_path,snap_suffix):
    """
    This function creates a logical volume snapshot and mounts it. The snapshot
    is mounted in a subfolder of SNAPSHOT_MOUNT_PATH. Before a snapshot is created
    the path is checked to see that it points to an existing block device.
    The function takes two parameters: lv_path and snap_suffix

    Parameters
    ----------
    lv_path :       The path of the logical volume to make snapshot of.
    snap_suffix :   A suffix that will be appended to the name of the snapshot.
                    This will need to be generated on the backup server, and
                    provided as a parameter when the script is called

    """

    #Check that provided path is valid and pointing to a block device
    if not verify_lv_path(lv_path):
        delete_lockfile(LOCK_FILE_PATH)
        write_to_log("critical","Logical volume path is not valid. Exiting!",LOG_FILE_PATH)
        print("Logical volume path is not valid")
        sys.exit(EXIT_CRITICAL)

    #Extract VG and LV portion of lv_path
    vg_name = lv_path.split("/")[2]
    lv_name = lv_path.split("/")[3]

    #Run the command 'vgs %volume group name%'. Check for errors. Extract free space portion of returned output
    space_in_vg = subprocess.run(['vgs', vg_name, '--noheadings', '--units', 'm', '--nosuffix'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if space_in_vg.stderr:
        print(space_in_vg.stderr)
        write_to_log("critical","Error while checking free space in volume group!",LOG_FILE_PATH)
        write_to_log("critical",space_in_vg.stderr,LOG_FILE_PATH)
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)
    space_in_vg = space_in_vg.stdout.split()[6]
    space_in_vg = float(space_in_vg)

    if SNAPSHOT_SIZE >= space_in_vg:
        print("CRITICAL! Not enough free space in volume group. A snapshot will not be created")
        write_to_log("critical","Not enough free space in volume group. A snapshot will not be created",LOG_FILE_PATH)
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)

    create_snap = subprocess.run(['lvcreate', '-L'+str(SNAPSHOT_SIZE)+'M', '-s', '-n', lv_name+snap_suffix, lv_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if create_snap.returncode:
        print("Error while creating snapshot")
        print(create_snap.stderr)
        write_to_log("critical","Error while creating snapshot",LOG_FILE_PATH)
        write_to_log("critical",create_snap.stderr,LOG_FILE_PATH)
        delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)
    else:
        print(create_snap.stdout)
        write_to_log("info",create_snap.stdout,LOG_FILE_PATH)

    make_mnt_dir = subprocess.run(['mkdir','-p',SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix],encoding='utf-8', stderr=subprocess.PIPE)
    if make_mnt_dir.stderr:
        print("CRITICAL! Unable to create directory to mount snapshot")
        write_to_log("critical","Unable to create directory to mount snapshot",LOG_FILE_PATH)
        if delete_lv_snapshot(lv_path,snap_suffix):
            print("Unable to clean up snapshot")
            write_to_log("critical","Unable to clean up snapshot",LOG_FILE_PATH)
            #Leave lock file since cleanup was not successfull
        else:
            print("Cleanup successful")
            write_to_log("info","Clean up of snapshot was successful",LOG_FILE_PATH)
            delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)

    mount_snap = subprocess.run(['mount','/dev/'+vg_name+'/'+lv_name+snap_suffix,SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if mount_snap.returncode:
        print("Error while trying to mount snapshot")
        print(mount_snap.stderr)
        write_to_log("critical","Error while trying to mount snapshot",LOG_FILE_PATH)
        write_to_log("critical",mount_snap.stderr,LOG_FILE_PATH)
        if delete_lv_snapshot(lv_path,snap_suffix):
            print("Unable to clean up")
            write_to_log("critical","Unable to clean up snapshot",LOG_FILE_PATH)
            #Leave lock file since cleanup was not successfull
        else:
            print("Cleanup successful")
            write_to_log("info","Clean up of snapshot was successful",LOG_FILE_PATH)
            delete_lockfile(LOCK_FILE_PATH)
        sys.exit(EXIT_CRITICAL)
    else:
        print(mount_snap.stdout)
        print("Snapshot mounted successfully!")
        write_to_log("info","Snapshot mounted successfully!",LOG_FILE_PATH)

# Need to add a check of the path to see that it is valid
def delete_lv_snapshot(lv_path,snap_suffix):
    """
    This function unmounts a logical volume snapshot, and deletes the snapshot.
    lv_path and snap_suffix are used to find the right mount path and path to
    snapshot. Before a snapshot is deleted the path is checked to see that it
    points to an existing block device.
    The function takes two parameters: lv_path and snap_suffix
    Returns 0 if any one of the three tasks(unmount, delete mount folder, remove snapshot)
    was completed successfully. Returns 1 if all three failed.

    Parameters
    ----------
    lv_path :       The path to the logical volume that was earlier taken a
                    snapshot of
    snap_suffix :   The suffix that was appended to the name of the snapshot.
                    This was generated on the backup server when the backup was
                    initialized, and provided as a parameter when the script was
                    called
    """

    #Extract VG and LV portion of lv_path
    snapshot_path = lv_path+snap_suffix
    vg_name = lv_path.split("/")[2]
    lv_name = lv_path.split("/")[3]
    snap_mount_path = SNAPSHOT_MOUNT_PATH+"/"+lv_name+snap_suffix
    status = 0

    ##### Unmount snapshot #####
    check_if_mounted = subprocess.run(['grep','-qs',lv_name+snap_suffix,'/proc/mounts'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if not check_if_mounted.returncode:
        proc = subprocess.run(['umount', snap_mount_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode:
            print("Critical! Error during unmounting of snapshot: "+snap_mount_path)
            print(proc.stderr)
            write_to_log("critical","Error during unmounting of snapshot: "+snap_mount_path,LOG_FILE_PATH)
            write_to_log("critical",proc.stderr,LOG_FILE_PATH)
            sys.exit(EXIT_CRITICAL)
        else:
            print("Snapshot unmounted successfully")
            print(proc.stdout)
            write_to_log("info","Snapshot unmounted successfully",LOG_FILE_PATH)
            write_to_log("info",proc.stdout,LOG_FILE_PATH)
    else:
        print("Warning! Specified snapshot is not mounted")
        write_to_log("warning","Specified snapshot is not mounted",LOG_FILE_PATH)
        status += 1

    ##### Delete mount folder #####
    if os.path.isdir(snap_mount_path):
        # Should probably add a check here to verify that we are not deleting something we shouldn't
        proc = subprocess.run(['rm','-rf',snap_mount_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode:
            print("Error while trying to remove snapshot mount folder: "+snap_mount_path)
            print(proc.stderr)
            write_to_log("critical","Error while trying to remove snapshot mount folder: "+snap_mount_path,LOG_FILE_PATH)
            write_to_log("critical",proc.stderr,LOG_FILE_PATH)
            sys.exit(EXIT_CRITICAL)
        else:
            print(proc.stdout)
            print("Snapshot mount folder deleted successfully!")
            write_to_log("info","Snapshot mount folder deleted successfully!",LOG_FILE_PATH)
            write_to_log("info",proc.stdout,LOG_FILE_PATH)
    else:
        print("Warning! Snapshot mount path was not found: "+snap_mount_path)
        write_to_log("warning","Snapshot mount path was not found: "+snap_mount_path,LOG_FILE_PATH)
        status += 1

    ##### Remove snapshot #####
    if verify_lv_path(snapshot_path):
        proc = subprocess.run(['lvremove', '-y', snapshot_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode:
            print("Error while trying to remove logical volume snapshot: "+snapshot_path)
            print(proc.stderr)
            write_to_log("critical","Error while trying to remove logical volume snapshot: "+snapshot_path,LOG_FILE_PATH)
            write_to_log("critical",proc.stderr,LOG_FILE_PATH)
            sys.exit(EXIT_CRITICAL)
        else:
            print("Logical volume snapshot removed successfully!")
            print(proc.stdout)
            write_to_log("info","Logical volume snapshot removed successfully!",LOG_FILE_PATH)
            write_to_log("info",proc.stdout,LOG_FILE_PATH)
    else:
        print("Warning! Could not find the specified snapshot: "+lv_path+snap_suffix)
        write_to_log("warning","Could not find the specified snapshot: "+lv_path+snap_suffix,LOG_FILE_PATH)
        status += 1

    #Check how many of the tasks failed
    if status < 3:
        return 0 #0 = OK
    else:
        return 1 #1 = Not OK




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
            write_to_log("info","The path exists and is a block device: "+lv_path,LOG_FILE_PATH)
            return True
        else:
            print("The path exists but is not pointing to a logical volume")
            write_to_log("critical","The path exists but is not pointing to a logical volume: "+lv_path,LOG_FILE_PATH)
            return False
    else:
        print("The path does not exist:")
        print(lv_path)
        write_to_log("critical","The path does not exist: "+lv_path,LOG_FILE_PATH)
        return False


if __name__ == "__main__":
    main()
