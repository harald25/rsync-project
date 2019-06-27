#! /usr/bin/env python3.6

import subprocess, sys
from pathlib import Path
from datetime import datetime

SNAPSHOT_SIZE = 512 #In megabytes
SNAPSHOT_NAME_SUFFIX = "_rsync-snapshot_"+datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
SNAPSHOT_MOUNT_PATH = "/mnt/rsyncbackup"
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

def main():
    if len(sys.argv) is 2:
        if sys.argv[1] == "--initiate-backup":
            if checkLockFile("lock"):
                print("Client lock file is already present. Exiting!")
                sys.exit(EXIT_WARNING)
            else:
                createLockfile("lock")
                createLvmSnapshot("/dev/centos/root")
                deleteLockfile("lock")
                sys.exit(EXIT_OK)

        elif sys.argv[1] == "--end-backup":
            if checkLockFile():
                print("Client lock file is already present. Exiting!")
                sys.exit(EXIT_WARNING)
            else:
                createLockfile("lock")
                deleteLvmSnapshot("/dev/centos/snap")
                deleteLockfile("lock")
                sys.exit(EXIT_OK)

        else:
            print("Parameter provided to the script was not accepted. Accepted parameters are \"--initiate-backup\", and \"--cleanup\"")
            sys.exit(EXIT_UNKNOWN)

    else:
        print("Wrong number of arguments. There should be exactly one argument. Accepted arguments are \"--initiate-backup\", and \"--cleanup\" ")
        sys.exit(EXIT_UNKNOWN)

# Need to add a check of the path to see that it is valid
# Need to add a check to see if there is enough available space in volume group for snapshot
def createLvmSnapshot(lvm_path):
    #Check that the lvm_path starts with a /
    if lvm_path[0] != "/":
        print("Logical volume path is invalid. Does not start with a /")
        sys.exit(EXIT_CRITICAL)
    #Extract VG and LV portion of lvm_path
    vg_name = lvm_path.split("/")[2]
    lv_name = lvm_path.split("/")[3]

    #Run the command 'vgs %volume group name%'. Check for errors. Extract free space portion of returned output
    space_in_vg = subprocess.run(['vgs', vg_name, '--noheadings', '--units', 'm', '--nosuffix'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if space_in_vg.stderr:
        print(space_in_vg.stderr)
        #Add deleteLockfile(file_name)
        sys.exit(EXIT_CRITICAL)
    space_in_vg = space_in_vg.stdout.split()[6]
    space_in_vg = float(space_in_vg)

    if SNAPSHOT_SIZE >= space_in_vg:
        print("Critical: Not enough free space in volume group. A snapshot will not be created")
        #Add logging
        #Add deleteLockfile(file_name)
        sys.exit(EXIT_CRITICAL)

    proc = subprocess.run(['lvcreate', '-L'+str(SNAPSHOT_SIZE)+'M', '-s', '-n', lv_name+SNAPSHOT_NAME_SUFFIX, lvm_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        #Add deleteLockfile(file_name)
        sys.exit(EXIT_CRITICAL)
    else:
        print(proc.stdout)

    rsync_dir = subprocess.run(['mkdir','-p',SNAPSHOT_MOUNT_PATH])
    proc = subprocess.run(['mount','/dev/'+vg_name+'/'+lv_name+SNAPSHOT_NAME_SUFFIX,SNAPSHOT_MOUNT_PATH], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        #Add deleteLockfile(file_name)
        sys.exit(EXIT_CRITICAL)
    else:
        print(proc.stdout)
        print("Snapshot mounted successfully!")

# Need to add a check of the path to see that it is valid
def deleteLvmSnapshot(snapshot_path):
    #Is this sufficient validation?
    if snapshot_path[0] != "/":
        print("Snapshot path is invalid. Does not start with a /")
        sys.exit(EXIT_CRITICAL)

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
