#! /usr/bin/env python3.6

import subprocess, sys
from pathlib import Path
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

def main():
    if len(sys.argv) is 2:
        if sys.argv(1) is "--initiate_backup":
            message, lock_exists = checkLockFile()
            if lock_exists:
                print("Run cleanup")
                # Call up cleaning function
            else:
                subprocess.run(['thouch', '/etc/zfsync/lock'])
                createLvmSnapshot("/dev/centos/root")
                print("LVM snapshot successfully created")
                sys.exit(EXIT_OK)

        elif sys.argv(1) is "--end_backup":
            deleteLvmSnapshot("/dev/centos/snap")
            subprocess.run(['rm', '-rf', '/etc/zfsync/lock'])
            print("LVM snapshot successfully deleted")
            sys.exit(EXIT_OK)

        elif sys.argv(1) is "--cleanup":
            print("Cleaning is not implemented yet")
            sys.exit(EXIT_OK)

        else:
            print("Parameter provided to the script was not accepted")
            sys.exit(EXIT_UNKNOWN)

    if:
        print("Wrong number of arguments. There should only be one. Accepted arguments are \"--initiate_backup\", \"--cleanup\", and \"--end_backup\" ")
        sys.exit(EXIT_UNKNOWN)

# Need to add a check of the path to see that it is valid
# Need to add a check to see if there is enough available space in volume group for snapshot
def createLvmSnapshot(lvm_path):
    subprocess.run(['lvcreate', '-L512MB', '-s', '-n' 'snap', lvm_path])
    subprocess.run(['mount','/dev/centos/snap','/mnt/snap'])

# Need to add a check of the path to see that it is valid
def deleteLvmSnapshot(snapshot_path):
    subprocess.run(['umount', '/mnt/snap'])
    subprocess.run(['lvremove', '-y', snapshot_path])

def checkLockFile():
    lockfile = Path("/etc/zfsync/lock")
    if lockfile.exists():
        message = "Lock file exists. Backup running or previous backup exited with error!"
        lock_exists = True
    else:
        message = "Lock file does not exist. Ready for backup!"
        lock_exists = False
    return (message, lock_exists)

if __name__ == "__main__":
    main()
