#! /usr/bin/env python3.6

import subprocess, sys
from pathlib import Path
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

def main():
    if len(sys.argv) is 2:
        if sys.argv[1] == "--initiate-backup":
            message, lock_exists = checkLockFile()
            if lock_exists:
                print("Lock file is present. Exiting")
            else:
                # Add a fail if creating lock file fails
                if subprocess.run(['touch', '/etc/zfsync/lock']):
                    createLvmSnapshot("/dev/centos/root")
                    print("LVM snapshot successfully created")
                    sys.exit(EXIT_OK)
                else:
                    print("Failed to create lock file")
                    sys.exit(EXIT_UNKNOWN)

        elif sys.argv[1] == "--end-backup":
            deleteLvmSnapshot("/dev/centos/snap")
            subprocess.run(['rm', '-rf', '/etc/zfsync/lock'])
            print("LVM snapshot successfully deleted")
            sys.exit(EXIT_OK)

        elif sys.argv[1] == "--cleanup":
            print("Cleaning is not implemented yet")
            sys.exit(EXIT_OK)

        else:
            print("Parameter provided to the script was not accepted")
            sys.exit(EXIT_UNKNOWN)

    else:
        print("Wrong number of arguments. There should be exactly one argument. Accepted arguments are \"--initiate-backup\", \"--cleanup\", and \"--end-backup\" ")
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
