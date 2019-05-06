#! /usr/bin/env python3.6

import subprocess, sys
from pathlib import Path
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

def main():
    if len(sys.argv) is 2:
        if sys.argv[1] == "--initiate-backup":
            message, lock_exists = checkLockFile()
            if lock_exists:
                print("Client lock file is already present. Exiting!")
            else:
                proc = subprocess.run(['touch', '/etc/zfsync/lock'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if proc.returncode:
                    print(proc.stderr)
                    sys.exit(EXIT_WARNING)
                else:
                    print(proc.stdout)
                    createLvmSnapshot("/dev/centos/root")
                    sys.exit(EXIT_OK)

        elif sys.argv[1] == "--end-backup":
            deleteLvmSnapshot("/dev/centos/snap")
            proc = subprocess.run(['rm', '-f', '/etc/zfsync/lock'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode:
                print(proc.stderr)
                sys.exit(EXIT_UNKNOWN)
            else:
                print(proc.stdout)
                sys.exit(EXIT_OK)

        elif sys.argv[1] == "--cleanup":
            if checkLockFile():
                print("Client lock file is present. Backup is already running, or the last backup crashed.")
                sys.exit(EXIT_WARNING)
            else:
                deleteLvmSnapshot("/dev/centos/snap")
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
    proc = subprocess.run(['lvcreate', '-L512MB', '-s', '-n' 'snap', lvm_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_CRITICAL)
    else:
        print(proc.stdout)

    proc = subprocess.run(['mount','/dev/centos/snap','/mnt/snap'], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_CRITICAL)
    else:
        print(proc.stdout)
        print("Snapshot mounted successfully!")

# Need to add a check of the path to see that it is valid
def deleteLvmSnapshot(snapshot_path):
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
