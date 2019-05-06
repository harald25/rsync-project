#! /usr/bin/env python3.6

import subprocess, sys
from pathlib import Path
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

def deleteLockfile(file_name):
    proc = subprocess.run(['rm', '-f', '/etc/zfsync/',file_name], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_UNKNOWN)
    else:
        print("Lock file deleted")

def createLockfile(file_name):
    proc = subprocess.run(['touch', '/etc/zfsync/',file_name], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
