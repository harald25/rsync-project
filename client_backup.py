#! /usr/bin/env python3.6

import subprocess

def main():
    createLvmSnapshot("/dev/centos/root")
    deleteLvmSnapshot("/dev/centos/snap")

# Need to add a check of the path to see that it is valid
def createLvmSnapshot(lvm_path):
    subprocess.run(['lvcreate', '-L512MB', '-s', '-n' 'snap', lvm_path])
    subprocess.run(['mount','/dev/centos/snap','/mnt/snap'])

# Need to add a check of the path to see that it is valid
def deleteLvmSnapshot(snapshot_path):
    subprocess.run(['umount', '/mnt/snap'])
    subprocess.run(['lvremove', '-y', snapshot_path])


if __name__ == "__main__":
    main()
