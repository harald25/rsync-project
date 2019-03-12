#! /usr/bin/env python3.6

from datetime import datetime
import subprocess

def main():
    createClone("backup/backup-ipsec/","5_inc","incremental")


def createClone(main_dataset_name, sub_dataset_name, backup_type):
    time_now = datetime.today().strftime('%Y-%m-%d--%H-%M-%S')
    #Take snapshot
    subprocess.run("zfs", "snapshot", main_dataset_name +"@" + time_now)
    #Make clone
    subprocess.run("zfs", "clone", main_dataset_name +"@" + time_now, main_dataset_name + "_" + backup_type + "_" + time_now )

def readSchedule():
    print("Schedule")

def checkLockFile1():
    print("Lock 1")

def checkLockFile2():
    print("Lock 2")

if __name__ == "__main__":
    main()
