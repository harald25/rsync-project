#! /usr/bin/env python3.6

from datetime import datetime
import subprocess
import os.path

def main():

    createClone("backup/backup-ipsec","5_inc","incremental")


def createClone(main_dataset_name, sub_dataset_name, backup_type):
    time_now = datetime.today().strftime('%Y-%m-%d--%H-%M-%S')
    #Take snapshot
    subprocess.run(['zfs', 'snapshot', main_dataset_name + '/' + sub_dataset_name +'@' + time_now])
    #Make clone
    subprocess.run(['zfs', 'clone', main_dataset_name + '/' + sub_dataset_name +'@' + time_now, main_dataset_name + '/' + backup_type + '_' + time_now ])

if __name__ == "__main__":
    main()
