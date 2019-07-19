# rsync-project
Linux backup system utilizing rsync, LVM, and ZFS. The backup scripts are written in Python 3.6.7 and tested on Centos7.  
Will be tested on RHEL 6 and 7 for clients. Testing other OSes for the server is not currently planned.

## Dependencies:

###### Dependencies for both client and server

    yum install -y https://centos7.iuscommunity.org/ius-release.rpm
    yum install -y python36u python36u-libs python36u-devel python36u-pip

###### Server dependencies

    /usr/bin/pip3.6 install paramiko
    zfs must be installed and configured, and a backup set called "backup" must be created

## Preparations:
SSH keys must be generated on backup server and copied to clients

    ssh-keygen
    ssh-copy-id root@clientmachine


## To-do:
* Implement check of wether last backup was successful or not
* Finish server_backupExecutor.py
* Finish server_backupInitiator.py
* Make install script to set up a fresh backup server
* Document set-up of the ZFS server
* Test on other Linux distributions
* Configure to work with selinux
