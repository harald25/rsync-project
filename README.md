# rsync-project
Linux backup system utilizing rsync, LVM, and ZFS. The backup scripts are written in Python 3.6.7 and tested on Centos7.  
Will be tested on RHEL 6 and 7 for clients. Testing other OSes for the server is not currently planned.

## Dependencies:

###### Centos7 clients/agents

    yum install -y https://centos7.iuscommunity.org/ius-release.rpm
    yum install -y python36u python36u-libs python36u-devel python36u-pip

###### Oracle Linux 6 clients/agents:
    yum install oracle-softwarecollection-release-el6
    yum -y install yum-utils
    yum-config-manager --enable ol6_software_collections
    yum install rh-python36 scl-utils
    scl enable rh-python36 bash

###### Server dependencies

    /usr/bin/pip3.6 install paramiko
    zfs must be installed and configured, and a backup set called "backup" must be created
    It is recommended to enable compression on the zpool level in zfs, so that all subsequently created datasets have compression enabled

## Preparations:
SSH keys must be generated on backup server and copied to clients

    ssh-keygen
    ssh-copy-id root@clientmachine

The shebang must be edited to reflect the python binary installed on your system


## To-do:
* Implement check of wether last backup was successful or not
* Finish server_backupExecutor.py
* Finish server_backupInitiator.py
* Make install script to set up a fresh backup server
* Document set-up of the ZFS server
* Test on other Linux distributions
* Configure to work with selinux
