# rsync-project
Linux backup system utilizing rsync, LVM, and ZFS. The backup scripts are written in Python 3.6.7 and tested on Centos7.  
Will be tested on RHEL 6 and 7 for clients. Testing other OSes for the server is not currently planned.

## Dependencies:

Python dependencies are needed on both clients and server

    yum install -y https://centos7.iuscommunity.org/ius-release.rpm
    yum install -y python36u python36u-libs python36u-devel python36u-pip
