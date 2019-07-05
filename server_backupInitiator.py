#! /usr/bin/env python3.6

from datetime import datetime
import subprocess
import os.path

central_config_file_path = "/etc/zfsync/zfsync.cfg"
machines_with_jobs = "/etc/zfsync/machines.cfg"

def main():
    checkSchedule()

def checkSchedule():
    # Open machines.cfg and make a matrix of backup jobs
    # Run through matrix and start backup on the machines where it is time for backup (after checking for a lock file)
    print("Schedule")

def check_lockfile1():
    # Check if lock file exists for given backup job
        # If lock file exists: check PID
            # If PID exists: return ("Backup running")
            # Else if PID does not exist: return ("Cleanup needed")
        # Else if lock file does not exist: return("OK. Ready for backup")
    print("Lock 1")


if __name__ == "__main__":
    main()
