import sys, subprocess, os

def delete_lockfile(lock_file_path):
    """
    This function deletes a lock file to mark that a backup job process is
    no longer running.
    The function takes one parameter: lock_file_path

    Parameters
    ----------
    lock_file_path :     The path of the lock file to be deleted

    """

    proc = subprocess.run(['rm', '-f', lock_file_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_UNKNOWN)
    else:
        print("Lock file deleted")

def create_lockfile(lock_file_path):
    """
    This function creates a lock file to mark that a backup job process is
    already running.
    The function takes one parameter: lock_file_path

    Parameters
    ----------
    lock_file_path :     The path of the lock file to be created

    """
    proc = subprocess.run(['touch', lock_file_path], encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode:
        print(proc.stderr)
        sys.exit(EXIT_WARNING)
    else:
        print("Lock file created")

def check_lockfile(lock_file_path):
    """
    This function checks for the presense of a lock file and returns true or false
    The function takes one parameter: lock_file_path

    Parameters
    ----------
    lock_file_path :     The path of the lock file to be checked

    """
    return os.path.isfile(lock_file_path)

def write_to_log(level, message, root_dataset_name):
    """
    Writes message and its verbosity level to log file. Timestamp is automatically added
    Log files are stored at the root dataset for the running backup job.

    Parameters
    ----------
    level :             Verbosity level: info, warning, critical
    message :           The message to log
    root_dataset_name : The name of the current dataset. Used to determine the log
                        file location

    """
    date_now = datetime.today().strftime('%Y-%m-%d')
    datetime_now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    with open("/"+root_dataset_name+"/"+time_now+".log", "a+", encoding="utf-8") as log:
        log.write(datetime_now + " - " + level + " - " + message)
    log.closed()
