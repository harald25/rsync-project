import sys, subprocess, os
from datetime import datetime
(EXIT_OK, EXIT_WARNING, EXIT_CRITICAL, EXIT_UNKNOWN) = (0,1,2,3)

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

def log_and_print(verbosity,level, message, log_file):
    """
    Writes message and its verbosity level to specified log file, and prints them.
    Timestamp is automatically added to log file.
    Specified verbosity level decides what gets logged and printed

    Parameters
    ----------
    level :     Verbosity level: info, warning, critical
    message :   The message to log
    log_file :  Path to, and name of the log file to write to
    verbosity:  Level of verbosity to print/log. 0 = no logging/printing, 1 = errors,
                2 = warning + error, 3 = warning+error+info

    """

    if level == "critical":
        level_as_int = 1
    elif level == "warning":
        level_as_int = 2
    elif level == "info":
        level_as_int = 3
    else:
        level_as_int = 0

    if verbosity >= level_as_int:
        date_now = datetime.today().strftime('%Y-%m-%d')
        datetime_now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_file, "a+", encoding="utf-8") as log:
            log.write(datetime_now + " - " + level + " - " + message + "\n")
        log.close()
        print(level,"-",message)
        if level_as_int == 0:
            print("The level specified is not valid. Message will be logged, but this should be fixed")
            log.write(datetime_now + "The level specified is not valid. Message will be logged, but this should be fixed \n")
