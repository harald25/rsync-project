import sys, subprocess, os

def deleteLockfile(lock_file_path):
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

def createLockfile(lock_file_path):
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

def checkLockFile(lock_file_path):
    """
    This function checks for the presense of a lock file and returns true or false
    The function takes one parameter: lock_file_path

    Parameters
    ----------
    lock_file_path :     The path of the lock file to be checked

    """
    return os.path.isfile(lock_file_path)
