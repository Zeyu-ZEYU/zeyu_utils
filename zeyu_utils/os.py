import subprocess
import sys


def run_cmd(cmd, failure_warning=True):
    try:
        return subprocess.check_output(cmd, shell=True, universal_newlines=True).strip("\n")
    except subprocess.CalledProcessError as e:
        if failure_warning:
            print("WARNING:", e, file=sys.stderr)
        return None
