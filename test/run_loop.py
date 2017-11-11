#!/usr/bin/env python3
import subprocess, time

def run_command(cmd):
    """Execute a shell command and return the output
       :param command: the command to be run and all of the arguments
       :returns: success_boolean, command_string, stdout, stderr
    """

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in iter(proc.stdout.readline, b''):
        print(">>> " + line.rstrip())
    (stdout, stderr) = proc.communicate()
    return proc.returncode == 0, proc 

def main():
    cmd = "bash travis.sh -n"
    for i in range(0, 50):
        print("Running loop {}".format(i+1))
        success, proc = run_command(cmd)
        if not success:
            for line in iter(proc.stderr.readline, b''):
                print(("--- " + line.rstrip()))
            break
        else:
            print("...........................Success\n")
        time.sleep(30)

if __name__ == "__main__":
        main()
