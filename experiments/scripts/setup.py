import subprocess
import sys
import os

SETUP_SCRIPT_PATH = "/proj/ove-PG0/murray/pringles/setup.sh"

# --- Setup and Compile Functions (Remain unchanged from previous submission) ---
def run_setup_script(ip, setup_script_path, ssh_user):
    """Runs the specified setup script on a remote machine synchronously (blocking run)."""
    command = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no', 
        '-o', 'UserKnownHostsFile=/dev/null', 
        f'{ssh_user}@{ip}',
        setup_script_path
    ]
    print(f"Running setup on {ip}: {setup_script_path}")
    try:
        subprocess.run(command, check=True, stdin=subprocess.DEVNULL)
        print(f"Setup on {ip} succeeded.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Setup script on {ip} failed with exit code {e.returncode}.")
        return False
    except Exception as e:
        print(f"ERROR connecting to {ip} for setup: {e}")
        return False

num_machines = 7 # make input variable! add to toml :p
ssh_user = "murray22"
for i in range(2, num_machines+2):
    ip_addr = '10.10.1.' + str(i)
    run_setup_script(ip_addr, SETUP_SCRIPT_PATH, ssh_user) 

