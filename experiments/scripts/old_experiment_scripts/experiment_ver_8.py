import toml
import yaml
import uuid
import random
import subprocess
import time
import sys
import os
from datetime import datetime # New import for timestamping

# --- Custom Quoting for YAML Strings ---
class QuotedString(str):
    """Custom string class to force quotes in YAML output."""
    pass

def represent_quoted_string(dumper, data):
    """Representer function to output QuotedString with double quotes."""
    # The 'style='\"'' argument forces PyYAML to use double quotes.
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

# Register the representer immediately after importing yaml
yaml.add_representer(QuotedString, represent_quoted_string)
# ---------------------------------------

# --- Configuration Constants ---
BASE_PORT = 50000
SERVER_START_DELAY = 5  # Time to wait after starting servers before starting client
SETUP_SCRIPT_PATH = "/proj/ove-PG0/murray/pringles/setup.sh"
COMPILATION_DIR = "/proj/ove-PG0/murray/pringles/build" # Directory where 'meson compile' is run
RESULTS_BASE_DIR = "/proj/ove-PG0/murray/pringles/experiments/results" # Base path for results folder

def generate_yaml_config(base_config, entity_type, entity_ip, port_offset, entity_id=None, dst_mac=None, json_name=None):
    """Generates the configuration dictionary for a client or server."""
    
    # Base port calculation to ensure uniqueness
    send_port = BASE_PORT + port_offset
    recv_port = BASE_PORT + port_offset + 1

    # Calculate experiment duration, adding a delay for servers (Feature 3)
    exp_duration = base_config['experiment_parameters']['experiment_duration']
    if entity_type == 'server':
        # Servers run longer than the client to ensure no early termination
        final_duration = exp_duration + SERVER_START_DELAY
    else:
        final_duration = exp_duration

    # Initialize the base YAML structure
    yaml_config = {
        'log_level': base_config['experiment_parameters']['log_level'],
        'send_port': send_port,
        'recv_port': recv_port,
        'send_threads': 1,  # [INACTIVE]
        # WRAPPED: Ensures 'RAW' or 'UDP' is quoted
        'socket_type': QuotedString(base_config['experiment_parameters']['socket_type']),
        # WRAPPED: Ensures self_ip is quoted
        'self_ip': QuotedString(entity_ip),
        # WRAPPED: Ensures interface name is quoted
        'interface': QuotedString(base_config['network_setup']['network_interface']),
        'batch_size': base_config['protocol_batching']['batch_size'], # [INACTIVE]
        'batch_on': base_config['protocol_batching']['batch_on'],
        'num_pkt_types': base_config['protocol_batching']['num_packet_types'],
        # Use the calculated final duration (adjusted for servers)
        'experiment_duration': final_duration, 
        'payload_size': base_config['experiment_parameters']['message_size'],
    }
    
    # Pre-calculate and wrap client destination MACs (used by both client to send, and server to reply)
    client_macs = [QuotedString(mac) for mac in base_config['routing']['client_dest_macs']]

    # --- Client Specific Fields ---
    if entity_type == 'client':
        # Routing: Wrap list elements (IPs)
        client_ips = [QuotedString(ip) for ip in base_config['routing']['list_client_dest_ips']]
        
        yaml_config.update({
            'sequencer_type': base_config['protocol_batching']['sequencer_type'],
            'num_client_threads': base_config['experiment_parameters']['num_client_threads'],
            'cli_id': entity_id, # Integer
            
            # Add json_name to client config (as a QuotedString)
            'json_name': QuotedString(json_name), 
            
            # packet_types holds only the quoted IPs
            'packet_types': [{'ips': client_ips}],
            
            # packet_types_macs holds only the quoted MACs
            'packet_types_macs': [{'macs': client_macs}] 
        })
    
    # --- Storage Server Specific Fields ---
    elif entity_type == 'server':
        # Routing: Wrap list elements (IPs)
        server_ips = [QuotedString(ip) for ip in base_config['routing']['list_storage_server_dest_ips']]

        # Randomly generated values for simplicity, as requested
        shard_id = random.randint(0, 999)
        shard_switch_id = random.randint(0, 9)

        yaml_config.update({
            'storage_type': base_config['protocol_batching']['storage_server_type'],
            'shard_id': shard_id,
            'shard_switch_id': shard_switch_id,
            'stor_id': entity_id, # Integer
            'dst_mac': QuotedString(dst_mac), # WRAPPED: Ensures server dst_mac is quoted
            # Each server uses one packet type that targets the quoted client IP
            'packet_types': [{'ips': server_ips}],
            
            # Add packet_types_macs to server config, using the client MACs for the return path
            'packet_types_macs': [{'macs': client_macs}]
        })

    return yaml_config

def execute_remote_command(ip, program_path, config_filename, ssh_key, ssh_user):
    """
    Executes a program on a remote machine asynchronously using SSH (non-blocking Popen).
    """
    # The remote command is wrapped in quotes to execute the whole string via SSH, using sudo
    remote_command = f'sudo {program_path} {config_filename}'
    
    command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no', # Bypass host key check
        '-o', 'UserKnownHostsFile=/dev/null', # Prevent known_hosts interference
        f'{ssh_user}@{ip}',
        remote_command # Execute the wrapped command with sudo
    ]
    
    print(f"Starting program on {ip} (as root): {' '.join(command)}")

    try:
        # Popen executes the command asynchronously (non-blocking)
        # Output is streamed directly to local console
        process = subprocess.Popen(command, 
                                   stdout=sys.stdout,
                                   stderr=sys.stderr,
                                   bufsize=1, 
                                   universal_newlines=True)
        return process
    except FileNotFoundError:
        print(f"ERROR: Could not find 'ssh'. Ensure SSH is installed and in your PATH.")
        return None
    except Exception as e:
        print(f"ERROR starting remote process on {ip}: {e}")
        return None

def transfer_file(local_path, remote_ip, remote_user, ssh_key, remote_filename=None):
    """
    Transfers a file from the local machine to the remote machine's home directory using SCP.
    """
    if not remote_filename:
        remote_filename = os.path.basename(local_path) # Use the basename remotely

    print(f"Transferring {local_path} to {remote_user}@{remote_ip}:~/{remote_filename}...")
    
    command = [
        'scp',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        local_path,
        f'{remote_user}@{remote_ip}:~/{remote_filename}'
    ]
    
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL
        )
        print("Transfer complete.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: SCP transfer failed with exit code {e.returncode}.")
        print(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"ERROR during file transfer: {e}")
        return False

def copy_results_back(remote_ip, remote_user, ssh_key, json_name_prefix, local_target_dir):
    """
    Copies all files matching the wildcard pattern [json_name_prefix]*.json from the 
    remote user's home directory to the local_target_dir using SCP.
    """
    
    # UPDATED: Use a direct prefix wildcard pattern
    remote_pattern = f'{json_name_prefix}*.json'
    
    # SCP command format: scp user@remote_ip:remote_path local_path
    command = [
        'scp',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        f'{remote_user}@{remote_ip}:~/{remote_pattern}', # Source (remote home directory)
        local_target_dir                             # Destination (local folder)
    ]
    
    print(f"\n--- Copying client results ({remote_pattern}) from {remote_ip} to {local_target_dir} ---")
    
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL
        )
        print(f"Results copy complete. Files saved in {local_target_dir}")
        return True
    except subprocess.CalledProcessError as e:
        # SCP often returns non-zero if the glob pattern matches no files.
        if "No such file or directory" in e.stderr or "lost connection" in e.stderr:
             print(f"Warning: No JSON files matching '{remote_pattern}' were found on {remote_ip}.")
             return False 
        
        print(f"ERROR: SCP results transfer failed with exit code {e.returncode}.")
        print(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"ERROR during results copy: {e}")
        return False


def cleanup_remote_json_files(ip, ssh_key, ssh_user, json_name_prefix):
    """
    Deletes all files matching the wildcard pattern [json_name_prefix]*.json 
    in the remote user's home directory.
    """
    # UPDATED: Use a direct prefix wildcard pattern
    remote_pattern = f'{json_name_prefix}*.json'
    
    # Use 'rm -f' to force deletion and suppress errors for non-existent files
    cleanup_command = f"rm -f ~/{remote_pattern}"
    
    command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no', 
        '-o', 'UserKnownHostsFile=/dev/null', 
        f'{ssh_user}@{ip}',
        cleanup_command
    ]

    print(f"Cleaning up old results on {ip}: Deleting files matching '{remote_pattern}'...")

    try:
        subprocess.run(
            command, 
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("Cleanup successful.")
        return True
    except subprocess.CalledProcessError as e:
        # A non-zero exit code might happen if 'rm' is blocked, 
        # but we treat this as a warning since rm -f is used.
        print(f"WARNING: Cleanup failed on {ip}. Error: {e.stderr.decode().strip()}")
        return True 
    except Exception as e:
        print(f"WARNING: Error during cleanup on {ip}: {e}")
        return True


def run_setup_script(ip, setup_script_path, ssh_key, ssh_user):
    """
    Runs the specified setup script on a remote machine synchronously (blocking run).
    """
    command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no', # Bypass host key check
        '-o', 'UserKnownHostsFile=/dev/null', # Prevent known_hosts interference
        f'{ssh_user}@{ip}',
        setup_script_path
    ]

    print(f"Running setup on {ip}: {setup_script_path}")

    try:
        subprocess.run(
            command, 
            check=True,  # Raise CalledProcessError for non-zero exit codes
            stdin=subprocess.DEVNULL # Prevent interactive hang
        )
        print(f"Setup on {ip} succeeded.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Setup script on {ip} failed with exit code {e.returncode}.")
        return False
    except Exception as e:
        print(f"ERROR connecting to {ip} for setup: {e}")
        return False

def run_compile_command(ip, ssh_key, ssh_user):
    """
    Compiles the project on the remote machine by running 'meson compile' in the build directory.
    """
    # Command uses 'cd' to change directory and then runs 'meson compile'
    compile_command = f"cd {COMPILATION_DIR} && meson compile"
    
    command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no', 
        '-o', 'UserKnownHostsFile=/dev/null', 
        f'{ssh_user}@{ip}',
        compile_command
    ]

    print(f"Compiling project on {ip} in {COMPILATION_DIR}...")

    try:
        subprocess.run(
            command, 
            check=True,
            stdin=subprocess.DEVNULL
        )
        print(f"Compilation on {ip} succeeded.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Compilation failed on {ip} with exit code {e.returncode}.")
        return False
    except Exception as e:
        print(f"ERROR connecting to {ip} for compilation: {e}")
        return False


def main(config_file="config.toml"):
    """Main function to parse config, generate YAMLs, and execute experiment."""
    
    # --- 1. Parse TOML Configuration ---
    try:
        with open(config_file, 'r') as f:
            config = toml.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return
    except toml.TomlDecodeError as e:
        print(f"Error: Failed to parse TOML file: {e}")
        return

    # Extract common remote execution parameters
    ssh_key = config['network_setup']['ssh_key']
    ssh_user = config['network_setup']['ssh_user']
    path_client = config['program_paths']['path_client']
    path_server = config['program_paths']['path_server']

    # Extract IPs
    client_ip = config['network_setup']['client_ip']
    server_ips = config['network_setup']['server_ips']
    
    # --- Extract optional and required experiment parameters ---
    try:
        # Set default for run_setup_script to False
        run_setup = config['experiment_parameters'].get('run_setup_script', False) 
        json_output_name = config['experiment_parameters']['json_name']
    except KeyError as e:
        print(f"Error: Missing required key in 'experiment_parameters' section of TOML: {e}")
        print("Please ensure 'json_name' is defined.")
        return
    # -----------------------------------------------------------------

    # --- 2. Create Unique Local Results Folder ---
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d_%H:%M:%S") + f".{now.microsecond // 1000:03d}"
    
    results_folder_name = f"{json_output_name}-{timestamp_str}"
    local_results_dir = os.path.join(RESULTS_BASE_DIR, results_folder_name)
    
    try:
        os.makedirs(local_results_dir, exist_ok=True)
        print(f"Created local results directory: {local_results_dir}")
    except OSError as e:
        print(f"FATAL: Failed to create results directory {local_results_dir}: {e}")
        return

    # -----------------------------------------------------------------

    all_ips = [client_ip] + server_ips

    # --- 3. RUN SETUP SCRIPT ON ALL MACHINES (Conditional) ---
    if run_setup:
        print("\n--- Running Setup Script on All Machines ---")
        for ip in all_ips:
            if not run_setup_script(ip, SETUP_SCRIPT_PATH, ssh_key, ssh_user):
                print("FATAL: Setup script failed on at least one machine. Aborting experiment.")
                return
        print("--- Setup Complete ---")
    else:
        print("\n--- Setup Script Execution Skipped (run_setup_script is false) ---")
        
    # --- 4. COMPILE BINARIES ON ALL MACHINES ---
    print("\n--- Compiling Binaries on All Machines ---")
    for ip in all_ips:
        if not run_compile_command(ip, ssh_key, ssh_user):
            print("FATAL: Compilation failed on at least one machine. Aborting experiment.")
            return
    print("--- Compilation Complete ---")

    # --- 5. Generate Server Configurations and Start Processes ---
    
    server_processes = []
    print("\n--- Starting Storage Servers ---")
    
    # Assumption: All servers use the single destination MAC defined in the TOML
    server_dst_mac_for_all = config['routing']['server_dest_macs'][0]

    for i, ip in enumerate(server_ips):
        server_id = random.randint(100000, 999999) 
        config_filename = f"server_{i}.yaml"
        port_offset = i * 2

        server_config = generate_yaml_config(
            config, 
            'server', 
            ip, 
            port_offset, 
            entity_id=server_id,
            dst_mac=server_dst_mac_for_all 
        )
        
        # Write YAML file
        with open(config_filename, 'w') as f:
            yaml.dump(server_config, f, default_flow_style=False)
        print(f"Generated server config: {config_filename}")

        # TRANSFER THE YAML CONFIG FILE TO THE REMOTE SERVER
        if not transfer_file(config_filename, ip, ssh_user, ssh_key):
            print(f"FATAL: Failed to transfer config to server {ip}. Aborting.")
            return
        
        # Start remote process - using the full path
        process = execute_remote_command(
            ip, 
            path_server, # Use full path from TOML
            config_filename, 
            ssh_key, 
            ssh_user
        )
        if process:
            server_processes.append(process)

    if not server_processes:
        print("FATAL: No servers were successfully started. Exiting.")
        return

    # --- 6. Wait for Servers to Initialize ---
    print(f"\nWaiting {SERVER_START_DELAY} seconds for storage servers to initialize...")
    time.sleep(SERVER_START_DELAY)

    # --- 7. Generate Client Configuration and Start Process ---
    
    print("\n--- Starting Client ---")
    client_id = random.randint(100000, 999999)
    config_filename = "client.yaml"
    client_port_offset = len(server_ips) * 2
    client_dst_mac = None 
    
    client_config = generate_yaml_config(
        config,
        'client',
        client_ip,
        client_port_offset,
        entity_id=client_id,
        dst_mac=client_dst_mac,
        json_name=json_output_name 
    )
    
    # Write YAML file
    with open(config_filename, 'w') as f:
        yaml.dump(client_config, f, default_flow_style=False)
    print(f"Generated client config: {config_filename}")

    # TRANSFER THE YAML CONFIG FILE TO THE REMOTE CLIENT
    if not transfer_file(config_filename, client_ip, ssh_user, ssh_key):
        print(f"FATAL: Failed to transfer config to client {client_ip}. Aborting.")
        return

    # --- Cleanup old JSON files on client machine ---
    cleanup_remote_json_files(
        client_ip, 
        ssh_key, 
        ssh_user, 
        json_output_name
    )
    # ----------------------------------------------------

    # Start remote process - using the full path
    client_process = execute_remote_command(
        client_ip, 
        path_client, # Use full path from TOML
        config_filename, 
        ssh_key, 
        ssh_user
    )
    
    if client_process:
        print(f"\nExperiment initiated. Client running with PID: {client_process.pid}")
        print(f"The experiment will run for {config['experiment_parameters']['experiment_duration']} seconds.")
        print("This script is now waiting for the client process to finish...")
        
        client_process.wait()
        
        # --- 8. Copy Results Back ---
        copy_results_back(
            client_ip, 
            ssh_user, 
            ssh_key, 
            json_output_name, 
            local_results_dir
        )
        
        print("\n--- Client finished. Killing server processes ---")
        for proc in server_processes:
            try:
                if proc.poll() is None: 
                    print(f"Terminating server process (PID: {proc.pid})...")
                    proc.terminate()
            except Exception as e:
                print(f"Could not terminate process: {e}")

    print("\n--- Experiment Runner Finished Successfully ---")


if __name__ == '__main__':
    # Add check for necessary libraries
    try:
        import toml
    except ImportError:
        print("ERROR: 'toml' library not found. Install with 'pip install toml'.")
        sys.exit(1)
    
    try:
        import yaml
    except ImportError:
        print("ERROR: 'PyYAML' library not found. Install with 'pip install PyYAML'.")
        sys.exit(1)

    try:
        import os
    except ImportError:
        print("ERROR: 'os' module not found. This should not happen in a standard Python environment.")
        sys.exit(1)

    main()
