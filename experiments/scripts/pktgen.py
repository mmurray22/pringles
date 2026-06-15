import toml
import yaml
import uuid
import random
import subprocess
import time
import sys
import os
import json 
import paramiko
from datetime import datetime 
from copy import deepcopy 
# NEW IMPORT: Matplotlib for plotting the results
import matplotlib.pyplot as plt

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
BASE_PORT = 30000
SERVER_START_DELAY = 5  # Time to wait after starting servers before starting client
SWITCH_START_DELAY = 5  # Time to wait after starting servers before starting client
EXPERIMENT_DELAY = 15  # Time to wait between experiments
COMPILATION_DIR = "/proj/ove-PG0/murray/pringles/build" # Directory where 'meson compile' is run
RESULTS_BASE_DIR = "/proj/ove-PG0/murray/pringles/experiments/results" # Base path for results folder

def generate_switch_config(base_config, i, cntrl_port_num, ring_size, client_base_recv_port, serv_recv_port, loopback_port, num_client_threads):
    switch_params = base_config['switches']
    port_idx = "switch_ports" + str(i)
    cntrl_port = "cntrl_port_switch" + str(i)
    switch_config = {
        'loopback_port': loopback_port,
        'switch_ports': [port for port in switch_params[port_idx]],
        'cntrl_port': cntrl_port_num,
        'cpu_port': switch_params['cpu_port'],
        'listen_port': switch_params['listen_port'],
        'dummy_ip_addr': QuotedString(switch_params['dummy_ip_addr']),
        'mac_dst_addr': QuotedString(switch_params['mac_dst_addr']),
        'mac_src_addr': QuotedString(switch_params['mac_src_addr']),
        'device_number': switch_params['dev_number'],
        'cpu_interface': QuotedString(switch_params['cpu_interface']),
        'external_interface': QuotedString(switch_params['external_interface']),
        'port_speed': QuotedString("BF_SPEED_10G"),
        'port_fec': QuotedString("BF_FEC_TYP_NONE"),
        'loopback_mode': QuotedString("BF_LPBK_MAC_NEAR"),
        'meta_circulate': 1,
        'in_cntrl': 1,
        'out_cntrl': 1,
        'storage_server_ip': QuotedString(base_config['network_setup']['stor_ips'][0]),
        'storage_server_recv_port': QuotedString(base_config['network_setup']['stor_recv_port']),
        'client_ip': QuotedString(base_config['network_setup']['cli_ips'][0]),
        'size_of_ring': ring_size,
        'switch_mac': QuotedString(switch_params['switch_macs'][i]),
        'switch_ip': QuotedString(switch_params['switch_ips'][i]),
        'client_mac': QuotedString(switch_params['jump_mac']),
        'client_ip': QuotedString(switch_params['jump_ip']),
        'num_client_threads': base_config['experiment_parameters']['num_client_threads'],
        'client_recv_port': client_base_recv_port,
        'use_stor': base_config['experiment_parameters']['use_store'],
        'ipv4_table_entries': [QuotedString(entry) for entry in base_config['switches']['ipv4_table_entries']],
        'cli_d_port': client_base_recv_port, #client_recv_port,
        'num_client_threads': num_client_threads,
        'serv_d_port': serv_recv_port
    }

    if i == base_config["experiment_parameters"]["switches_in_ring"][len(base_config["experiment_parameters"]["switches_in_ring"]) - 1]:
        switch_config.update({'send_cntrl_pkt': 1})
    else:
        switch_config.update({'send_cntrl_pkt': 0})
    """Generates the configuration dictionary for a switch."""
    return switch_config


def generate_yaml_config(base_config, entity_type, entity_ip, port_offset, entity_id=None, entity_idx=None, dst_mac=None, json_name=None, num_failures=None, network_interface=None, shard_id=None, total_shards=None, shard_multicast=None):
    """Generates the configuration dictionary for a client or server."""
    
    # Base port calculation to ensure uniqueness
    send_port = BASE_PORT + port_offset
    recv_port = BASE_PORT + port_offset + 1

    # Extract required parameters from the fully merged base_config
    exp_params = base_config['experiment_parameters']
    proto_params = base_config['protocol_batching']
    net_params = base_config['network_setup']

    # Calculate experiment duration, adding a delay for servers (Feature 3)
    exp_duration = exp_params['experiment_duration']
    warm_up = exp_params['warm_up']
    cool_down = exp_params['cool_down']

    if entity_type == 'server':
        # Servers run longer than the client to ensure no early termination
        final_duration = exp_duration + warm_up + cool_down + SERVER_START_DELAY
    elif entity_type == 'switch':
        final_duration = exp_duration + warm_up + cool_down + SWITCH_START_DELAY
    else:
        final_duration = exp_duration + warm_up + cool_down
    
    # Initialize the base YAML structure
    # Include the network information for EVERY component of the system in every YAML
    cli_macs = [QuotedString(mac) for mac in net_params['cli_macs']]
    cli_ips = [QuotedString(mac) for mac in net_params['cli_ips']]
    stor_macs = [QuotedString(mac) for mac in net_params['stor_macs']]
    stor_ips = [QuotedString(mac) for mac in net_params['stor_ips']]

    yaml_config = {
        'log_level': exp_params['log_level'],
        'switch_ip': QuotedString(net_params['switch_ip']),
        'switch_mac': QuotedString(net_params['switch_mac']),
        'cli_macs': cli_macs,
        'cli_ips': cli_ips,
        'stor_macs': stor_macs,
        'stor_ips': stor_ips,
        'send_port': QuotedString(send_port),
        'recv_port': QuotedString(recv_port),
        'stor_recv_port': QuotedString(net_params['stor_recv_port']),
        'switch_recv_port': QuotedString(net_params['switch_recv_port']),
        'send_threads': 1,  # [INACTIVE]
        # WRAPPED: Ensures 'RAW' or 'UDP' is quoted
        'socket_type': QuotedString(exp_params['socket_type']),
        # WRAPPED: Ensures self_ip is quoted
        'self_ip': QuotedString(entity_ip),
        # WRAPPED: Ensures interface name is quoted
        'batch_size': exp_params['batch_size'],
        'batch_usec_timeout': exp_params['batch_usec_timeout'], 
        'batch_on': proto_params['batch_on'],
        'num_pkt_types': proto_params['num_packet_types'],
        # Use the calculated final duration (adjusted for servers)
        'experiment_duration': final_duration, 
        'payload_size': exp_params['message_size'],
        'use_switch': exp_params['use_switch'],
        'use_store': exp_params['use_store'],
        'use_shard': exp_params['use_shard'],
        'use_streams': exp_params['use_streams'],
        'ack_threshold': exp_params['ack_threshold'],
        'use_client_count_acks': exp_params['use_client_count_acks'] 
    }
    
    # Pre-calculate and wrap client destination MACs (used by both client to send, and server to reply)

    # --- Client Specific Fields ---
    if entity_type == 'client':
        dummy = ""
        yaml_config.update({
            'sequencer_type': proto_params['sequencer_type'],
            'num_client_threads': exp_params['num_client_threads'],
            'cli_id': entity_id, # Integer
            
            # Add json_name to client config (as a QuotedString)
            'json_name': QuotedString(json_name), 
            
            # Add num_failures to client config
            'num_failures': num_failures, 

            # warm up time
            'warm_up': warm_up,

            # cool down time
            'cool_down': cool_down,
            'interface': QuotedString(network_interface),
            'cli_idx': entity_idx,
            'shard_multicast_addr': QuotedString(dummy)
        })
    
    # --- Storage Server Specific Fields ---
    elif entity_type == 'server':
        # TODO dummy shard_switch_id
        shard_switch_id = random.randint(0, 9)

        yaml_config.update({
            'storage_type': proto_params['storage_server_type'],
            'shard_id': shard_id,
            'shard_switch_id': shard_switch_id,
            'stor_id': entity_id, # Integer
            'use_switch': exp_params['use_switch'],
            'num_storage_threads': exp_params['num_storage_threads'],
            'interface': QuotedString(network_interface),
            'shard_multicast_addr': QuotedString(shard_multicast)
        })

    elif entity_type == 'switch':
        dummy = ""
        yaml_config.update({
            'interface': QuotedString(network_interface),
            'all_shards': total_shards,
            'shard_multicast_addr': QuotedString(dummy)
        })

    return yaml_config

def kill_process(process_name, ssh_key, ssh_user, ip):
    """
    Executes a program on a remote machine asynchronously using SSH, 
    redirecting stdout/stderr to a log file. Returns the Popen object and the log filename.
    """
    # NEW/MODIFIED: redirect all output (&>) to the log file, and run in background (&)
    command = []
    remote_command = f'killall {process_name}'
    full_remote_command = f'/bin/bash -c "{remote_command} ; sleep 1"'
    command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no', # Bypass host key check
        '-o', 'UserKnownHostsFile=/dev/null', # Prevent known_hosts interference
        f'{ssh_user}@{ip}',
        remote_command # Use the command that includes logging/backgrounding
    ]
    
    # UPDATED PRINT: Reflects the logging change
    print(f"KILLING program on {ip} (as root): {' '.join(command)}...")

    subprocess.Popen(command, stdout=subprocess.PIPE)


def execute_remote_command(ip, program_path, config_filename, ssh_key, ssh_user, exp_index, prefix):
    """
    Executes a program on a remote machine asynchronously using SSH, 
    redirecting stdout/stderr to a log file. Returns the Popen object and the log filename.
    """
    # NEW/MODIFIED: Log file is named after the IP address
    log_filename = prefix + f"_{ip}_{exp_index}.txt" 
    
    # NEW/MODIFIED: redirect all output (&>) to the log file, and run in background (&)
    command = []
    remote_command = f'{program_path} ~/{config_filename} > ~/{log_filename} &'
    full_remote_command = f'/bin/bash -c "{remote_command} ; sleep 1"'
    command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no', # Bypass host key check
        '-o', 'UserKnownHostsFile=/dev/null', # Prevent known_hosts interference
        f'{ssh_user}@{ip}',
        remote_command # Use the command that includes logging/backgrounding
    ]
    
    # UPDATED PRINT: Reflects the logging change
    print(f"Starting program on {ip} (as root): {' '.join(command)}, logging to ~/{log_filename}...")

    try:
        # Popen executes the command asynchronously (non-blocking)
        # We redirect the local Popen stdout/stderr to /dev/null since the remote program's 
        # output is already being redirected to the remote log file.
        process = subprocess.Popen(command, 
                                   stdout=subprocess.DEVNULL, # Change from PIPE to DEVNULL
                                   stderr=subprocess.DEVNULL, # Change from PIPE to DEVNULL
                                   bufsize=1)
        # NEW RETURN: Return the log filename
        return process, log_filename
    except FileNotFoundError:
        print(f"ERROR: Could not find 'ssh'. Ensure SSH is installed and in your PATH.")
        return None, log_filename # Return log_filename even on error
    except Exception as e:
        print(f"ERROR starting remote process on {ip}: {e}")
        return None, log_filename # Return log_filename even on error

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
    print(f"Command: {command}")
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
             print(f"Warning: No JSON files matching '{remote_pattern}' were found on {remote_ip}. This is expected if the client did not finish correctly.")
             return False 
        
        print(f"ERROR: SCP results transfer failed with exit code {e.returncode}.")
        print(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"ERROR during results copy: {e}")
        return False

def copy_log_file_back(remote_ip, remote_user, ssh_key, log_filename, local_target_dir):
    """
    NEW FUNCTION: Copies the specified remote log file ([ip].txt) from the remote machine 
    to the local results folder using SCP.
    """
    # Local path for the log file
    local_log_path = os.path.join(local_target_dir, log_filename)
    
    command = [
        'scp',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        f'{remote_user}@{remote_ip}:~/{log_filename}', # Source (remote home directory)
        local_log_path                               # Destination (local folder)
    ]
    
    print(f"--- Copying remote log file {log_filename} from {remote_ip} ---")
    
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL
        )
        print(f"Log copy complete. Saved to {local_log_path}")
        return True
    except subprocess.CalledProcessError as e:
        # Log failure with a warning, as logs might not exist if the process failed immediately
        print(f"WARNING: SCP log transfer failed for {log_filename}. Code {e.returncode}. Log may not exist or connection failed.")
        return False
    except Exception as e:
        print(f"ERROR during log file copy: {e}")
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

    print(f"Cleaning up old JSON results on {ip}: Deleting files matching '{remote_pattern}'...")

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

def cleanup_remote_yaml_files(hosts, ssh_key, ssh_user):
    """Deletes all generated YAML config files on all remote hosts after all experiments."""
    print("\n--- Final Cleanup: Deleting all remote YAML config files ---")
    
    ssh_options = f"-i {ssh_key} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    # Command: ssh user@ip "rm -f ~/*.yaml" (Deletes all .yaml files in the home directory)
    # Using 'rm -f ~/*.yaml' to ensure all temporary config files are deleted.
    remote_command = f"rm -f ~/*.yaml"

    for ip in hosts:
        print(f"Deleting YAML files on {ip}...")
        full_ssh_command = f"ssh {ssh_options} {ssh_user}@{ip} \"{remote_command}\""
        
        try:
            # Use subprocess.run for synchronous, non-critical deletion
            subprocess.run(
                full_ssh_command, 
                shell=True, 
                check=False, # Do not raise error if files are not found
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL
            )
        except Exception as e:
            # Non-critical error, just report
            print(f"Warning: Failed to delete YAML files on {ip}. Error: {e}")
            
    print("Remote YAML cleanup finished.")

def process_and_aggregate_results(local_target_dir, ring_size):
    """
    Reads all JSON files in the target directory, calculates aggregate throughput and 
    total average latency PER JSON_NAME, and writes a summary JSON file for each group.
    """
    # Find all JSON files in the directory
    print(ring_size)
    all_files = os.listdir(local_target_dir)
    # Filter out files that look like aggregated summaries (ends with just .json)
    result_files = [f for f in all_files if f.endswith('.json') and len(f.split('_')) > 1]
    
    if not result_files:
        print(f"Warning: No raw client result JSON files found in {local_target_dir} for aggregation. Cannot aggregate.")
        return

    # Group files by their json_name prefix (e.g., 'two_clients', 'four_clients')
    # The prefix is the part before the first underscore in the filename.
    grouped_results = {}
    for filename in result_files:
        # Extract the json_name_prefix from the filename
        # Assumes format is: prefix_..._threadID.json
        parts = filename.split('_')
        print(parts)
        # Use everything up to the first underscore as the prefix
        json_name_prefix = parts[0]
            
        if json_name_prefix not in grouped_results:
            grouped_results[json_name_prefix] = []
        grouped_results[json_name_prefix].append(filename)

    print(f"\n--- Aggregating Results for {len(grouped_results)} experiment groups ---")
    it = 0
    for json_name_prefix, files_to_aggregate in grouped_results.items():
        
        total_agg_tput = 0.0
        total_avg_latency_sum = 0.0
        file_count = 0
        batch_size = 0 
        print(f"Processing group: {json_name_prefix} ({len(files_to_aggregate)} client results)")

        for filename in files_to_aggregate:
            filepath = os.path.join(local_target_dir, filename)
            data = None
            
            try:
                with open(filepath, 'r') as f:
                    content = f.read().strip()
                
                # Robust JSON Decoding
                start_index = content.find('{')
                end_index = content.rfind('}')
                
                if start_index != -1 and end_index != -1 and end_index > start_index:
                    json_string = content[start_index:end_index + 1]
                    data = json.loads(json_string)
                else:
                    print(f"Error: File {filename} does not contain a valid JSON object. Skipping.")
                    continue
                
                # Aggregate Throughput
                throughput = data.get('throughput')
                if isinstance(throughput, (int, float)):
                    total_agg_tput += throughput

                # Aggregate Latency
                avg_latency = data.get('avg_latency')
                if isinstance(avg_latency, (int, float)):
                    total_avg_latency_sum += avg_latency
                    file_count += 1
                batch_size = data.get('batch_size')
            except json.JSONDecodeError:
                print(f"Error: Failed to decode JSON from file: {filename}. Skipping.")
            except IOError as e:
                print(f"Error: Failed to read file {filename}: {e}. Skipping.")

        # Calculate Final Average Latency
        final_avg_latency = total_avg_latency_sum / file_count if file_count > 0 else 0.0

        # Construct Final Output
        final_results = {
            "agg_tput": total_agg_tput,
            "total_avg_latency": final_avg_latency,
            "num_clients": file_count,
            "batch_size": batch_size,
            #"num_switches_in_ring": ring_size[it]
        }
        it += 1

        # Write the final aggregated JSON file named [json_name].json
        output_filename = f"{json_name_prefix}.json"
        output_filepath = os.path.join(local_target_dir, output_filename)
        
        try:
            with open(output_filepath, 'w') as f:
                json.dump(final_results, f, indent=4)
            print(f"Summary for {json_name_prefix} written to: {output_filepath}")
        except IOError as e:
            print(f"Error: Failed to write final summary JSON to {output_filepath}: {e}")

def run_compile_command(ip, ssh_key, ssh_user):
    """Compiles the project on the remote machine by running 'meson compile' in the build directory."""
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
        subprocess.run(command, check=True, stdin=subprocess.DEVNULL)
        print(f"Compilation on {ip} succeeded.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Compilation failed on {ip} with exit code {e.returncode}.")
        return False
    except Exception as e:
        print(f"ERROR connecting to {ip} for compilation: {e}")
        return False

def execute_scp_switch_cmd(config, program_path, binary_name):
    print(f"Setting up the switches now!")
    # here is the setup
    jump_host = config["switches"]["jump_host"]
    jump_user = config["switches"]["jump_user"]
    target_user = "root"
    switch_ips = config["switches"]["switch_ips"]
    switches_in_ring = config["experiment_parameters"]["switches_in_ring"]
    config_filename = config["switches"]["config_name"]
    print(f"Switch ips: {switch_ips}")
    it = 0

    for idx in switches_in_ring:
        switch_ip = switch_ips[idx]
        # 1. Connect to Jumppoint
        jump_client = paramiko.SSHClient()
        jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jump_client.connect(jump_host, username=jump_user)
        
        # 2. Open a transport channel through the Jumppoint to the Target
        jump_transport = jump_client.get_transport()
        dest_addr = (switch_ip, 22)
        local_addr = ('localhost', 0) # Source addr on jump host
        jump_channel = jump_transport.open_channel("direct-tcpip", dest_addr, local_addr)
        
        # 3. Connect to Target using the Jumppoint channel as a socket
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(switch_ip, username=target_user, sock=jump_channel)
        
        # 4. Run your command
        transport = target_client.get_transport()
        chan = transport.open_session()
       
        remote_path = "/root/" + binary_name
        print(remote_path)
        print(program_path)
        sftp = target_client.open_sftp()
        sftp.put(program_path, remote_path) 
        
        # Cleanup
        sftp.close()
        target_client.close()
        jump_client.close()

def execute_remote_switch_cmd(config, program_path, config_filename):
    print(f"Setting up the switches now!")
    # here is the setup
    jump_host = config["switches"]["jump_host"]
    jump_user = config["switches"]["jump_user"]
    target_user = "root"
    switch_ips = config["switches"]["switch_ips"]
    switches_in_ring = config["experiment_parameters"]["switches_in_ring"]
    print(f"Switch ips: {switch_ips}")
    it = 0

    for idx in switches_in_ring:
        switch_ip = switch_ips[idx]
        # 1. Connect to Jumppoint
        jump_client = paramiko.SSHClient()
        jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jump_client.connect(jump_host, username=jump_user)
        
        # 2. Open a transport channel through the Jumppoint to the Target
        jump_transport = jump_client.get_transport()
        dest_addr = (switch_ip, 22)
        local_addr = ('localhost', 0) # Source addr on jump host
        jump_channel = jump_transport.open_channel("direct-tcpip", dest_addr, local_addr)
        
        # 3. Connect to Target using the Jumppoint channel as a socket
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(switch_ip, username=target_user, sock=jump_channel)
        
        # 4. Run your command
        transport = target_client.get_transport()
        chan = transport.open_session()
        
        # Clean up the machine
        #kill_prior_process = "pkill -9 -f 'bf_switchd|net_cli'"
        #chan.exec_command(kill_prior_process)
        #time.sleep(5)

        program_command = "nohup " + program_path + f"> ~/{config_filename} 2>&1 &"
        print(program_command)
        chan = transport.open_session()
        chan.exec_command(program_command)
        time.sleep(5)

        # Cleanup
        target_client.close()
        jump_client.close()


def setup_switches(config):
    print(f"Setting up the switches now!")
    # here is the setup
    jump_host = config["switches"]["jump_host"]
    jump_user = config["switches"]["jump_user"]
    target_user = "root"
    tofino_model = config["switches"]["tofino_model"]
    switchd = config["switches"]["switchd"]
    control_plane = config["switches"]["ptf_test"] 
    arch = config["switches"]["arch"]
    switch_ips = config["switches"]["switch_ips"]
    switches_in_ring = config["experiment_parameters"]["switches_in_ring"]
    config_filename = config["switches"]["config_name"]
    print(f"Switch ips: {switch_ips}")
    it = 0
    for idx in switches_in_ring:
        switch_ip = switch_ips[idx]
        # 1. Connect to Jumppoint
        jump_client = paramiko.SSHClient()
        jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jump_client.connect(jump_host, username=jump_user)
        
        # 2. Open a transport channel through the Jumppoint to the Target
        jump_transport = jump_client.get_transport()
        dest_addr = (switch_ip, 22)
        local_addr = ('localhost', 0) # Source addr on jump host
        jump_channel = jump_transport.open_channel("direct-tcpip", dest_addr, local_addr)
        
        # 3. Connect to Target using the Jumppoint channel as a socket
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(switch_ip, username=target_user, sock=jump_channel)
        
        # 4. Run your command
        transport = target_client.get_transport()
        chan = transport.open_session()
        
        # Control port
        cntrl_key = "cntrl_port_switch" + str(idx)
        if it == 0:
            cntrl_port = config["switches"][cntrl_key][switches_in_ring[(len(switches_in_ring)-1)]] #config["switches"][cntrl_key][(it - 1) % len(switches_in_ring)]
        else:
            cntrl_port = config["switches"][cntrl_key][switches_in_ring[(it-1)]] #config["switches"][cntrl_key][(it - 1) % len(switches_in_ring)]
        it += 1

        # Copy over config
        server_ips = config['network_setup']['stor_ips']
        num_client_threads = config['experiment_parameters']['num_client_threads']
        client_recv_port = BASE_PORT + len(server_ips) * 2 + num_client_threads # TODO hardcoded
        server_recv_port = BASE_PORT + 3 # TODO mega hardcoded
        loopback_port = config["switches"]["loopback_ports"][idx] 
        switch_config = generate_switch_config(config, idx, cntrl_port, len(switches_in_ring), client_recv_port, server_recv_port, loopback_port, num_client_threads)
        # Write YAML file locally
        with open(config_filename, 'w') as f:
            yaml.dump(switch_config, f, default_flow_style=False)
        print(f"Generated switch config: {config_filename}")
        local_path = os.path.abspath(config_filename)
        remote_path = "/root/" + config_filename

        print(local_path)
        print(remote_path)
        sftp = target_client.open_sftp()
        sftp.put(local_path, remote_path) 
        sftp.close()

        # Clean up the machine
        kill_prior_process = "pkill -9 -f 'bf_switchd|run_switchd.sh'"
        chan.exec_command(kill_prior_process)
        time.sleep(5)

        cfg_switchd = "switch_switchd_log.txt"
        sde_command = "nohup " + switchd + f"> ~/{cfg_switchd} 2>&1 &"
        print(f"Full SDE command: {sde_command}")
        chan = transport.open_session()
        chan.exec_command(sde_command)
        time.sleep(10)
        
        # Clean up the machine
        kill_prior_process = "pkill -9 -f 'run_p4_tests.sh|/root/bf-sde-9.4.0/install/bin/ptf'"
        chan = transport.open_session()
        chan.exec_command(kill_prior_process)
        time.sleep(5)

        cfg_control_plane = "control_plane_log.txt"
        cntrl_command = "nohup " + control_plane + f"> ~/{cfg_control_plane} 2>&1 &"
        print(f"Full Control Plane command: {cntrl_command}")
        chan = transport.open_session()
        chan.exec_command(cntrl_command)
        time.sleep(5)

        # Cleanup
        target_client.close()
        jump_client.close()

# To be executed for each experiment
def run_experiment_cycle(config, exp_index, local_results_dir, with_tunnel):
    """Runs a single, full experiment cycle based on the merged configuration."""
    
    # Extract run-specific parameters from the merged config
    ssh_key = config['network_setup']['ssh_key']
    ssh_user = config['network_setup']['ssh_user']
    client_ips = config['network_setup']['cli_ips'] # TODO SEQ make plural?
    cli_net_ifs = config['network_setup']['cli_net_ifs'] # TODO SEQ make plural?
    switch_ip = config['network_setup']['switch_ip']
    switch_net_if = config['network_setup']['switch_net_if']
    server_ips = config['network_setup']['stor_ips']
    server_net_ifs = config['network_setup']['stor_net_ifs']
    path_client = config['program_paths']['path_client']
    path_server = config['program_paths']['path_server']
    path_switch = config['program_paths']['path_switch']
    
    json_output_name = config['experiment_parameters']['json_name']
    num_failures = config['experiment_parameters']['num_failures']
    
    print(f"\n========================================================")
    print(f"   RUNNING EXPERIMENT {exp_index + 1}: {json_output_name}")
    print(f"   Client Threads: {config['experiment_parameters']['num_client_threads']}")
    print(f"========================================================")
    
    server_processes = []
    client_log_files = []
    client_processes = []
    server_log_files = {} # MODIFIED: Dictionary to store the log file name for each server
    switch_processes = []
    switch_log_files = {} # MODIFIED: Dictionary to store the log file name for each server
    
    # Setup shards TODO only for a single switch
    size_of_shard = config['experiment_parameters']['number_of_machines_per_shard']
    print(f"Size of shards: {size_of_shard}")
    total_list_of_shards = []
    ip_to_shard = {}
    shard_to_multicast_addr = []
    num_of_shard = 0
    base_multicast_addr = "239.1.1."
    for i in range(0, len(server_ips), size_of_shard): # TODO check whether the number of servers divides evenly into shard size
        shard = []
        for j in range(i, i+size_of_shard):
            print(server_ips[j])
            shard.append(server_ips[j])
            ip_to_shard[server_ips[j]] = num_of_shard
        total_list_of_shards.append(shard)
        num_of_shard += 1

    for i in range(0, num_of_shard): #255 <-- TODO max number of shards
        shard_to_multicast_addr.append((base_multicast_addr + str(i)));
    yaml_shard_to_multicast_addr = [QuotedString(addr) for addr in shard_to_multicast_addr]
    print(yaml_shard_to_multicast_addr)

    try:
        # Assumption: All servers respond to all destination MAC defined in the TOML
        server_dst_mac_for_all = config['routing']['server_dest_macs'][0]

        # --- 5. Generate Server Configurations and Start Processes ---
        print("\n--- Starting Storage Servers ---")
   
        for i, ip in enumerate(server_ips):
            server_id = random.randint(100000, 999999) 
            config_filename = f"server_config_{json_output_name}_{i}.yaml" # Unique filename
            port_offset = i * 2

            server_config = generate_yaml_config(
                config, 
                'server', 
                ip, 
                port_offset,
                entity_id=server_id,
                entity_idx=i,
                dst_mac=server_dst_mac_for_all,
                network_interface=server_net_ifs[i],
                shard_id=ip_to_shard[ip],
                shard_multicast=shard_to_multicast_addr[ip_to_shard[ip]]
            )
            
            server_exec = os.path.basename(path_server) # Use the basename remotely
            kill_process(server_exec, ssh_key, ssh_user, ip)
               
            # Write YAML file locally
            with open(config_filename, 'w') as f:
                yaml.dump(server_config, f, default_flow_style=False)
            print(f"Generated server config: {config_filename}")
            print(f"Server binary to copy: {path_server}")

            # TRANSFER THE YAML CONFIG FILE TO THE REMOTE SERVER
            if not with_tunnel:
                if not transfer_file(config_filename, ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer config to server {ip}")
                if not transfer_file(path_server, ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer server binary to server {ip}")
            else:
                local_path = os.path.abspath(config_filename)
                print("SCPing the server config")
                execute_scp_switch_cmd(config, local_path, config_filename)
                print("Done SCPing the server config")
            
            # Start remote process
            if not with_tunnel:
                print("MADE IT HERE ====================================================")
                print(path_server)
                exec_filepath = "~/" + server_exec
                prefix = "server"
                execute_remote_command(ip, exec_filepath, config_filename, ssh_key, ssh_user, exp_index, prefix)
                # Second, execute the command
                process, log_filename = execute_remote_command(ip, exec_filepath, config_filename, ssh_key, ssh_user, exp_index, prefix) # MODIFIED: Get log filename
                print("Done executing the server!")
                if process:
                    server_processes.append(process)
                    server_log_files[ip] = log_filename # MODIFIED: Store log filename
                else:
                    raise Exception(f"Failed to start server process on {ip}")
            else:
                cli_binary = config['program_paths']['client_binary']
                server_binary = config['program_paths']['server_binary']
                print("SCPing the server binary")
                execute_scp_switch_cmd(config, path_server, server_binary)
                print("Executing the server binary")
                execute_remote_switch_cmd(config, path_server, config_filename)
                print("Done with the server binary!")


        if not with_tunnel and not server_processes:
            raise Exception("No servers were successfully started.")

        # --- 6. Wait for Servers to Initialize ---
        print(f"\nWaiting {SERVER_START_DELAY} seconds for storage servers to initialize...")
        time.sleep(SERVER_START_DELAY)
        
        # --- 7. Generate Switch Configuration and Start Process --- # TODO
        if not config['experiment_parameters']['use_hardware_switch']: #not with_tunnel:
            print("\n--- Starting Switch ---")
            switch_id = random.randint(100000, 999999)
            switch_config_filename = f"switch_config_{json_output_name}.yaml" # Unique filename
            switch_port_offset = len(server_ips) * 2
            
            switch_config = generate_yaml_config(
                config, 
                'switch',
                switch_ip,
                switch_port_offset,
                entity_id=switch_id,
                json_name=json_output_name,
                num_failures=num_failures,
                network_interface=switch_net_if,
                total_shards=yaml_shard_to_multicast_addr
            )
            
            switch_exec = os.path.basename(path_switch) # Use the basename remotely
            kill_process(switch_exec, ssh_key, ssh_user, ip)
             
            # Write YAML file locally
            with open(switch_config_filename, 'w') as f:
                yaml.dump(switch_config, f, default_flow_style=False)
            print(f"Generated client config: {switch_config_filename}")

            # TRANSFER THE YAML CONFIG FILE TO THE REMOTE SERVER
            if not transfer_file(switch_config_filename, switch_ip, ssh_user, ssh_key):
                raise Exception(f"Failed to transfer config to server {ip}")
            
            # Start remote process
            prefix = "switch"
            process, log_filename = execute_remote_command(switch_ip, path_switch, switch_config_filename, ssh_key, ssh_user, exp_index, prefix) # MODIFIED: Get log filename
            if process:
                switch_processes.append(process)
                switch_log_files[ip] = log_filename # MODIFIED: Store log filename
            else:
                raise Exception(f"Failed to start server process on {ip}")

            if not switch_processes:
                raise Exception("No servers were successfully started.")

            # --- 8. Wait for Servers to Initialize --- TODO
            print(f"\nWaiting {SWITCH_START_DELAY} seconds for software switch to initialize...")
            time.sleep(SWITCH_START_DELAY)

        # --- 9. Generate Client Configuration and Start Process ---
        print("\n--- Starting Client ---")
        for i, ip in enumerate(client_ips):
            client_id = random.randint(100000, 999999) 
            config_filename = f"client_config_{json_output_name}_{i}.yaml" # Unique filename
            client_port_offset = len(server_ips) * 2
            client_config = generate_yaml_config(
                 config, 
                 'client',
                 ip,
                 client_port_offset,
                 entity_id=client_id,
                 entity_idx=i,
                 json_name=json_output_name,
                 num_failures=num_failures,
                 network_interface=cli_net_ifs[i]
            )
            client_exec = os.path.basename(path_client) # Use the basename remotely
            kill_process(client_exec, ssh_key, ssh_user, ip)
             
            # Write YAML file locally
            with open(config_filename, 'w') as f:
                yaml.dump(client_config, f, default_flow_style=False)
            print(f"Generated server config: {config_filename}")

            # TRANSFER THE YAML CONFIG FILE TO THE REMOTE SERVER
            if not with_tunnel: 
                if not transfer_file(config_filename, ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer config to server {ip}")
                if not transfer_file(path_client, ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer server binary to server {ip}")
            else:
                local_path = os.path.abspath(config_filename)
                execute_scp_switch_cmd(config, local_path, config_filename)
            
            #cleanup_remote_json_files(ip, ssh_key, ssh_user, json_output_name) TODO
            if not with_tunnel:
                cli_exec_file = "~/" + client_exec
                prefix = "client"
                client_process, client_log_filename = execute_remote_command( # MODIFIED: Get log filename
                    ip, 
                    cli_exec_file, 
                    config_filename, 
                    ssh_key, 
                    ssh_user,
                    exp_index,
                    prefix
                )
                if client_process:
                    print(f"\nExperiment initiated. Client running with PID: {client_process.pid}")
                    print("This script is now waiting for the client process to finish...")
                    client_processes.append(client_process)
                    client_log_files.append(client_log_filename)
                else: # TODO more error handling?
                    raise Exception("Failed to start client process.")
            else:
                cli_binary = config['program_paths']['client_binary']
                execute_scp_switch_cmd(config, path_server, cli_binary)
                execute_remote_switch_cmd(config, path_server, config_filename)

        i = 0 
        for proc in client_processes: 
            # Wait for the client process to finish
            print("Waiting for the client process {proc.id} to finish!")
            proc.wait()
            
            # --- 8. Copy JSON Results Back ---
            copy_results_back(
                client_ips[i], 
                ssh_user, 
                ssh_key, 
                json_output_name, 
                local_results_dir
            )
            
            # NEW: Copy Client Log File Back
            copy_log_file_back(
                client_ips[i],
                ssh_user,
                ssh_key,
                client_log_files[i],
                local_results_dir
            )
            i += 1

    except Exception as e:
        print(f"\nFATAL ERROR during experiment cycle {exp_index + 1}: {e}")
        
    finally:
        # --- 10. Kill all server processes and retrieve logs ---
        print("\n--- Experiment finished. Retrieving server logs and cleaning up ---")
        
        # MODIFIED: Copy Server Log Files Back (for all servers)
        for ip, log_filename in server_log_files.items():
            copy_log_file_back(
                ip,
                ssh_user,
                ssh_key,
                log_filename,
                local_results_dir
            )
        
        for ip, log_filename in switch_log_files.items():
            copy_log_file_back(
                switch_ip,
                ssh_user,
                ssh_key,
                log_filename,
                local_results_dir
            )
   
        for proc in server_processes:
            try:
                if proc.poll() is None:
                    print(f"Terminating server process (PID: {proc.pid})...")
                    proc.kill()
                # The nohup process is difficult to kill via Popen.terminate(). 
                # Relying on the server timeout is safer.
                #pass 
            except Exception as e:
                print(f"Could not check on server process: {e}")
        
        print("Server processes are assumed to exit on their own after the client terminates.")
        for proc in switch_processes:
            try:
                if proc.poll() is None:
                    print(f"Terminating server process (PID: {proc.pid})...")
                    proc.kill()
                # The nohup process is difficult to kill via Popen.terminate(). 
                # Relying on the server timeout is safer.
                #pass 
            except Exception as e:
                print(f"Could not check on switch process: {e}")

        print("Switch processes are assumed to exit on their own after the client terminates.")
        
def main(config_file="pktgen.toml"):
    """Main function to parse config, generate YAMLs, and execute experiments in a loop."""
    
    # --- 1. Parse TOML Configuration ---
    try:
        with open(config_file, 'r') as f:
            full_config = toml.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return
    except toml.TomlDecodeError as e:
        print(f"Error: Failed to parse TOML file: {e}")
        return

    # Extract the list of experiments to run and remove it from the base config
    experiments_to_run = full_config.pop('experiment', [])
    if not experiments_to_run:
        print("Warning: No '[[experiment]]' sections found. Running only the default configuration once.")
        default_params = full_config.get('experiment_parameters', {})
        experiments_to_run.append(default_params)
        
    # The remaining dictionary is the base configuration
    base_config = full_config.copy() 

    # --- Initial Setup ---
    client_ips = base_config['network_setup']['cli_ips']
    stor_ips = base_config['network_setup']['stor_ips']
    switch_ip = base_config['network_setup']['switch_ip']
    all_ips = client_ips + stor_ips + [switch_ip]
    ssh_key = base_config['network_setup']['ssh_key']
    ssh_user = base_config['network_setup']['ssh_user']
    with_tunnel = base_config['experiment_parameters']['with_tunnel']

    # --- 2. Create Unique Local Results Folder (All results will be copied here) ---
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d_%H%M%S") + f".{now.microsecond // 1000:03d}"
    
    # Using a generic prefix + timestamp only
    timestamp_name = f"run-{timestamp_str}"
    results_folder_name = base_config['experiment_parameters']['experiment_name'] + '_' + timestamp_name
    local_results_dir = os.path.join(RESULTS_BASE_DIR, results_folder_name)
    
    try:
        os.makedirs(local_results_dir, exist_ok=True)
        print(f"Created **shared** results directory: {local_results_dir}")
    except OSError as e:
        print(f"FATAL: Failed to create results directory {local_results_dir}: {e}")
        return

    # --- 3. RUN SETUP SCRIPT ON ALL MACHINES (Conditional) ---
    run_setup = base_config['program_paths'].get('run_setup_script', 'False').lower() == 'true'
    if run_setup:
        print("\n--- Running Setup Script on All Machines ---")
        for ip in all_ips:
            if not run_setup_script(ip, SETUP_SCRIPT_PATH, ssh_key, ssh_user):
                print("FATAL: Setup script failed on at least one machine. Aborting experiment.")
                return
        print("--- Setup Complete ---")
    else:
        print("\n--- Setup Script Execution Skipped ---")
        
    # --- 4. COMPILE BINARIES ON ALL MACHINES ---
    print("\n--- Compiling Binaries on All Machines ---") # TODO BRING BACK
    #for ip in all_ips:
    #    if not run_compile_command(ip, ssh_key, ssh_user):
    #        print("FATAL: Compilation failed on at least one machine. Aborting experiment.")
    #        return
    #print("--- Compilation Complete ---")

    # --- Start Experiment Loop ---
    print(f"\n--- Starting {len(experiments_to_run)} Experiment Runs ---")
    ring_sizes = [] 
    for exp_index, exp_params in enumerate(experiments_to_run):
        # 1. Create a deep copy of the base config for this specific run
        current_config = deepcopy(base_config)
        
        # 2. Merge experiment-specific parameters (overrides)
        for key, value in exp_params.items():
            print(key)
            print(value)
            if key in current_config.get('experiment_parameters', {}):
                current_config['experiment_parameters'][key] = value
            elif key in current_config.get('protocol_batching', {}):
                current_config['protocol_batching'][key] = value
        
        ring_sizes.append(len(current_config['experiment_parameters']['switches_in_ring']))
    
        # --- 2.5. START SWITCHES --- 
        if current_config['experiment_parameters']['use_hardware_switch']:
            current_config = deepcopy(base_config)
            switches_up = True
            switches_up = setup_switches(current_config) #TODO 

            num_switches = len(base_config['experiment_parameters']['switches_in_ring'])
            if switches_up:
                print("--- All {num_switches} switches up and running ---")
    
        # 3. Run the full experiment cycle with the merged configuration
        run_experiment_cycle(current_config, exp_index, local_results_dir, with_tunnel)

        time.sleep(EXPERIMENT_DELAY)
    
    # --- Final Step A: Aggregate ALL results from the shared directory ---\
    # Only run this once after ALL experiment cycles are finished
    process_and_aggregate_results(local_results_dir, ring_sizes)
    
    # --- Final Cleanup: Delete all temporary YAML files on remote hosts ---
    #cleanup_remote_yaml_files(all_ips, ssh_key, ssh_user)

    print("\n--- All Experiment Cycles Finished Successfully ---")


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

    try:
        import json
    except ImportError:
        print("ERROR: 'json' module not found. This should not happen in a standard Python environment.")
        sys.exit(1)
    
    try:
        import paramiko
    except ImportError:
        print("ERROR: 'paramiko' module not found. This should not happen in a standard Python environment.")
        sys.exit(1)
     
        
    # NEW CHECK for matplotlib
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: 'matplotlib' library not found. Install with 'pip install matplotlib'.")
        sys.exit(1)
    main()
