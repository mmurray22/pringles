import shlex
import toml
import yaml
import uuid
import random
import subprocess
import time
import sys
import os
import paramiko
import json
import glob
import shutil
from pathlib import Path
from itertools import chain
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
pringles_base = os.environ.get("PRINGLES_PATH", "/home/mathwiz23pi/")
BASE_PORT = 30000
SERVER_START_DELAY = 5  # Time to wait after starting servers before starting client
SWITCH_START_DELAY = 5  # Time to wait after starting servers before starting client
EXPERIMENT_DELAY = 30  # Time to wait between experiments
SETUP_SCRIPT_PATH = f"{pringles_base}/pringles/setup.sh"
COMPILATION_DIR = f"{pringles_base}/pringles/build" # Directory where 'meson compile' is run
RESULTS_BASE_DIR = f"{pringles_base}/pringles/experiments/results" # Base path for results folder

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


def generate_yaml_config(base_config, pringles_config, entity_type, entity_ip, port_offset, server_ips, entity_id=None, entity_idx=None, json_name=None, num_failures=None, network_interface=None, shard_id=None, total_shards=None, shard_multicast=None):
    """Generates the configuration dictionary for a client or server."""

    # Base port calculation to ensure uniqueness
    send_port = BASE_PORT + port_offset
    recv_port = BASE_PORT + port_offset + 1

    # Extract required parameters from the fully merged base_config
    general_exp_params = base_config['experiment_parameters']
    exp_params = pringles_config['experiment_parameters']
    net_params = pringles_config['network_setup']
    print(net_params)

    # Calculate experiment duration, adding a delay for servers (Feature 3)
    exp_duration = general_exp_params['experiment_duration']
    warm_up = general_exp_params['warm_up']
    cool_down = general_exp_params['cool_down']

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
    print("STOR IPS")
    stor_ips = [QuotedString(ips) for ips in server_ips]
    print(stor_ips)

    yaml_config = {
        'log_level': general_exp_params['log_level'],
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
        # WRAPPED: Ensures 'RAW' or 'UDP' is quoted
        'socket_type': QuotedString(exp_params['socket_type']),
        # WRAPPED: Ensures self_ip is quoted
        'self_ip': QuotedString(entity_ip),
        # WRAPPED: Ensures interface name is quoted
        'batch_size': exp_params['batch_size'],
        'batch_usec_timeout': exp_params['batch_usec_timeout'], 
        'batch_on': exp_params['batch_on'],
        # Use the calculated final duration (adjusted for servers)
        'experiment_duration': final_duration, 
        'payload_size': general_exp_params['message_size'],
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
            'num_client_threads': general_exp_params['num_client_threads'],
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
            'shard_id': shard_id,
            'shard_switch_id': shard_switch_id,
            'stor_id': entity_id, # Integer
            'use_switch': exp_params['use_switch'],
            'num_storage_threads': exp_params['num_storage_threads'],
            'interface': QuotedString(network_interface),
            'shard_multicast_addr': QuotedString(shard_multicast)
        })
    
    # --- Switch Specific Fields ---
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

def transfer_file(local_path, remote_ip, remote_user, ssh_key, remote_filename=None, remote_filepath=None):
    """
    Transfers a file from the local machine to the remote machine's home directory using SCP.
    """
    if not remote_filename:
        remote_filename = os.path.basename(local_path) # Use the basename remotely
    if not remote_filepath:
        remote_filepath = f'~/{remote_filename}'
    else:
        remote_filepath = f'{remote_filepath}/{remote_filename}'
    print(remote_filepath)

    print(f"Transferring {local_path} to {remote_user}@{remote_ip}:{remote_filepath}...")

    command = [
        'scp',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        local_path,
        f'{remote_user}@{remote_ip}:{remote_filepath}'
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

def copy_log_file_back(remote_ip, remote_user, ssh_key, log_filename, local_target_dir, remote_dir='~'):
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
        f'{remote_user}@{remote_ip}:{remote_dir}/{log_filename}',
        local_log_path
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

def process_and_aggregate_results(local_target_dir, json_name, system_name, base_config, system_config):
    """
    Reads all JSON files in the target directory, calculates aggregate throughput and 
    total average latency PER JSON_NAME, and writes a summary JSON file for each group.
    """
    # Find all JSON files in the directory
    all_files = os.listdir(local_target_dir)
    # Filter out files that look like aggregated summaries (ends with just .json)
    result_files = [f for f in all_files if f.endswith('.json') and len(f.split('_')) > 1]

    if not result_files:
        print(f"Warning: No raw client result JSON files found in {local_target_dir} for aggregation. Cannot aggregate.")
        return

    print(f"\n--- Aggregating Results for this experiment group ---")
    it = 0
    total_agg_tput = 0.0
    total_avg_latency_sum = 0.0
    total_sub_lat_sum = 0.0
    file_count = 0
    batch_size = 0 

    for filename in result_files:
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

            # Aggregate Time-to-first-Subscribe
            sub_lat = data.get('sub_lat')
            if isinstance(sub_lat, (int, float)):
                total_sub_lat_sum += sub_lat
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON from file: {filename}. Skipping.")
        except IOError as e:
            print(f"Error: Failed to read file {filename}: {e}. Skipping.")

    # Calculate Final Average Latency
    final_avg_latency = total_avg_latency_sum / file_count if file_count > 0 else 0.0
    final_avg_sub_lat = total_sub_lat_sum / file_count if file_count > 0 else 0.0
    print("=========== INAL STATS")
    # Construct Final Output
    num_switches = 0
    if system_name == "pringles":
        num_switches = len(system_config['experiment_parameters']['switches_in_ring'])

    final_results = {
        "agg_tput": total_agg_tput,
        "total_avg_latency": final_avg_latency,
        "subscribe_delay": final_avg_sub_lat,
        "num_clients": file_count,
        "batch_size": batch_size,
        "payload_size": base_config['experiment_parameters']['message_size'],
        "num_switches_in_ring": num_switches,
        "num_shards": base_config['experiment_parameters']['num_shards'],
        "num_servers_per_shard": base_config['experiment_parameters']['num_servers_per_shard'],
        "git_hash": base_config['experiment_parameters']['git_hash'],
        "system_name": system_name
    }
    print(final_results)

    # Iterate over all .json files in the source directory
    local_indiv_json_dir = Path(local_target_dir) /  json_name #os.path.join(local_target_dir, json_name) # NEW
    print(str(local_indiv_json_dir))
    local_indiv_json_dir.mkdir(parents=True, exist_ok=True)
    print(result_files)
    for json_file in result_files:
        source_path = Path(local_target_dir) / json_file
        print(str(source_path))
        target_path = local_indiv_json_dir
        shutil.move(str(source_path), str(target_path))
        print(f"Moved: {source_path.name} to {str(target_path)}")
    patterns = ['*.txt', '*.log']
    files_to_move = chain.from_iterable(Path(local_target_dir).glob(pattern) for pattern in patterns)
    for file_path in files_to_move:
        source_path = Path(local_target_dir) / file_path
        target_path = local_indiv_json_dir
        shutil.move(str(source_path), str(target_path))
        print(f"Moved: {source_path.name}")
    # Write the final aggregated JSON file named [json_name].json
    output_filename = f"{json_name}.json"
    output_filepath = os.path.join(local_target_dir, output_filename)
    #print(f"Moved: {json_file.name}")  

    try:
        with open(output_filepath, 'w') as f:
            json.dump(final_results, f, indent=4)
        print(f"Summary for {json_name} written to: {output_filepath}")
    except IOError as e:
        print(f"Error: Failed to write final summary JSON to {output_filepath}: {e}")


def run_remote_command_sync(ip, command, ssh_key, ssh_user):
    """
    Executes a command on a remote machine synchronously (blocks until complete).
    Used for setup steps that must finish before proceeding (e.g., topic creation).
    Returns True on success, False on failure.
    """
    full_command = [
        'ssh',
        '-i', ssh_key,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        f'{ssh_user}@{ip}',
        command
    ]
    print(f"Running on {ip} (sync): {full_command}")
    try:
        subprocess.run(full_command, check=True, stdin=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Command failed on {ip} with exit code {e.returncode}.")
        return False
    except Exception as e:
        print(f"ERROR running command on {ip}: {e}")
        return False


def kill_remote_process(ip, process_name, ssh_key, ssh_user):
    """
    Kills all remote processes matching process_name via pkill -f.
    Used for comparison systems that don't self-terminate after experiment_duration.
    """
    print(f"Killing '{process_name}' on {ip}...")
    return run_remote_command_sync(ip, f'sudo pkill {process_name}', ssh_key, ssh_user)


def setup_kafka_nodes(config, ssh_key, ssh_user):
    """
    Sets up Kafka on broker and client nodes.

    Broker nodes (seq_ips): the Kafka broker distribution is still downloaded remotely
    since it is ~100 MB of pre-compiled JVM bytecode with no local build step needed.

    Client nodes (cli_ips): kafka-log is cloned and built locally via `sbt assembly`
    into a fat jar, then SCPed to the remote. The remote only needs Java, not sbt.

    Requires in [program_paths]:
      kafka_dir              - where to install the Kafka distribution on broker nodes
      kafka_download_url     - full URL to a Kafka .tgz release
      kafka_log_local_src    - local path to clone kafka-log into for building
      kafka_log_jar_remote   - full remote path where the fat jar will be placed
                               (e.g. /proj/.../kafka-log-assembly.jar)
    """
    seq_ips              = config['network_setup'].get('seq_ips', [])
    cli_ips              = config['network_setup']['cli_ips']
    kafka_dir            = config['program_paths'].get('kafka_dir', '/opt/kafka')
    download_url         = config['program_paths'].get('kafka_download_url', '')
    kafka_log_local_src  = config['program_paths']['kafka_log_local_src']
    kafka_log_jar_remote = config['program_paths']['kafka_log_jar_remote']

    if not download_url:
        print("ERROR: kafka_download_url not set in [program_paths].")
        return False

    # --- Install Java on all nodes ---
    all_ips = list(dict.fromkeys(seq_ips + cli_ips))
    print("\n--- Installing Java on all nodes ---")
    java_install_cmd = (
        'if ! command -v java &>/dev/null; then '
        'sudo apt-get update -qq && sudo apt-get install -y -qq default-jre-headless; '
        'fi'
    )
    for ip in all_ips:
        print(f"  Ensuring Java on {ip}...")
        if not run_remote_command_sync(ip, java_install_cmd, ssh_key, ssh_user):
            print(f"ERROR: Failed to install Java on {ip}")
            return False

    # --- Download Kafka broker distribution on each broker node ---
    print("\n--- Installing Kafka broker on broker nodes ---")
    for ip in seq_ips:
        install_cmd = (
            f'if [ ! -d {kafka_dir} ]; then '
            f'wget {download_url} -O /tmp/kafka.tgz && '
            f'sudo mkdir -p {kafka_dir} && '
            f'sudo tar -xzf /tmp/kafka.tgz -C {kafka_dir} --strip-components=1 && '
            f'sudo chmod -R 755 {kafka_dir} && '
            f'rm /tmp/kafka.tgz; '
            f'fi'
        )
        print(f"  Installing Kafka on {ip}...")
        if not run_remote_command_sync(ip, install_cmd, ssh_key, ssh_user):
            print(f"ERROR: Failed to install Kafka broker on {ip}")
            return False

    # --- Build fat jar locally ---
    if not os.path.isdir(kafka_log_local_src):
        print(f"\nCloning kafka-log into {kafka_log_local_src}...")
        try:
            subprocess.run(
                ['git', 'clone', 'https://github.com/mmurray22/kafka-log', kafka_log_local_src],
                check=True, stdin=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to clone kafka-log: {e}")
            return False

    # Inject sbt-assembly plugin (not present in the upstream repo)
    plugins_sbt_path = os.path.join(kafka_log_local_src, 'project', 'plugins.sbt')
    with open(plugins_sbt_path, 'w') as f:
        f.write('addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")\n')

    # Add merge strategy to build.sbt to suppress deduplicate errors
    build_sbt_path = os.path.join(kafka_log_local_src, 'build.sbt')
    with open(build_sbt_path, 'a') as f:
        f.write('\nassemblyMergeStrategy in assembly := {\n'
                '  case PathList("META-INF", _*) => MergeStrategy.discard\n'
                '  case _                        => MergeStrategy.first\n'
                '}\n')

    print("\nBuilding kafka-log fat jar locally (sbt assembly)...")
    try:
        subprocess.run(['sbt', 'assembly'], cwd=kafka_log_local_src,
                       check=True, stdin=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sbt assembly failed: {e}")
        return False

    jar_matches = glob.glob(
        os.path.join(kafka_log_local_src, 'target', '**', '*assembly*.jar'), recursive=True
    )
    if not jar_matches:
        print("ERROR: Could not find assembled jar after sbt assembly.")
        return False
    local_jar = jar_matches[0]
    print(f"  Built jar: {local_jar}")

    # --- SCP fat jar to each client node ---
    print("\n--- Distributing kafka-log jar to client nodes ---")
    jar_filename   = os.path.basename(kafka_log_jar_remote)
    jar_remote_dir = os.path.dirname(kafka_log_jar_remote)
    for ip in cli_ips:
        print(f"  Sending jar to {ip}:{kafka_log_jar_remote}...")
        if not transfer_file(local_jar, ip, ssh_user, ssh_key, remote_filename=jar_filename):
            print(f"ERROR: Failed to transfer kafka-log jar to {ip}")
            return False
        if jar_remote_dir and jar_remote_dir != '~':
            move_cmd = f'mkdir -p {jar_remote_dir} && mv ~/{jar_filename} {kafka_log_jar_remote}'
            if not run_remote_command_sync(ip, move_cmd, ssh_key, ssh_user):
                print(f"ERROR: Failed to install kafka-log jar on {ip}")
                return False

    print("--- Kafka setup complete ---")
    return True


def setup_scalog_nodes(config, ssh_key, ssh_user):
    """
    Builds Scalog binaries locally (cross-compiled for Linux amd64) then SCPs each
    binary to only the nodes that need it. Remote machines need no build tooling.

    Requires in [program_paths]:
      scalog_local_src - local path to clone the Scalog repo into for building
      path_discovery   - full remote path for the discovery binary (deployed to seq_ips[0])
      path_order       - full remote path for the order binary    (deployed to all seq_ips)
      path_data        - full remote path for the data binary     (deployed to all stor_ips)
      path_client      - full remote path for the client binary   (deployed to all cli_ips)

    TODO: verify cmd/ subdirectory names match the Scalog repo layout and update
    the cmd_binaries dict below if they differ.
    """
    seq_ips          = config['network_setup'].get('seq_ips', [])
    stor_ips         = config['network_setup'].get('stor_ips', [])
    cli_ips          = config['network_setup']['cli_ips']
    scalog_local_src = config['program_paths']['scalog_local_src']
    paths            = config['program_paths']

    # Map: local binary name → (cmd subdirectory, remote path, list of target IPs)
    # TODO: update cmd/ subdirectory names if the Scalog repo layout differs.
    cmd_binaries = {
        'discovery': (f'./cmd/discovery', paths['path_discovery'], [seq_ips[0]]),
        'order':     (f'./cmd/order',     paths['path_order'],     seq_ips),
        'data':      (f'./cmd/data',      paths['path_data'],      stor_ips),
        'client':    (f'./cmd/client',    paths['path_client'],    cli_ips),
    }

    # --- Clone locally if not already present ---
    if not os.path.isdir(scalog_local_src):
        print(f"\nCloning Scalog into {scalog_local_src}...")
        try:
            subprocess.run(
                ['git', 'clone', 'https://github.com/chn0318/scalog', scalog_local_src],
                check=True, stdin=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to clone Scalog: {e}")
            return False

    # --- Cross-compile each binary for Linux amd64 ---
    local_build_dir = os.path.join(scalog_local_src, '_build_linux_amd64')
    os.makedirs(local_build_dir, exist_ok=True)
    linux_env = {**os.environ, 'GOOS': 'linux', 'GOARCH': 'amd64', 'CGO_ENABLED': '0'}

    print("\nCross-compiling Scalog binaries for Linux amd64...")
    for binary_name, (cmd_pkg, remote_path, _) in cmd_binaries.items():
        local_binary = os.path.join(local_build_dir, binary_name)
        print(f"  Building {binary_name}...")
        try:
            subprocess.run(
                ['go', 'build', '-mod=vendor', '-o', local_binary, cmd_pkg],
                cwd=scalog_local_src, env=linux_env,
                check=True, stdin=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to build Scalog {binary_name}: {e}")
            return False

    # --- SCP each binary to its target nodes ---
    print("\n--- Distributing Scalog binaries to remote nodes ---")
    for binary_name, (_, remote_path, target_ips) in cmd_binaries.items():
        local_binary   = os.path.join(local_build_dir, binary_name)
        remote_dir     = os.path.dirname(remote_path)
        for ip in target_ips:
            print(f"  Sending {binary_name} → {ip}:{remote_path}...")
            if not transfer_file(local_binary, ip, ssh_user, ssh_key, remote_filename=binary_name):
                print(f"ERROR: Failed to transfer {binary_name} to {ip}")
                return False
            install_cmd = f'mkdir -p {remote_dir} && mv ~/{binary_name} {remote_path} && chmod +x {remote_path}'
            if not run_remote_command_sync(ip, install_cmd, ssh_key, ssh_user):
                print(f"ERROR: Failed to install {binary_name} on {ip}")
                return False
    print("--- Scalog setup complete ---")
    return True

def run_setup_script(ip, setup_script_path, ssh_key, ssh_user):
    """Runs the specified setup script on a remote machine synchronously (blocking run)."""
    command = [
        'ssh',
        '-i', ssh_key,
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
def run_experiment_cycle_pringles(base_config, pringles_config_file, exp_index, local_results_dir, system_name, with_tunnel=False):
    """Runs a single, full experiment cycle based on the merged configuration."""
    try:
        with open(pringles_config_file, 'r') as f:
            full_config = toml.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{pringles_config_file}' not found.")
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
    config = full_config.copy() 

    #print("\n--- Compiling Binaries on All Machines ---") TODO do I need this?
    #for ip in all_ips:
    #    if not run_compile_command(ip, ssh_key, ssh_user):
    #        print("FATAL: Compilation failed on at least one machine. Aborting experiment.")
    #        return
    #print("--- Compilation Complete ---")

    for exp_index, exp_params in enumerate(experiments_to_run): 
       # 2. Merge experiment-specific parameters (overrides)
        for key, value in exp_params.items():
            print(key)
            print(value)
            if key in config.get('experiment_parameters', {}):
                config['experiment_parameters'][key] = value
        
        # Extract run-specific parameters from the merged config
        ssh_key = os.path.expanduser(config['network_setup']['ssh_key'])
        ssh_user = config['network_setup']['ssh_user']
        client_ips = config['network_setup']['cli_ips'] # TODO SEQ make plural?
        cli_net_ifs = config['network_setup']['cli_net_ifs'] # TODO SEQ make plural?
        switch_ip = config['network_setup']['switch_ip']
        switch_net_if = config['network_setup']['switch_net_if']
        all_server_ips = config['network_setup']['stor_ips']
        server_net_ifs = config['network_setup']['stor_net_ifs']
        path_client = f"{pringles_base}" + config['program_paths']['path_client']
        path_server = f"{pringles_base}" + config['program_paths']['path_server']
        path_switch = f"{pringles_base}" + config['program_paths']['path_switch']

        general_json_output_name = base_config['experiment_parameters']['general_json_name']
        json_output_name = general_json_output_name + "-" + config['experiment_parameters']['json_name']

        num_failures = config['experiment_parameters']['num_failures']

        print(f"\n========================================================")
        print(f"   RUNNING EXPERIMENT {exp_index + 1}: {json_output_name}")
        print(f"   Client Threads: {base_config['experiment_parameters']['num_client_threads']}")
        print(f"========================================================")

        server_processes = []
        client_log_files = []
        client_processes = []
        server_log_files = {} # MODIFIED: Dictionary to store the log file name for each server
        switch_processes = []
        switch_log_files = {} # MODIFIED: Dictionary to store the log file name for each server

        # Setup shards TODO only for a single switch
        server_ips = []
        total_list_of_shards = []
        total_list_of_shards_yaml = []
        shard_to_multicast_addr = []
        ip_to_shard = {}
        base_multicast_addr = "239.1.1."
        if config['experiment_parameters']['use_shard']: 
            size_of_shard = base_config['experiment_parameters']['num_servers_per_shard']
            print(f"Size of shards: {size_of_shard}")
            num_of_shard = 0
            max_num_shards = base_config['experiment_parameters']['num_shards']
            for i in range(0, len(all_server_ips), size_of_shard): # TODO check number of servers divides evenly into shard size
                shard = []
                shard_yaml = []
                for j in range(i, i+size_of_shard):
                    shard.append(all_server_ips[j])
                    server_ips.append(all_server_ips[j])
                    shard_yaml.append(QuotedString(all_server_ips[j]))
                    ip_to_shard[all_server_ips[j]] = num_of_shard
                total_list_of_shards.append(shard)
                total_list_of_shards_yaml.append(shard_yaml)
                num_of_shard += 1
                if (num_of_shard == max_num_shards):
                    break
            for i in range(0, num_of_shard): #255 <-- TODO max number of shards
                shard_to_multicast_addr.append((base_multicast_addr + str(i)));
            yaml_shard_to_multicast_addr = [QuotedString(addr) for addr in shard_to_multicast_addr]
            print(yaml_shard_to_multicast_addr)
        else:
            total_list_of_shards.append([all_server_ips[0]])
            total_list_of_shards_yaml.append([all_server_ips[0]])
            ip_to_shard[all_server_ips[0]] = 0
            shard_to_multicast_addr.append((base_multicast_addr + str(0)));
            server_ips.append(all_server_ips[0])
        print(total_list_of_shards_yaml)
        print(total_list_of_shards)

        try:
            # --- 5. Generate Server Configurations and Start Processes ---
            print("\n--- Starting Storage Servers ---")
            for shard in total_list_of_shards: 
                print(shard)
                for i, ip in enumerate(shard):
                    print(ip)
                    print(server_ips)
                    server_id = random.randint(100000, 999999) 
                    config_filename = f"server_config_{json_output_name}_{i}.yaml" # Unique filename
                    port_offset = i * 2
                    print(config_filename)
                    server_config = generate_yaml_config(
                        base_config,
                        config, 
                        'server', 
                        ip, 
                        port_offset,
                        server_ips,
                        entity_id=server_id,
                        entity_idx=i,
                        network_interface=server_net_ifs[i],
                        shard_id=ip_to_shard[ip],
                        shard_multicast=shard_to_multicast_addr[ip_to_shard[ip]]
                    )
                    print("Done with the YAML file!")

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
            if not config['experiment_parameters']['use_hardware_switch']:
                print("\n--- Starting Switch ---")
                switch_id = random.randint(100000, 999999)
                switch_config_filename = f"switch_config_{json_output_name}.yaml" # Unique filename
                switch_port_offset = len(server_ips) * 2

                switch_config = generate_yaml_config(
                    base_config,
                    config, 
                    'switch',
                    switch_ip,
                    switch_port_offset,
                    server_ips,
                    entity_id=switch_id,
                    json_name=json_output_name,
                    num_failures=num_failures,
                    network_interface=switch_net_if,
                    total_shards=total_list_of_shards_yaml
                )

                switch_exec = os.path.basename(path_switch) # Use the basename remotely
                kill_process(switch_exec, ssh_key, ssh_user, switch_ip)

                # Write YAML file locally
                with open(switch_config_filename, 'w') as f:
                    yaml.dump(switch_config, f, default_flow_style=False)
                print(f"Generated client config: {switch_config_filename}")

                # TRANSFER THE YAML CONFIG FILE TO THE REMOTE SERVER
                if not transfer_file(switch_config_filename, switch_ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer config to server {switch_ip}")
                if not transfer_file(path_switch, switch_ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer switch binary to switch {switch_ip}")
                    
                # Start remote process
                exec_switch_filepath = "~/" + switch_exec
                prefix = "switch"
                process, log_filename = execute_remote_command(switch_ip, exec_switch_filepath, switch_config_filename, ssh_key, ssh_user, exp_index, prefix) # MODIFIED: Get log filename
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
                client_port_offset = len(server_ips) * 3
                client_config = generate_yaml_config(
                    base_config,
                     config, 
                     'client',
                     ip,
                     client_port_offset,
                     server_ips,
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

            print("Switch processes are assumed to exit on their own after the client terminates.") # TODO
            process_and_aggregate_results(local_results_dir, json_output_name, system_name, base_config, config)

def run_experiment_cycle_hardware(config, hardware_config, exp_index, local_results_dir, system_name, with_tunnel=False): # TODO
    switches_up = True
    switches_up = setup_switches(hardware_config)
    num_switches = len(base_config['experiment_parameters']['switches_in_ring'])
    if switches_up:
        print("--- All {num_switches} switches up and running ---")

def generate_kafka_server_properties(node_id, ip, all_seq_ips, num_partitions, replication_factor, log_dir):
    """Returns KRaft-mode server.properties content for a single Kafka broker node."""
    # Each node acts as both broker and controller (combined mode).
    # controller.quorum.voters lists every node's controller port.
    controller_quorum_voters = ','.join(
        f'{i + 1}@{seq_ip}:9093' for i, seq_ip in enumerate(all_seq_ips)
    )
    # Internal replication topics must have RF <= num brokers.
    internal_rf = min(replication_factor, len(all_seq_ips))
    return f"""\
# KRaft mode (no ZooKeeper)
node.id={node_id}
process.roles=broker,controller
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://{ip}:9092
controller.quorum.voters={controller_quorum_voters}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={log_dir}
num.partitions={num_partitions}
default.replication.factor={replication_factor}
offsets.topic.replication.factor={internal_rf}
transaction.state.log.replication.factor={internal_rf}
transaction.state.log.min.isr=1
"""


def parse_kafka_results(consumer_output_path, consumer_log_path, json_name, exp_index, local_results_dir):
    """
    Converts kafka-log consumer output into the standard result schema and writes
    <json_name>_<exp_index>_kafka.json into local_results_dir.

    Throughput comes from the consumer's JSON output file (Overall_Throughput_MPS).
    Average latency is computed from per-message lines in the consumer stdout log:
      "Kafka message processing latency: 42ms"
    Note: this latency is one-way (broker timestamp → consumer receipt), not full
    round-trip from producer send. It is the closest proxy available without
    modifying the kafka-log source.
    """
    throughput = 0.0
    avg_latency = 0.0

    try:
        with open(consumer_output_path, 'r') as f:
            data = json.load(f)
        throughput = float(data.get('Overall_Throughput_MPS', 0.0))
    except Exception as e:
        print(f"WARNING: Could not parse Kafka consumer output JSON at {consumer_output_path}: {e}")

    latencies = []
    try:
        with open(consumer_log_path, 'r') as f:
            for line in f:
                if 'Kafka message processing latency:' in line:
                    parts = line.strip().split()
                    if parts:
                        try:
                            latencies.append(float(parts[-1].replace('ms', '')))
                        except ValueError:
                            pass
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
    except Exception as e:
        print(f"WARNING: Could not parse Kafka consumer log at {consumer_log_path}: {e}")

    result = {'throughput': throughput, 'avg_latency': avg_latency, 'batch_size': 1}
    output_path = os.path.join(local_results_dir, f'{json_name}_{exp_index}_kafka.json')
    try:
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=4)
        print(f"Kafka results written to: {output_path}")
    except IOError as e:
        print(f"ERROR: Could not write Kafka results to {output_path}: {e}")
    return output_path


def run_experiment_cycle_kafka(config, kafka_config_file, exp_index, local_results_dir, system_name):
    """
    Runs a single experiment cycle for the Kafka comparison system.

    Expects in TOML:
      [network_setup]          seq_ips, cli_ips, ssh_key, ssh_user
      [program_paths]          kafka_dir, kafka_log_jar_remote
      [comparison_parameters]  num_partitions, replication_factor
      [experiment_parameters]  json_name, experiment_duration, warm_up, cool_down, message_size

    NOTE: Multi-partition scaling requires running multiple Producer instances (one per
    partition), which is not yet implemented. Keep num_partitions=1 for now.
    """
    CONSUMER_HEAD_START = 5  # seconds; fat jar JVM startup is fast (~1-2s)

    ssh_key   = os.path.expanduser(config['network_setup']['ssh_key'])
    ssh_user  = config['network_setup']['ssh_user']
    client_ips = config['network_setup']['cli_ips']
    seq_ips    = config['network_setup'].get('seq_ips', [])

    if not seq_ips:
        raise Exception("No seq_ips defined for Kafka brokers in [network_setup].")

    cmp  = config.get('comparison_parameters', {})
    num_partitions     = cmp.get('num_partitions', 1)
    replication_factor = cmp.get('replication_factor', 2)

    if replication_factor > len(seq_ips):
        raise Exception(
            f"replication_factor ({replication_factor}) > number of brokers ({len(seq_ips)}). "
            f"Reduce replication_factor or add more seq_ips."
        )

    exp     = config['experiment_parameters']
    json_output_name   = exp['json_name']
    experiment_duration = exp['experiment_duration']
    warm_up             = exp['warm_up']
    cool_down           = exp['cool_down']
    message_size        = exp['message_size']

    kafka_dir            = config['program_paths'].get('kafka_dir', '/opt/kafka')
    kafka_log_jar_remote = config['program_paths']['kafka_log_jar_remote']
    kafka_broker_log_dir = '/tmp/kafka-logs'

    broker_ips_str = ','.join(f'{ip}:9092' for ip in seq_ips)
    topic_name     = 'pringles-bench'
    cluster_uuid   = str(uuid.uuid4())
    message_payload = 'x' * message_size

    consumer_log_filename    = f'kafka_consumer_{json_output_name}_{exp_index}.log'
    producer_log_filename    = f'kafka_producer_{json_output_name}_{exp_index}.log'
    consumer_output_filename = f'kafka_output_{json_output_name}_{exp_index}.json'
    consumer_output_remote   = f'/tmp/{consumer_output_filename}'

    broker_log_files = {}
    client_ip = client_ips[0]

    print(f"\n========================================================")
    print(f"   RUNNING KAFKA EXPERIMENT {exp_index + 1}: {json_output_name}")
    print(f"   Brokers: {seq_ips}  |  Partitions: {num_partitions}  |  RF: {replication_factor}")
    print(f"========================================================")

    try:
        # --- 0. Kill any stale consumer/producer from previous runs ---
        print("\n--- Killing any stale consumer/producer processes ---")
        kill_remote_process(client_ip, 'main.Consumer', ssh_key, ssh_user)
        kill_remote_process(client_ip, 'main.Producer', ssh_key, ssh_user)

        # --- 1. Generate and transfer server.properties to each broker ---
        print("\n--- Configuring Kafka Brokers ---")
        for i, ip in enumerate(seq_ips):
            props = generate_kafka_server_properties(
                node_id=i + 1,
                ip=ip,
                all_seq_ips=seq_ips,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                log_dir=kafka_broker_log_dir,
            )
            props_filename = f'kafka_server_{json_output_name}_{i}.properties'
            with open(props_filename, 'w') as f:
                f.write(props)
            if not transfer_file(props_filename, ip, ssh_user, ssh_key):
                raise Exception(f"Failed to transfer server.properties to {ip}")

        # --- 2. Format storage on each broker (sync) ---
        print("\n--- Formatting Kafka Storage ---")
        for i, ip in enumerate(seq_ips):
            props_filename = f'kafka_server_{json_output_name}_{i}.properties'
            if not run_remote_command_sync(ip, f'sudo rm -rf {kafka_broker_log_dir}', ssh_key, ssh_user):
                raise Exception(f"Failed to clean Kafka log dir on {ip}")
            format_cmd = f'{kafka_dir}/bin/kafka-storage.sh format -t {cluster_uuid} -c ~/{props_filename}'
            if not run_remote_command_sync(ip, format_cmd, ssh_key, ssh_user):
                raise Exception(f"Failed to format Kafka storage on {ip}")

        # --- 3. Start brokers (async) ---
        print("\n--- Starting Kafka Brokers ---")
        for i, ip in enumerate(seq_ips):
            props_filename = f'kafka_server_{json_output_name}_{i}.properties'
            process, log_filename = execute_remote_command(
                ip, f'{kafka_dir}/bin/kafka-server-start.sh', props_filename,
                ssh_key, ssh_user, exp_index
            )
            if not process:
                raise Exception(f"Failed to start Kafka broker on {ip}")
            broker_log_files[ip] = log_filename

        print(f"\nWaiting {SERVER_START_DELAY}s for brokers to initialize...")
        time.sleep(SERVER_START_DELAY)

        # --- 4. Create topic (sync) ---
        print(f"\n--- Creating topic '{topic_name}' ---")
        create_topic_cmd = (
            f'{kafka_dir}/bin/kafka-topics.sh --create'
            f' --bootstrap-server {seq_ips[0]}:9092'
            f' --topic {topic_name}'
            f' --partitions {num_partitions}'
            f' --replication-factor {replication_factor}'
        )
        if not run_remote_command_sync(seq_ips[0], create_topic_cmd, ssh_key, ssh_user):
            raise Exception("Failed to create Kafka topic.")

        # --- 5. Generate config.json and copy to kafka-log resources ---
        client_config = {
            'topic1': topic_name,
            'topic2': topic_name,  # same topic so producer and consumer share it
            'rsm_id': 1,
            'node_id': 0,          # with rsm_size=1, node_id=0 ensures all messages are sent
            'broker_ips': broker_ips_str,
            'rsm_size': 1,         # TODO: >1 requires multiple producer instances
            'benchmark_duration': experiment_duration,
            'warmup_duration': warm_up,
            'cooldown_duration': cool_down,
            'message': message_payload,
            'read_from_pipe': False,
            'input_path': '/tmp/kafka-input',
            'output_path': consumer_output_remote,
            'write_dr': False,
            'write_ccf': False,
        }
        config_json_filename = f'kafka_client_config_{json_output_name}_{exp_index}.json'
        with open(config_json_filename, 'w') as f:
            json.dump(client_config, f, indent=4)
        if not transfer_file(config_json_filename, client_ip, ssh_user, ssh_key):
            raise Exception(f"Failed to transfer client config.json to {client_ip}")

        # Place config where the fat jar's classpath will find it (classpath override).
        # /tmp/kafka-config/ is prepended to the classpath, shadowing the bundled config.json.
        setup_config_cmd = (
            f'mkdir -p /tmp/kafka-config && cp ~/{config_json_filename} /tmp/kafka-config/config.json'
        )
        if not run_remote_command_sync(client_ip, setup_config_cmd, ssh_key, ssh_user):
            raise Exception("Failed to place config.json in /tmp/kafka-config/.")

        jar_cp = kafka_log_jar_remote.replace('~/', '$HOME/', 1)

        print("\n--- Config sent to client ---")
        run_remote_command_sync(client_ip, 'cat /tmp/kafka-config/config.json', ssh_key, ssh_user)
        print("\n--- Bundled config.json from jar ---")
        run_remote_command_sync(client_ip, f'unzip -p {jar_cp} config.json 2>/dev/null || echo "(no bundled config.json found in jar)"', ssh_key, ssh_user)

        # --- 6. Start Consumer (async, head start before producer) ---
        # Consumer uses auto.offset.reset=latest so it must be subscribed before
        # the producer sends. java -jar starts in ~1-2s, so CONSUMER_HEAD_START=5 is enough.
        print(f"\n--- Starting Kafka Consumer (will wait {CONSUMER_HEAD_START}s before producer) ---")
        consumer_cmd = f'nohup java -cp /tmp/kafka-config:{jar_cp} main.Consumer > ~/{consumer_log_filename} 2>&1 &'
        run_remote_command_sync(client_ip, consumer_cmd, ssh_key, ssh_user)
        time.sleep(3)
        print("\n--- Consumer liveness check (3s after launch) ---")
        run_remote_command_sync(client_ip, 'pgrep -a -f "main.Consumer" || echo "NOT RUNNING"', ssh_key, ssh_user)
        print("--- Consumer log so far ---")
        run_remote_command_sync(client_ip, f'cat ~/{consumer_log_filename}', ssh_key, ssh_user)
        time.sleep(CONSUMER_HEAD_START - 3)

        # --- 7. Start Producer (async) ---
        print("\n--- Starting Kafka Producer ---")
        producer_cmd = f'nohup java -cp /tmp/kafka-config:{jar_cp} main.Producer > ~/{producer_log_filename} 2>&1 &'
        run_remote_command_sync(client_ip, producer_cmd, ssh_key, ssh_user)

        # --- 8. Wait for experiment to complete ---
        total_wait = warm_up + experiment_duration + cool_down + 15  # 15s buffer for JVM teardown/flush
        print(f"\nWaiting {total_wait}s for experiment to complete...")
        time.sleep(total_wait)

        # --- 9. Stop client processes so shutdown hooks flush the output JSON ---
        print("\n--- Stopping consumer/producer (triggers JSON flush) ---")
        kill_remote_process(client_ip, 'main.Consumer', ssh_key, ssh_user)
        kill_remote_process(client_ip, 'main.Producer', ssh_key, ssh_user)
        print("Waiting 5s for JVM shutdown hooks to complete...")
        time.sleep(5)

        # --- 10. Retrieve results ---
        print("\n--- Retrieving Kafka Results ---")
        consumer_output_local = os.path.join(local_results_dir, consumer_output_filename)
        consumer_log_local    = os.path.join(local_results_dir, consumer_log_filename)

        json_ok = copy_log_file_back(client_ip, ssh_user, ssh_key, consumer_output_filename, local_results_dir, remote_dir='/tmp')
        copy_log_file_back(client_ip, ssh_user, ssh_key, consumer_log_filename,    local_results_dir)
        copy_log_file_back(client_ip, ssh_user, ssh_key, producer_log_filename,    local_results_dir)

        json_empty = json_ok and os.path.getsize(consumer_output_local) == 0
        if not json_ok or json_empty:
            if os.path.exists(consumer_log_local):
                print("\n--- Consumer log ---")
                with open(consumer_log_local) as f:
                    print(f.read())
                print("--- End consumer log ---")
            producer_log_local = os.path.join(local_results_dir, producer_log_filename)
            if os.path.exists(producer_log_local):
                print("\n--- Producer log ---")
                with open(producer_log_local) as f:
                    print(f.read())
                print("--- End producer log ---")

        # --- 10. Parse and normalize results ---
        parse_kafka_results(
            consumer_output_local, consumer_log_local,
            json_output_name, exp_index, local_results_dir
        )

    except Exception as e:
        print(f"\nFATAL ERROR during Kafka experiment cycle {exp_index + 1}: {e}")

    finally:
        # --- 11. Stop all brokers ---
        print("\n--- Stopping Kafka Brokers ---")
        for ip in seq_ips:
            kill_remote_process(ip, 'kafka.Kafka', ssh_key, ssh_user)
        for ip, log_filename in broker_log_files.items():
            copy_log_file_back(ip, ssh_user, ssh_key, log_filename, local_results_dir)


def generate_scalog_config(discovery_ip, order_ips, data_ips, order_replication_factor, data_replication_factor,
                           batching_interval,
                           discovery_port=23472, order_port=26733, data_port=23282, raft_port=27238):
    """
    Generates .scalog.yaml content for the Scalog cluster.
    """
    cfg = {
        'order-port': order_port,
        'raft-port': raft_port,
        'data-port': data_port,
        'disc-port': discovery_port,
        'disc-ip': QuotedString(discovery_ip),
        'order-replication-factor': order_replication_factor,
        'data-replication-factor': data_replication_factor,
        'order-batching-interval': QuotedString(batching_interval),
        'data-batching-interval': QuotedString(batching_interval),
    }
    for idx in range(0, len(order_ips)):
        key = "order-" + str(idx) + "-ip"
        print(key)
        cfg.update({
            key: QuotedString(order_ips[idx]),
        })
    for idx in range(0, len(data_ips)):
        key = "data-0-" + str(idx) + "-ip" # TODO: 
        print(key)
        cfg.update({
            key: QuotedString(data_ips[idx]),
        })
    return yaml.dump(cfg, default_flow_style=False)

def generate_scalog_stats_config(json_name, client_ip):
    """
    Generates .scalog.yaml content for the Scalog cluster.
    """
    cfg = {
        'json_name': json_name,
        'client_ip': QuotedString(client_ip),
    }
    return yaml.dump(cfg, default_flow_style=False)


def parse_scalog_results(client_log_path, json_name, exp_index, local_results_dir):
    """
    Converts Scalog client benchmark output into the standard result schema and writes
    <json_name>_<exp_index>_scalog.json into local_results_dir.

    TODO: Confirm the actual output format of the Scalog benchmark client binary and
    update the parsing logic below accordingly. The current implementation looks for
    common patterns; update once confirmed from the source.
    """
    throughput  = 0.0
    avg_latency = 0.0

    try:
        with open(client_log_path, 'r') as f:
            for line in f:
                line_lower = line.lower()
                # TODO: replace these with actual output patterns from the Scalog client
                if 'throughput' in line_lower:
                    # Expected pattern: "Throughput: 100000.0 ops/sec" or similar
                    parts = line.strip().split()
                    for part in parts:
                        try:
                            throughput = float(part.replace(',', ''))
                            break
                        except ValueError:
                            continue
                elif 'latency' in line_lower and 'avg' in line_lower:
                    # Expected pattern: "Avg latency: 1.5 ms" or similar
                    parts = line.strip().split()
                    for part in parts:
                        try:
                            avg_latency = float(part.replace('ms', '').replace(',', ''))
                            break
                        except ValueError:
                            continue
    except Exception as e:
        print(f"WARNING: Could not parse Scalog client log at {client_log_path}: {e}")

    result = {'throughput': throughput, 'avg_latency': avg_latency, 'batch_size': 1}
    output_path = os.path.join(local_results_dir, f'{json_name}_{exp_index}_scalog.json')
    try:
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=4)
        print(f"Scalog results written to: {output_path}")
    except IOError as e:
        print(f"ERROR: Could not write Scalog results to {output_path}: {e}")
    return output_path


def run_experiment_cycle_scalog(base_config, scalog_config_file, exp_index, local_results_dir, system_name):
    """
    Runs a single experiment cycle for the Scalog comparison system.

    Expects in TOML:
      [network_setup]         seq_ips (order nodes), stor_ips (data nodes), cli_ips, ssh_key, ssh_user
      [program_paths]         path_discovery, path_order, path_data, path_client
                              (paths to compiled Scalog binaries on the remote machines)
      [experiment_parameters] json_name, experiment_duration, warm_up, cool_down, message_size, num_shards, num_sequencer_nodes, order/data replication_factor

    Component startup order: discovery → order nodes → data nodes → client.
    Each component is started independently via SSH using execute_remote_command.
    The discovery node is run on seq_ips[0]; it can share the machine with an order node.

    TODO: Verify parse_scalog_results output patterns against actual client output.
    """
    try:
        with open(scalog_config_file, 'r') as f:
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
    config = full_config.copy() 

    #print("\n--- Compiling Binaries on All Machines ---") TODO do I need this?
    #for ip in all_ips:
    #    if not run_compile_command(ip, ssh_key, ssh_user):
    #        print("FATAL: Compilation failed on at least one machine. Aborting experiment.")
    #        return
    #print("--- Compilation Complete ---")

    for exp_index, exp_params in enumerate(experiments_to_run): 
        # 2. Merge experiment-specific parameters (overrides)
        for key, value in exp_params.items():
            print(key)
            print(value)
            if key in config.get('experiment_parameters', {}):
                config['experiment_parameters'][key] = value
     
        ssh_key   = os.path.expanduser(config['network_setup']['ssh_key'])
        ssh_user  = config['network_setup']['ssh_user']
        client_ips = config['network_setup']['cli_ips']
        seq_ips    = config['network_setup'].get('seq_ips', [])   # order layer nodes
        stor_ips   = config['network_setup']['stor_ips']           # data layer nodes
        discovery_ip   = config['network_setup']['discovery_ip']           # discovery layer nodes
        client_ip     = config['network_setup']['cli_ips'][0]
        order_port = config['network_setup']['order_port']
        raft_port = config['network_setup']['raft_port']
        data_port = config['network_setup']['data_port']
        disc_port = config['network_setup']['disc_port']
        data_yaml_path = f"{pringles_base}" + config['program_paths']['data_yaml_path']

        if not seq_ips:
            raise Exception("No seq_ips defined for Scalog order nodes in [network_setup].")
        if not stor_ips:
            raise Exception("No stor_ips defined for Scalog data nodes in [network_setup].")

        exp              = config['experiment_parameters']
        general_json_output_name = base_config['experiment_parameters']['general_json_name']
        json_output_name = general_json_output_name + "-" + exp['json_name']
        num_sequencer_nodes = exp['num_sequencer_nodes']
        #data_replication_factor  = exp['data_replication_factor']
        order_replication_factor  = exp['order_replication_factor']
        batching_interval = exp['batching_interval']
        scalog_exec = f"{pringles_base}" + config['program_paths']['scalog_bin']
        config_path = f"{pringles_base}" + config['program_paths']['config_path']
        
        general_exp = base_config['experiment_parameters']
        message_size     = general_exp['message_size']
        num_client_threads = general_exp['num_client_threads']
        duration         = general_exp['experiment_duration']
        warm_up          = general_exp['warm_up']
        cool_down        = general_exp['cool_down']
        num_shards = general_exp['num_shards']
        data_replication_factor  = general_exp['num_servers_per_shard']

        if num_sequencer_nodes > len(seq_ips):
            raise Exception(f"num_sequencer_nodes ({num_sequencer_nodes}) > len(seq_ips) ({len(seq_ips)}).")
        if num_shards > len(stor_ips):
            raise Exception(f"num_shards ({num_shards}) > len(stor_ips) ({len(stor_ips)}).")

        active_order  = seq_ips[:num_sequencer_nodes]
        active_data   = stor_ips[:num_shards]

        scalog_yaml_filename = f'scalog_{json_output_name}_{exp_index}.yaml'
        client_log_filename  = f'scalog_client_{json_output_name}_{exp_index}.log'
        scalog_stats_yaml_filename = 'data.yaml'

        component_log_files  = {}   # ip -> log filename, for cleanup retrieval

        print(f"\n========================================================")
        print(f"   RUNNING SCALOG EXPERIMENT {exp_index + 1}: {json_output_name}")
        print(f"   Order nodes: {active_order}")
        print(f"   Data  nodes: {active_data}")
        print(f"   RF: {order_replication_factor} and {data_replication_factor}")
        print(f"========================================================")

        try:
            # --- 1. Generate and distribute .scalog.yaml ---
            print("\n--- Generating Scalog config ---")
            scalog_yaml = generate_scalog_config(
                    discovery_ip, 
                    active_order, 
                    active_data, 
                    order_replication_factor, 
                    data_replication_factor,
                    batching_interval,
                    disc_port, 
                    order_port, 
                    data_port, 
                    raft_port
            )
            with open(scalog_yaml_filename, 'w') as f:
                f.write(scalog_yaml)
            print("Done generating Scalog config!")
            stats_yaml = generate_scalog_stats_config(json_output_name, client_ip)
            with open(scalog_stats_yaml_filename, 'w') as f:
                f.write(stats_yaml)
            print("Done generating Scalog data.yaml config!")

            all_scalog_nodes = list(dict.fromkeys(active_order + active_data + [discovery_ip] + [client_ip]))
            for ip in all_scalog_nodes:
                if not transfer_file(scalog_yaml_filename, ip, ssh_user, ssh_key):
                    raise Exception(f"Failed to transfer scalog config to {ip}")
            transfer_file(scalog_stats_yaml_filename, client_ip, ssh_user, ssh_key, None, data_yaml_path)

            # --- 2. Start order layer nodes ---
            print("\n--- Starting Scalog order nodes ---")
            for idx in range(0, len(active_order)):
                path_order_exec = f"sudo {scalog_exec} order -i {str(idx)} --config"
                print(path_order_exec)
                process, log_filename = execute_remote_command(
                    active_order[idx], path_order_exec, scalog_yaml_filename, ssh_key, ssh_user, exp_index, json_output_name
                )
                if not process:
                    raise Exception(f"Failed to start order node on {ip}")
                component_log_files[f'order_{active_order[idx]}'] = (active_order[idx], log_filename)

            time.sleep(2)  # order nodes register with discovery before data nodes connect


            # --- 3. Start discovery node ---
            print(f"\n--- Starting Scalog discovery node on {discovery_ip} ---")
            path_order_discovery = f"sudo {scalog_exec} discovery --config"
            print(path_order_discovery)
            process, log_filename = execute_remote_command(
                discovery_ip, path_order_discovery, scalog_yaml_filename, ssh_key, ssh_user, exp_index, json_output_name
            )
            if not process:
                raise Exception(f"Failed to start discovery node on {discovery_ip}")
            component_log_files[f'discovery_{discovery_ip}'] = (discovery_ip, log_filename)

            time.sleep(2)  # discovery must be up before order nodes connect

            # --- 4. Start data layer nodes ---
            print("\n--- Starting Scalog data nodes ---")
            for shard in range(0, num_shards):
                for idx in range(0, len(active_data)):
                    path_data_exec = f"sudo {scalog_exec} data --rid {str(idx)} --sid {str(shard)} --config"
                    print(path_data_exec)
                    process, log_filename = execute_remote_command(
                        active_data[idx], path_data_exec, scalog_yaml_filename, ssh_key, ssh_user, exp_index, json_output_name
                    )
                    if not process:
                        raise Exception(f"Failed to start data node on {ip}")
                    component_log_files[f'data_{active_data[idx]}'] = (active_data[idx], log_filename)
            print(f"\nWaiting {SERVER_START_DELAY}s for cluster to stabilize...")
            time.sleep(SERVER_START_DELAY)

            # --- 5. Run client benchmark ---
            # TODO: Verify the client binary accepts these flags; update if it uses a config-only
            # approach (in which case add duration/message_size/threads to scalog_yaml instead).
            print(f"\n--- Starting Scalog client benchmark on {client_ip} ---")
            total_bench = warm_up + duration + cool_down
            # execute_remote_command runs: sudo <path_client> ~/<config>
            # We append extra flags by embedding them in the path string.
            # TODO: adjust if the client takes flags differently.
            client_cmd = f'sudo {scalog_exec} perf -t {num_client_threads} -s {message_size} -d {duration}s --config {config_path} > ~/{client_log_filename} 2>&1'
            client_ssh_cmd = [
                'ssh', '-i', ssh_key,
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                f'{ssh_user}@{client_ip}',
                f'/bin/bash -c "{client_cmd} &"',
            ]
            print(client_ssh_cmd)
            client_process = subprocess.Popen(
                client_ssh_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

            total_wait = total_bench + 15  # 15s buffer for startup/teardown
            print(f"Waiting {total_wait}s for client benchmark to complete...")
            time.sleep(total_wait)

            # --- 6. Retrieve results ---
            print("\n--- Retrieving Scalog results ---")
            client_log_local = os.path.join(local_results_dir, client_log_filename)
            copy_log_file_back(client_ip, ssh_user, ssh_key, client_log_filename, local_results_dir)
            copy_log_file_back(client_ip, ssh_user, ssh_key, client_log_filename, local_results_dir)
            copy_results_back(
                client_ip, 
                ssh_user, 
                ssh_key, 
                json_output_name, 
                local_results_dir
            )

        except Exception as e:
            print(f"\nFATAL ERROR during Scalog experiment cycle {exp_index + 1}: {e}")

        finally:
            # --- 7. Stop all cluster components and retrieve logs ---
            print("\n--- Stopping Scalog cluster ---")
            for component, (ip, log_filename) in component_log_files.items():
                copy_log_file_back(ip, ssh_user, ssh_key, log_filename, local_results_dir)
            
            for idx in range(0, len(active_data)):
                kill_remote_process(active_data[idx], "scalog", ssh_key, ssh_user)
            for idx in range(0, len(active_order)):
                kill_remote_process(active_order[idx], "scalog", ssh_key, ssh_user)
            kill_remote_process(discovery_ip, "scalog", ssh_key, ssh_user)
            kill_remote_process(client_ip, "scalog", ssh_key, ssh_user)
            process_and_aggregate_results(local_results_dir, json_output_name, system_name, base_config, config)

def run_experiment_cycle_speclog(base_config, exp_index, local_results_dir, system_name):
    """Runs a single experiment cycle for Speclog. Not yet implemented."""
    raise NotImplementedError("run_experiment_cycle_speclog is not yet implemented.")


EXPERIMENT_CYCLE_FNS = {
    'pringles': run_experiment_cycle_pringles,
    'pringles_hardware': run_experiment_cycle_hardware,
    'scalog':   run_experiment_cycle_scalog,
    'speclog':  run_experiment_cycle_speclog,
    'kafka':    run_experiment_cycle_kafka,
}


def main(config_file="general.toml"):
    """Main function to parse config, generate YAMLs, and execute experiments in a loop."""

    # --- 1. Parse Generic TOML Configuration ---
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
    run_setup = base_config['setup'].get('run_setup_script', 'False').lower() == 'true'
    if run_setup:
        print("\n--- Running Setup Script on All Machines ---")
        for ip in all_ips:
            if not run_setup_script(ip, SETUP_SCRIPT_PATH, ssh_key, ssh_user):
                print("FATAL: Setup script failed on at least one machine. Aborting experiment.")
                return
        print("--- Setup Complete ---")
    else:
        print("\n--- Setup Script Execution Skipped ---")

        # --- Start Experiment Loop ---
    print(f"\n--- Starting {len(experiments_to_run)} Experiment Runs ---")
    for exp_index, exp_params in enumerate(experiments_to_run):
        # 1. Create a deep copy of the base config for this specific run
        current_config = deepcopy(base_config)

        # 2. Merge experiment-specific parameters (overrides)
        for key, value in exp_params.items():
            print(key)
            print(value)
            if key in current_config.get('experiment_parameters', {}):
                current_config['experiment_parameters'][key] = value
        print(current_config['experiment_parameters'])
        system_name = current_config['experiment_parameters']['protocol_name']

        protocol_config = ""
        if system_name == "pringles":
            protocol_config = "pringles_software.toml"
        elif system_name == "pringles_hardware" or system_name == "pktgen":
            protocol_config = "pringles_hardware.toml"
        elif system_name == "scalog":
            protocol_config = "scalog.toml"
        elif system_name == "speclog":
            protocol_config = "speclog.toml"
        elif system_name == "kakfa":
            protocol_config = "kafka.toml"

        cycle_fn = EXPERIMENT_CYCLE_FNS.get(system_name)
        if cycle_fn is None:
            print(f"FATAL: Unknown system '{system_name}'. Valid options: {list(EXPERIMENT_CYCLE_FNS.keys())}")
            return
        print(f"\n--- System: {system_name} ---")
        cycle_fn(current_config, protocol_config, exp_index, local_results_dir, system_name)
        time.sleep(EXPERIMENT_DELAY)
        #client_ips = base_config['network_setup']['cli_ips']
        #stor_ips = base_config['network_setup']['stor_ips']
        #ssh_key = os.path.expanduser(base_config['network_setup']['ssh_key'])
        #ssh_user = base_config['network_setup']['ssh_user']
        #with_tunnel = base_config['experiment_parameters']['with_tunnel']
        #switch_ip = base_config['network_setup']['switch_ip']
        #all_ips = client_ips + stor_ips + [switch_ip]

        # --- 5. SETUP COMPARISON SYSTEM BINARIES (conditional) --- TODO
        #run_comparison_setup = base_config['program_paths'].get('run_comparison_setup', 'False').lower() == 'true'
        #if run_comparison_setup:
        #    setup_fns = {'kafka': setup_kafka_nodes, 'scalog': setup_scalog_nodes}
        #    setup_fn = setup_fns.get(system_name)
        #    if setup_fn:
        #        print(f"\n--- Setting up {system_name} binaries on remote nodes ---")
        #        if not setup_fn(base_config, ssh_key, ssh_user):
        #            print(f"FATAL: {system_name} setup failed. Aborting experiment.")
        #            return
        #    else:
        #        print(f"\n--- No comparison setup defined for system '{system_name}', skipping ---")
        #else:
        #    print(f"\n--- Comparison system setup skipped (run_comparison_setup = False) ---")

        # 3. Run the full experiment cycle with the merged configuration
        # --- Final Step A: Aggregate ALL results from the shared directory ---\
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

    import argparse
    parser = argparse.ArgumentParser(description="Run Pringles or comparison system experiments.")
    parser.add_argument("config", nargs="?", default="general.toml",
                        help="Path to the TOML config file (default: general.toml)")
    args = parser.parse_args()
    main(args.config)
