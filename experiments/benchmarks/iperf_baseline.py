import threading
import time
import re
import paramiko
import time

# --- CONFIGURATION ---
SERVER_IP = "10.10.1.3"
SERVER_USER = "murray22"
CLIENT_USER = "murray22"
NUM_SERVER_PORTS = 10  
BASE_PORT = 5001

# List of your 10 client IP addresses
CLIENT_IPS = [
    "10.10.1.2", "10.10.1.4", "10.10.1.5", "10.10.1.6",
    "10.10.1.7", "10.10.1.8", "10.10.1.9", "10.10.1.10", "10.10.1.11", "10.10.1.12"
]

# The thread steps you want to test per machine
THREAD_STEPS = [1, 5, 10, 15, 20, 30]
TEST_DURATION = 60  # Updated to 40 seconds per run
PAYLOAD_SIZE = 100

# Dictionary to hold raw string outputs from clients
raw_results = {step: [] for step in THREAD_STEPS}

NUM_SERVER_PORTS = 10  # Spin up 10 independent server engines
BASE_PORT = 5001

def start_multithreaded_servers():
    print(f"[*] Launching {NUM_SERVER_PORTS} independent iperf2 server instances...")
    for i in range(NUM_SERVER_PORTS):
        port = BASE_PORT + i
        # Launch each server process independently in the background
        SSH_execute(SERVER_IP, SERVER_USER, f"iperf -s -p {str(port)} -e > /dev/null 2>&1 &", read_output=False)
    time.sleep(2)  # Give the operating system sockets a moment to bind

# --- HELPER FUNCTIONS ---
def SSH_execute(ip, user, command, read_output=True):
    """Connects via SSH, runs a command, and optionally returns output."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(ip, username=user)
        stdin, stdout, stderr = ssh.exec_command(command)
        if read_output:
            return stdout.read().decode('utf-8')
    except Exception as e:
        print(f"SSH Error on {ip}: {e}")
    finally:
        ssh.close()
    return ""

def parse_iperf2_native_bounceback(output_text, client_ip, step):
    """
    Parses native iperf2 bounceback lines by aggregating completed transaction 
    counts (cnt) and calculating a weighted average for latency across all threads.
    """
    pps = 0.0
    latency_ms = 0.0
    
    # Track metrics per unique thread ID to handle multi-thread outputs perfectly
    # Structure: { thread_id: (cnt, mean_latency) }
    thread_metrics = {}
    
    # Pattern to match thread lines, e.g., "[ 15] 0.00-45.00 sec ... 200840=0.223/..."
    # Group 1: Thread ID
    # Group 2: Transaction Count (cnt)
    # Group 3: Average (mean) Latency
    thread_pattern = re.compile(r'\[\s*(\d+)\]\s+\d+\.\d+-\d+\.\d+\s+sec.*?\s+(\d+)=\s*([\d\.]+)/')
    
    lines = output_text.strip().split('\n')
    
    for line in lines:
        match = thread_pattern.search(line)
        if match:
            thread_id = match.group(1)
            cnt = int(match.group(2))
            mean_lat = float(match.group(3))
            
            # Store/overwrite with the final cumulative interval line for this thread
            thread_metrics[thread_id] = (cnt, mean_lat)

    if thread_metrics:
        total_cnt = 0
        total_weighted_latency = 0.0
        
        # Calculate true aggregated metrics
        for thread_id, (cnt, mean_lat) in thread_metrics.items():
            total_cnt += cnt
            total_weighted_latency += (cnt * mean_lat)
            
        # Hardcoded to your test run duration of 45.0 seconds
        test_duration = 45.0 
        
        # 1 Request/Reply transaction = 2 wire packets (Tx + Rx)
        total_rps = total_cnt / test_duration
        pps = total_rps * 2
        
        # Calculate true overall mean latency across all threads
        if total_cnt > 0:
            latency_ms = total_weighted_latency / total_cnt
    else:
        print(f"[ERROR] Could not parse summary metrics for {client_ip} at scale {step}")

    return pps, latency_ms

def teardown_servers():
    print("[*] Tearing down server cluster...")
    SSH_execute(SERVER_IP, SERVER_USER, "killall iperf", read_output=False)

# --- MAIN EXPERIMENT LOOP ---
def main():
    # 1. Start the iperf2 Server
        # 2. Loop through different thread counts
    for threads_per_machine in THREAD_STEPS:
        print(f"\n--- Running Experiment: {threads_per_machine} threads/machine ({threads_per_machine * len(CLIENT_IPS)} total) ---")
        
        client_threads = []
        
        # -u: UDP mode
        # -l 72: 72 bytes payload + 28 bytes IP/UDP headers = 100 byte packets
        # -b 100m: Uncapped target bandwidth per thread to push max packets


        def worker(client_ip, step, cmd):
            out = SSH_execute(client_ip, CLIENT_USER, cmd)
            raw_results[step].append(out)

        # Launch all clients simultaneously
        thread_index = 0
        for ip in CLIENT_IPS:
            #out = SSH_execute(ip, CLIENT_USER, "iperf -v")
            #print(out)
            # Assign each parallel worker thread to a different server port sequentially
            assigned_port = BASE_PORT + (thread_index % NUM_SERVER_PORTS)
            cmd = f"iperf -c {SERVER_IP} -p {assigned_port} --bounceback -l 100 --bounceback-period=0 --bounceback-reply 100 -t {TEST_DURATION} -N -e -P {threads_per_machine}"
            t = threading.Thread(target=worker, args=(ip, threads_per_machine, cmd))
            client_threads.append(t)
            t.start()
            thread_index = thread_index + 1

        # Wait for all clients to complete the 45-second run
        for t in client_threads:
            t.join()
            
        print(f"[+] Finished test round for {threads_per_machine} threads. Waiting for cool-down...")
        time.sleep(5) 

    # Clean up server
    print("\n[*] Tearing down iperf2 server...")
    SSH_execute(SERVER_IP, SERVER_USER, "killall iperf", read_output=False)

    # 3. Aggregation and Data Calculation
    print("\n================ EXPERIMENT RESULTS ================")
    print(f"{'Threads/Machine':<18}{'Total Streams':<15}{'Total PPS (packets/sec)':<25}{'Avg Latency (ms)':<15}")
    print("-" * 75)

    for step in THREAD_STEPS:
        if raw_results[step]:
       		#print(f"--- RAW OUTPUT FOR STEP {step} (First Client) ---")
       		#print(raw_results[step][0]) # Prints the raw string from the first client
       		print("-" * 40)
        total_pps = 0.0
        all_round_latencies = []
        
        for client_ip, client_output in zip(CLIENT_IPS, raw_results[step]):
            pps, lat = parse_iperf2_native_bounceback(client_output, client_ip, step)
            total_pps += pps
            if lat > 0:
                all_round_latencies.append(lat)
        
        avg_round_latency = sum(all_round_latencies) / len(all_round_latencies) if all_round_latencies else 0.0
        total_streams = step * len(CLIENT_IPS)
        
        print(f"{step:<18}{total_streams:<15}{total_pps:<25,.2f}{avg_round_latency:<15.2f}")

if __name__ == "__main__":
    print(f"[*] Starting multiple iperf2 servers on {SERVER_IP}...")
    # Server needs -u for UDP mode
    #out = SSH_execute(SERVER_IP, SERVER_USER, "iperf -v")
    #print(out)
    #SSH_execute(SERVER_IP, SERVER_USER, "iperf -s -e > /dev/null 2>&1 &", read_output=False)
    start_multithreaded_servers()
    try:
        main()
    finally:
        teardown_servers()
        print("\n================ BENCHMARK COMPLETE ================")
