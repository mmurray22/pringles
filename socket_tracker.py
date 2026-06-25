import subprocess
import re
import csv
import time
import sys

# Configuration
OUTPUT_FILE = "socket_metrics.csv"
INTERVAL = 1.0  # seconds

# Regex to match the specific process entry
# This looks for users:(("switch",pid=...,fd=...))
PROCESS_REGEX = re.compile(r'\"switch\"')#r'users:\(\(\"switch\",pid=\d+,fd=\d+\)\)')

def get_socket_data():
    """Runs 'ss -unlp' and extracts Recv-Q and Send-Q for the target sockets."""
    try:
        # Run the ss command
        result = subprocess.run(['ss', '-unlp'], capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running 'ss': {e}", file=sys.stderr)
        return []
    except FileNotFoundError:
        print("Error: 'ss' command not found. Are you on Linux?", file=sys.stderr)
        sys.exit(1)
    lines = result.stdout.strip().split('\n')
    matched_sockets = []

    # Skip the header line
    for line in lines[1:]:
        # Check if the line contains our target process
        if PROCESS_REGEX.search(line):
            parts = line.split()
            # ss output format usually: Netid State Recv-Q Send-Q Local:Address... Process
            # Recv-Q is typically index 1, Send-Q is index 2
            if len(parts) >= 4:
                try:
                    recv_q = int(parts[1])
                    send_q = int(parts[2])
                    matched_sockets.append((recv_q, send_q))
                except ValueError:
                    # In case the parsing alignment shifts unexpectedly
                    continue
    print(len(matched_sockets))
    return matched_sockets

def main():
    print(f"Tracking sockets every {INTERVAL}s... Press Ctrl+C to stop.")
    
    # Open CSV file and write headers
    with open(OUTPUT_FILE, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        # Creating columns for both Socket 1 and Socket 2
        writer.writerow([
            'Sock1_Recv_Q', 'Sock1_Send_Q', 
            'Sock2_Recv_Q', 'Sock2_Send_Q', 
            'Timestamp'
        ])

        try:
            while True:
                start_time = time.time()
                sockets = get_socket_data()

                # Default values if one or both sockets aren't found in this slice
                s1_recv, s1_send = 0, 0
                s2_recv, s2_send = 0, 0

                if len(sockets) >= 1:
                    s1_recv, s1_send = sockets[0]
                if len(sockets) >= 2:
                    s2_recv, s2_send = sockets[1]
                
                current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([s1_recv, s1_send, s2_recv, s2_send, current_time])
                csv_file.flush() # Force write to disk so data isn't lost if killed

                # Adjust sleep to account for execution time (keeps interval precise)
                elapsed = time.time() - start_time
                sleep_time = max(0.0, INTERVAL - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print(f"\nTracking stopped. Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    # Note: 'ss -unlp' usually requires root privileges to see process names
    main()
