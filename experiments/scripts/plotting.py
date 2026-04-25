# Plotting script

if base_config['testing']['only_plot_gen']:
    ring_sizes = [] 
    for exp_index, exp_params in enumerate(experiments_to_run):
        for key, value in exp_params.items():
            if key == 'switches_in_ring':
                ring_sizes.append(len(value))
    print("ONLY TESTING THE GRAPH GENERATION")
    process_and_aggregate_results(base_config['testing']['local_results_dir'], ring_sizes)
    plot_results(base_config['testing']['local_results_dir'], base_config['experiment_parameters']['plot_param'])
    return
    

# REVISED FUNCTION: Creates three separate PNG files
def plot_results(local_target_dir, plot_param):
    """
    Reads the aggregated JSON files and plots the three required graphs as separate PNGs.
    """
    print("\n--- Generating Summary Plots ---")
    
    all_files = os.listdir(local_target_dir)
    # Filter for aggregated summary files (those without underscores in the name, e.g., 'two_clients.json')
    summary_files = [f for f in all_files if f.endswith('.json') and len(f.split('_')) == 1]
    print(summary_files)
    
    if not summary_files:
        print(f"Warning: No aggregated summary JSON files found in {local_target_dir}. Cannot plot results.")
        return
        
    # Lists to store the data points
    num_clients_list = []
    num_switches_in_ring = []
    tput_list = []
    latency_list = []
    batch_list = []
    
    # 1. Gather Data
    for filename in summary_files:
        filepath = os.path.join(local_target_dir, filename)
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
                
            # Ensure all required fields exist and are numeric
            if plot_param == 'num_clients':
                num_clients = data.get('num_clients')
                agg_tput = data.get('agg_tput')
                total_avg_latency = data.get('total_avg_latency')
                batch_size = data.get('batch_size')
                if all(isinstance(v, (int, float)) for v in [num_clients, agg_tput, total_avg_latency]):
                    num_clients_list.append(num_clients)
                    tput_list.append(agg_tput)
                    latency_list.append(total_avg_latency)
                    batch_list.append(batch_size)
                else:
                    print(f"Warning: Skipping file {filename} due to missing or invalid data fields.")
            elif plot_param == 'switches_in_ring':
                num_switches = data.get('num_switches_in_ring')
                agg_tput = data.get('agg_tput')
                total_avg_latency = data.get('total_avg_latency')
                batch_size = data.get('batch_size')
                if all(isinstance(v, (int, float)) for v in [num_switches, agg_tput, total_avg_latency]):
                    num_switches_in_ring.append(num_switches)
                    tput_list.append(agg_tput)
                    latency_list.append(total_avg_latency)
                    batch_list.append(batch_size)
                else:
                    print(f"Warning: Skipping file {filename} due to missing or invalid data fields.")

        except Exception as e:
            print(f"Error reading or processing summary file {filename}: {e}")

    if not num_clients_list and not num_switches_in_ring:
        print(num_clients_list)
        print(num_switches_in_ring)
        print("No valid data points collected for plotting.")
        return

    # 2. Sort the lists by the number of clients (for cleaner X-axes)
    # Combine into tuples, sort, and unpack
    if plot_param == 'num_clients':
        combined = sorted(zip(num_clients_list, tput_list, latency_list))
        num_clients_list, tput_list, latency_list = zip(*combined)
    elif plot_param == 'switches_in_ring':
        combined = sorted(zip(num_switches_in_ring, tput_list, latency_list))
        num_switches_in_ring, tput_list, latency_list = zip(*combined)

    # 3. Create and save plots individually
    
    # --- Plot 1: Clients vs. Aggregate Throughput (throughut_vs_clients.png) ---
    plt.figure(figsize=(8, 6))
    if plot_param == 'num_clients':
        plt.plot(num_clients_list, tput_list, marker='o', linestyle='-', color='blue')
    elif plot_param == 'switches_in_ring':
        plt.plot(num_switches_in_ring, tput_list, marker='o', linestyle='-', color='blue')
    plt.xlabel('Number of Clients')
    plt.ylabel('Aggregate Throughput')
    plt.title(f'Aggregate Throughput vs. Client Count\nExperiment: {os.path.basename(local_target_dir)}')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xlim(xmin=0) # NEW
    plt.ylim(ymin=0) # NEW
    if plot_param == 'num_clients':
        plt.xticks(num_clients_list) # Force X-ticks to match data points
    elif plot_param == 'switches_in_ring':
        plt.xticks(num_switches_in_ring) # Force X-ticks to match data points
    plot_filepath_1 = os.path.join(local_target_dir, "throughput_vs_clients.png")
    plt.savefig(plot_filepath_1)
    plt.close()
    print(f"Plot 1 saved to: {plot_filepath_1}")


    # --- Plot 2: Clients vs. Total Average Latency (latency_vs_clients.png) ---
    plt.figure(figsize=(8, 6))
    if plot_param == 'num_clients':
        plt.plot(num_clients_list, latency_list, marker='o', linestyle='-', color='red')
    elif plot_param == 'switches_in_ring':
        plt.plot(num_switches_in_ring, latency_list, marker='o', linestyle='-', color='red')
    plt.xlabel('Number of Clients')
    plt.ylabel('Total Average Latency (ms)')
    plt.title(f'Total Average Latency vs. Client Count\nExperiment: {os.path.basename(local_target_dir)}')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xlim(xmin=0) # NEW
    plt.ylim(ymin=0) # NEW
    if plot_param == 'num_clients':
        plt.xticks(num_clients_list) # Force X-ticks to match data points
    elif plot_param == 'switches_in_ring':
        plt.xticks(num_switches_in_ring) # Force X-ticks to match data points
    plot_filepath_2 = os.path.join(local_target_dir, "latency_vs_clients.png")
    plt.savefig(plot_filepath_2)
    plt.close()
    print(f"Plot 2 saved to: {plot_filepath_2}")

    
    # --- Plot 3: Aggregate Throughput vs. Total Average Latency (throughput_vs_latency.png) ---
    plt.figure(figsize=(8, 6))
    plt.plot(tput_list, latency_list, marker='o', linestyle='-', color='green')
    #plt.scatter(tput_list, latency_list, marker='o', color='green')
    # Annotate each point with the number of clients
    if plot_param == 'num_clients':
        for i, clients in enumerate(num_clients_list):
            plt.annotate(f'{clients} Cli', (tput_list[i], latency_list[i]), 
                         textcoords="offset points", xytext=(5,-5), ha='left')
    elif plot_param == 'switches_in_ring':
        for i, switches in enumerate(num_switches_in_ring):
            plt.annotate(f'{switches} Switch', (tput_list[i], latency_list[i]), 
                         textcoords="offset points", xytext=(5,-5), ha='left')

    #for i, batch_sz in enumerate(batch_list):
    #    plt.annotate(f'{batch_sz} Batch', (tput_list[i], latency_list[i]), 
    #                 textcoords="offset points", xytext=(5,-5), ha='left')
                         
    plt.xlabel('Aggregate Throughput')
    plt.ylabel('Total Average Latency (ms)')
    plt.title(f'Throughput-Latency Tradeoff\nExperiment: {os.path.basename(local_target_dir)}')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xlim(xmin=0) # NEW
    plt.ylim(ymin=0) # NEW
    plot_filepath_3 = os.path.join(local_target_dir, "throughput_vs_latency.png")
    plt.savefig(plot_filepath_3)
    plt.close()
    print(f"Plot 3 saved to: {plot_filepath_3}")


