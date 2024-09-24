# Helper library with common functions

def get_storage_server_ids(log_idx, num_racks, num_storage_servers, num_storage_failures):
    # Get storage server ids corresponding to log index
    primary_storage_id = (log_idx % num_storage_servers)
    #primary_rack_id = math.ceil((log_idx % num_storage_servers) / num_storage_servers_per_rack)
    storage_servers = [] # [storage_id]
    for i in range(0, num_storage_failures + 1):
        #rack_id = (primary_rack_id + i) % num_racks
        storage_id = (primary_storage_id + i * num_storage_servers) % (num_storage_servers)
        storage_servers.append(storage_id)
    return storage_servers


