# Controller library

# State for controller 
# TODO: Not thread safe
view_number = 0
cntrl_id = 0
storage_server_ips = []

def init_controller(list_of_ss_ips):
    # Initializes the controller
    storage_server_ips = list_of_ss_ips
