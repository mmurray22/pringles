# Storage Server API
import headers

# State for storage server
# TODO: Not thread safe!
starting_ss_id = -1
store = {}
view_number = 0
num_racks = 0
num_ss = 0
max_ss_failures = 0

def get_store_entry(idx):
    # 1. TODO Check to make sure idx is stored on server
    # 2. Get entry if stored at server
    entry = None
    if idx in store:
        entry = store[idx]
    return entry 

def send_reply(idx, entry, request_pkt, addr):
    # Get IP address
    if addr == None:
        print("ERROR: No return address found!")
        return
    # Create and send packet
    pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
    pkt = pkt / SendEntry(log_idx=idx) / IP(dst=addr) / entry
    #pkt.show2()
    sendp(pkt, iface='eth0')

def send_ack(idx, pkt, addr):
    if addr == None:
        print("ERROR: No return address found!")
        return
    # Create and send packet
    pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
    pkt = pkt / AckEntry(log_idx=idx) / IP(dst=addr)
    #pkt.show2()
    sendp(pkt, iface='eth0')

# Input: scapy packet object
def expand(x):
    yield x
    while x.payload:
        x = x.payload
        yield x

def process_requests(pkt):
    if StoreEntry in pkt:
        addr = None
        for l in expand(pkt):
            if l.name == 'StoreEntry':
                store[l.idx] = l.payload # TODO: Is this how you access payload? 
            elif l.name == 'IP':
                addr = l.src
        send_ack(l.idx, pkt, addr)
    elif GetEntry in pkt:
        addr = None
        for l in expand(pkt):
            if l.name == 'GetEntry':
                entry = get_store_entry(l.idx) 
            elif l.name == 'IP':
                addr = l.src
        send_reply(l.idx, entry, pkt, addr)
    elif UpdateStorageMapping in pkt:
        for l in expand(pkt):
            if l.name=='UpdateStorageMapping':
                if l.new_view_number > view_number:
                    print("RECEIVED New View No: {}".format(l.new_view_number))
                    view_number = l.new_view_number
                elif l.num_racks != num_racks:
                    num_racks = l.num_racks
                elif l.num_ss != num_ss:
                    num_ss = l.num_ss
                elif l.max_failures != max_failures:
                    max_ss_failures = l.max_failures
        # TODO: Do we need to return anything to the controller?

def storage_thread():
    iface = 'eth0'
    print("Append: sniffing on {}".format(iface))
    sniff(iface = iface, prn = lambda x: process_requests(x))

def get_view_number():
    return view_number

# Note: Only returns correct storage server ID after calling
# init_storage_server
def get_ss_id():
    return starting_ss_id

# Initialize storage server ID and view number
def init_storage_server(racks, total_ss, max_failures):
    view_number = 0
    staring_ss_id += 1
    store = {}
    num_racks = racks
    num_ss = total_ss
    max_ss_failures = max_failures 
    store_thread = Thread(target = storage_thread, args = ())
    store_thread.start()
    return starting_ss_id
