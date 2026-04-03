# TODO: LOTS of repeated code, when porting to Go reduce code size
# TODO: Need to use a thread pool, need to use class object
import math
import time

# Constants
NUM_RACKS = 4
NUM_STORAGE_SERVERS_PER_RACK = 1
NUM_STORAGE_FAILURES = 3
NUM_SWITCHES = 4
ACK_TIMEOUT = 30
SEQNO_TIMEOUT = 30
TAIL_TIMEOUT = 30
READ_TIMEOUT = 30
SLEEP_PERIOD = 5
storage_server_ips = ['10.0.2.2', '10.0.4.4', '10.0.6.6', '10.0.8.8']
starting_client_id = -1 # TODO: THIS IS NOT THREAD SAFE!
local_log_entries={}
acks={}

def update_storage_server_mapping():
    # TODO: This changes the storage server mapping

# Run as subroutine in thread
def expand(x):
    yield x
    while x.payload:
        x = x.payload
        yield x

def get_acks(pkt, idx):
    if AckEntry in pkt:
        for l in expand(pkt):
            if l.name=='AckEntry' and l.log_idx == idx:
                acks[idx] += 1

def get_acks_thread(idx):
    iface = 'eth0'
    print("Append: sniffing on {}".format(iface))
    # Part 1: Get sequence number
    sniff(iface = iface, prn = lambda x: get_acks(x, idx))

async def store_entry(idx, log_entry):
    ss_ids_arr = get_storage_server_ids(idx, NUM_RACKS, NUM_RACKS*NUM_STORAGE_SERVERS_PER_RACK, NUM_STORAGE_FAILURES)
    for ss_id in ss_ids_arr:
        # Get IP address
        ip = storage_server_ips[ss_id]
        addr = socket.gethostbyname(ip)
        print("Address: ", addr)
        iface = get_if()
        try:
            # Create and send packet
            pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
            pkt = pkt / StoreEntry(log_idx=idx) / IP(dst=addr) / log_entry
            #pkt.show2()
            sendp(pkt, iface='eth0')
        except KeyboardInterrupt:
            print("EXCEPTION DETECTED while sending storage packet to IP ", ip)
            sys.exit()
    # Initialize receiving thread
    acks[idx] = 0
    ack_thread = Thread(target = get_acks_thread, args = (idx))
    ack_thread.start()
    # Wait for all the acks
    end_timeout = time.time() + ACK_TIMEOUT
    while time.time() < end_timeout:
        if (acks[idx] >= NUM_STORAGE_FAILURES + 1):
            ack_thread.join()
            return True
        time.sleep(SLEEP_PERIOD)
    ack_thread.join()
    return False

# Receives local sequence number and global sequence offset
def get_sequence_number(pkt, local_seq_no, global_offset):
    if pkt:
        for l in expand(pkt):
            if l.name=='SeqNoReq':
                print("RECEIVED Local seqno: {}".format(l.local_sequence_no))
                local_seq_no = l.local_sequence_no
            if l.name=='Cntrl':
                print("RECEIVED Control pkt global offset: {}".format(l.last_global_offset))
                global_offset = l.last_global_offset

def sequence_number_thread(local_seq_no, global_offset):
    iface = 'eth0'
    print("Append: sniffing on {}".format(iface))
    # Part 1: Get sequence number
    sniff(iface = iface, prn = lambda x: get_sequence_number(x, local_seq_no, global_offset))

def get_global_seq_no():
    idx = 0
    try:
        local_seq_no = 0
        global_offset = 0
        receive_thread = Thread(target = sequence_number_thread, args = (local_seq_no, global_offset))
        receive_thread.start() 
        # Create and send packet
        pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
        pkt = pkt / SeqNoReq(log_idx=0) / IP(dst=addr) / log_entry
        #pkt.show2()
        sendp(pkt, iface='eth0')
        end_timeout = time.time() + SEQNO_TIMEOUT
        while time.time() < end_timeout:
            if (local_seq_no > 0):
                if (global_offset > 0):     
                     idx = local_seq_no + global_offset
            time.sleep(SLEEP_PERIOD)
        receive_thread.join()    
    except KeyboardInterrupt:
        print("EXCEPTION DETECTED while sending storage packet to IP ", ip)
        sys.exit()
    return idx

def update_tail(idx):
    try:
        receive_thread = Thread(target = update_tail_thread, args = ())
        receive_thread.start() 
        # Create and send packet
        pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
        pkt = pkt / UpdateTail(log_idx=0) / IP(dst=addr) / log_entry
        #pkt.show2()
        sendp(pkt, iface='eth0')
        end_timeout = time.time() + SEQNO_TIMEOUT
        while time.time() < end_timeout:
            if (local_seq_no > 0):
                if (global_offset > 0):     
                     idx = local_seq_no + global_offset
            time.sleep(SLEEP_PERIOD)
        receive_thread.join()
        return True    
    except KeyboardInterrupt:
        print("EXCEPTION DETECTED while sending storage packet to IP ", ip)
        sys.exit()
        return False


def append(log_entry):
    # Here we append to the end of the log
    # Part 1: Get sequence number
    idx = get_global_seq_no()
    # Part 2: Store entry durably in storage servers
    success = store_entry(idx, log_entry)
    sucess = update_tail(idx)
    # TODO: Wait and communicate tail up to the switch

# Filling holes
def fill_hole():
    # Fills holes either with known contents or with junk

# Get entry at a particular index 
def get_idx(pkt, idx_entry):
    if pkt:
        for l in expand(pkt):
            if l.name=='SendEntry':
                print("RECEIVED Local seqno: {}".format(l.payload))
                idx_entry.append(l.payload) # TODO: Is this the correct way to get payloads? 

def get_idx_thread(idx_entry):
    iface = 'eth0'
    print("Append: sniffing on {}".format(iface))
    # Part 1: Get sequence number
    sniff(iface = iface, prn = lambda x: get_idx(x, idx_entry))

# Read at an index
def readAt(idx):
        # Step 0: Check whether local log has it
        if idx in local_log_entries:
            return local_log_entries[idx] 
        # Step 1: Find where index is located
	ss_ids_arr = get_storage_server_ids(idx, NUM_RACKS, NUM_RACKS*NUM_STORAGE_SERVERS_PER_RACK, NUM_STORAGE_FAILURES)
        # Step 2: Send broadcast to those servers where it is located
        for ss_id in ss_ids_arr:
            # Get IP address
            ip = storage_server_ips[ss_id]
            addr = socket.gethostbyname(ip)
            print("Address: ", addr)
            iface = get_if()
            try:
                # Create and send packet
                pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
                pkt = pkt / GetEntry(log_idx=idx) / IP(dst=addr) / log_entry
                sendp(pkt, iface='eth0')
            except KeyboardInterrupt:
                print("EXCEPTION DETECTED while sending storage packet to IP ", ip)
                sys.exit()
	# Step 3: Wait for reply to those servers
        idx_entry = []
        read_thread = Thread(target=get_idx_thread, args = (idx_entry))
        read_thread.start() 
        # Create and send packet
        pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
        pkt = pkt / GetEntry(log_idx=idx) / IP(dst=addr)
        #pkt.show2()
        sendp(pkt, iface='eth0')
        end_timeout = time.time() + READ_TIMEOUT
        while time.time() < end_timeout:
            if (len(idx_entry) >= NUM_STORAGE_FAILURES + 1):
                 break 
            time.sleep(SLEEP_PERIOD)
        # TODO: Process either not having all entries, having all entries but different values, or
        # having all entires with same values
        # TODO Add entry to local dictionary
        read_thread.join() 
        # TODO: Send tail information to switch?        

# Get entry at a particular index 
def tail_subroutine(pkt, tail):
    if pkt:
        for l in expand(pkt):
            if l.name=='TailReq':
                print("RECEIVED tail: {}".format(l.tail))
                tail = l.tail 

def tail_thread(tail):
    iface = 'eth0'
    print("Append: sniffing on {}".format(iface))
    # Part 1: Get sequence number
    sniff(iface = iface, prn = lambda x: tail_subroutine(x, tail))

def getTail():
    # TODO Get the tail of the log
    # Here we append to the end of the log
    # Part 1: Request tail
    tail = 0
    try:
        receive_thread = Thread(target = tail_thread, args = (tail))
        receive_thread.start() 
        # Create and send packet
        ip = ni.ifaddresses('eth0')[ni.AF_INET][0]['addr']
        addr = socket.gethostbyname(ip)        
        pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
        pkt = pkt / TailReq(tail=0) / IP(dst=addr)
        #pkt.show2()
        sendp(pkt, iface='eth0')
        end_timeout = time.time() + TAIL_TIMEOUT
        while time.time() < end_timeout:
            if (tail > 0):
                break
            time.sleep(SLEEP_PERIOD)
        receive_thread.join()    
    except KeyboardInterrupt:
        print("EXCEPTION DETECTED while sending storage packet to IP ", ip)
        sys.exit()
    return tail

def subscribe_thread(start_idx):
    start = start_idx
    while True:
        tail = getTail()
        for i in range(start, tail+1):
            readAt(i) # TODO: Check that these were successful
        start = tail+1
        time.sleep(SLEEP_PERIOD)

# Function which adds updates to the log to the server's local log
# Spawns a separate thread at client startup to periodically check
# the log tail and see what new updates it needs to read
def subscribe(start_idx):
    # Subscribe at an index
    thread = Thread(target=subscribe_thread, args = (start_idx))
    thread.start() 
   
def create_client():
    starting_client_id += 1
    view_number = 0
    return (starting_client_id, view_number)
