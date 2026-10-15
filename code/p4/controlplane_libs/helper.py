########## PACKET GENERATION HELPER FUNCTIONS ###################

def pgen_timer_hdr_to_dmac(self, pipe_id, app_id, batch_id, packet_id):
    """
    Given the fields of a 6-byte packet-gen header return an Ethernet MAC address
    which encodes the same values.
    """
    pipe_shift = 3
    return '%02x:00:%02x:%02x:%02x:%02x' % ((pipe_id << pipe_shift) | app_id,
                                            batch_id >> 8,
                                            batch_id & 0xFF,
                                            packet_id >> 8,
                                            packet_id & 0xFF)
def pgen_port(self, pipe_id, pipe_local_port_idx):
    """
    Given a pipe return a port in that pipe which is usable for packet
    generation.  Note that Tofino allows ports 68-71 in each pipe to be used for
    packet generation while Tofino2 allows ports 0-7.  This example will use
    either port 68 or port 6 in a pipe depending on chip type.
    """
    list_pgen_ports = [68, 69, 70, 71]
    pipe_local_port = list_pgen_ports[pipe_local_port_idx]
    return self.make_port(pipe_id, pipe_local_port)

def make_port(self, pipe, local_port):
    return (pipe << 7) | local_port


######### READ INFO FROM THE DATAPLANE #################
def get_counter_number(bfrt_info, target, counter_name):
    counter = bfrt_info.table_get(counter_name)
    # 3. Request the entry at index 0
    # 'from_hw=True' ensures you get the latest count from the ASIC registers, 
    # not a cached software value.
    resp = counter.entry_get(
        target,
        [counter.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
        {"from_hw": True}
    )
    
    # 4. Parse the response
    try:
        data, _ = next(resp)
        data_dict = data.to_dict()
        
        # Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
        return data_dict['$COUNTER_SPEC_PKTS']
    except gcBfruntimeRpcException as e:
        logger.info(e)
        return 0

def get_register_number(self, bfrt_info, target, register_name):
    reg_table = bfrt_info.table_get(register_name)
    table_resp = reg_table.entry_get(
        target,
        [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
        {"from_hw": True}
    )
    reg_data, _ = next(table_resp)
    reg_data_dict = reg_data.to_dict()
    register_entry_name = register_name + ".f1"
    return reg_data_dict[register_entry_name][0]

def get_full_register(self, bfrt_info, target, register_name):
    reg_table = bfrt_info.table_get(register_name)
    table_resp = reg_table.entry_get(
        target,
        [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
        {"from_hw": True}
    )
    reg_data, _ = next(table_resp)
    reg_data_dict = reg_data.to_dict()
    register_entry_name = register_name + ".f1"
    return reg_data_dict[register_entry_name]
     
