import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2

class TestTables:
    """
    Function name: setup_link_hop_table
    Arguments: 
        - target: pipeline
        - bfrt_info: device info object
        - src_switch_id: This is the ID of the switch which will originate the link test
        - self_switch_id: This is the ID of this switch which will be used to determine what next switch port to send to based on hop number
        - max_hops: max number of hops the link test will traverse (e.g. for ring of 2 nodes, max_hops = 2)
        - ports_to_link_members: mapping (for THIS switch) from switch ID to port to send to next switch
        - control_port: Port to send packet from data plane back to control plane for processing

    Purpose: This function initializes the link hop table in the P4 with actual routing information. The purpose
    of said table is to direction a link_test packet through as many specified hops of a ring, with the end goal
    of measuring how long it takes to go around the ring

    Assumption: We send to the next hop based on the inherent switch_id ordering of switches. Ordering of hops derived from switch_ids
    """
    @staticmethod
    def setup_link_hop_table(target, bfrt_info, src_switch_id, self_switch_id, max_hops, ports_to_link_members, control_port):
        # First check to make sure we have enough ports to hop through
        if max_hops > len(ports_to_link_members):
            print("!!!!!!!!!!!!!!!! ILLEGAL OPERATION: Max hops cannot be greater than the number of ports available to hop through. No link_hop table initialized")
            return

        # Basic table initialization
        table_hops = bfrt_info.table_get("MyIngress.link_hop")
        table_hops.info.key_field_annotation_add("hdr.link_test.hop", "bit<8>")

        # First hop: When link_test packet is first injected into the source switch data plane 
        first_hop = src_switch_id + 1
        table_hops.entry_add(
                target, 
                [table_hops.make_key([gc.KeyTuple('hdr.link_test.hop', 0)])], # Semantically, first "hop" is the switch the link test starts on
                [table_hops.make_data(action_name="MyIngress.first_hop", data_field_list_in=[gc.DataTuple(name="port", val=ports_to_link_members[first_hop])])])

        # Last hop: When link_test packet has reached the final hop we want to test 
        table_hops.entry_add(
                target, 
                [table_hops.make_key([gc.KeyTuple('hdr.link_test.hop', max_hops-1)])], # Semantically, last "hop" in the switch the link test before returning to the beginning
                [table_hops.make_data(action_name="MyIngress.final_hop", data_field_list_in=[gc.DataTuple(name="port", val=ports_to_link_members[src_switch_id])])])

        # Last step: Send link_test packet to the control plane
        table_hops.entry_add(
                target, 
                [table_hops.make_key([gc.KeyTuple('hdr.link_test.hop', max_hops)])], # Semantically, last "hop" in the switch the link test before returning to the beginning
                [table_hops.make_data(action_name="MyIngress.send_to_control", data_field_list_in=[gc.DataTuple(name="port", val=control_port)])])

        # Intermediate hops: Forward packet along to next link 
        for i in range(1, max_hops-1):
            idx = (self_switch_id + i) % len(ports_to_link_members)
            table_hops.entry_add(
                    target, 
                    [table_hops.make_key([gc.KeyTuple('hdr.link_test.hop', i)])],
                    [table_hops.make_data(action_name="MyIngress.forward_pkt", data_field_list_in=[gc.DataTuple(name="port", val=ports_to_link_members[idx])])])

