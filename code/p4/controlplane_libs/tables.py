import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
from bfruntime_client_base_tests import BfRuntimeTest
from multicast_helpers import *
from headers import *

default_p4_name = "pktgen"
# List of Types:
# 0 = Dummy 0x0
# 1 = Append 0x0860 
# 2 = Append_Wait 0x0862 
# 3 = Append_Resp 0x0861 
# 4 = Read 0x0870 
# 5 = Read_Resp 0x0871
# 6 = Tail 0x0840
# 7 = Sub 0x0850
# 8 = Sub_Resp 0x0851
# 9 = View_Change 0x0900
# 10 = View_Ack 0x0910



class P4Tables:
    def __init__(self, bfrt_info, global_group_id, list_of_types, logger):
        self.bfrt_info = bfrt_info
        self.global_group_id = global_group_id
        self.logger = logger
        self.mgid_table = self.bfrt_info.table_get("$pre.mgid")
        self.node_table = self.bfrt_info.table_get("$pre.node")
        self.ecmp_table = self.bfrt_info.table_get("$pre.ecmp")
        self.lag_table = self.bfrt_info.table_get("$pre.lag")
        self.prune_table = self.bfrt_info.table_get("$pre.prune")
        self.mirror_cfg_table = self.bfrt_info.table_get("$mirror.cfg")
        self.list_of_types = list_of_types
        self.general_target = gc.Target(device_id=0, pipe_id=0xffff)
        self.pipe_target0 = gc.Target(device_id=0, pipe_id=0x00)
        self.pipe_target1 = gc.Target(device_id=0, pipe_id=0x01)
        self.pipe_targets = [self.pipe_target0, self.pipe_target1]
    
    ############# CONSTANT TABLES ################
    def setup_recirc_port_table(self, tot_num_recirc_ports, pipe_idx):
        table_recirc = self.bfrt_info.table_get("MyIngress{}.num_recirc_port_table".format(pipe_idx))
        table_recirc.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_recirc.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_recirc.make_data(action_name="MyIngress{}.get_num_recirc".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="num_recirc_ports", val=tot_num_recirc_ports)]))
            
    def setup_wait_port_table(self, tot_num_wait_ports, pipe_idx):
        table_wait = self.bfrt_info.table_get("MyIngress{}.num_wait_port_table".format(pipe_idx))
        table_wait.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_wait.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_wait.make_data(action_name="MyIngress{}.get_num_wait".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="num_wait_ports", val=tot_num_wait_ports)]))

    def setup_ack_const(self, ack_threshold, pipe_idx):
        table_ack = self.bfrt_info.table_get("MyIngress{}.get_ack_threshold".format(pipe_idx))
        table_ack.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_ack.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_ack.make_data(action_name="MyIngress{}.ack_threshold".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="threshold", val=ack_threshold)]))

    def setup_num_shard_const(self, num_shards, pipe_idx):
        table_shard = self.bfrt_info.table_get("MyIngress{}.get_num_shards".format(pipe_idx))
        table_shard.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_shard.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_shard.make_data(action_name="MyIngress{}.num_shards".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="shards", val=num_shards)]))

    def setup_shard_size_const(self, shard_size, pipe_idx):
        table_shard = self.bfrt_info.table_get("MyIngress{}.get_shard_size".format(pipe_idx))
        table_shard.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_shard.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_shard.make_data(action_name="MyIngress{}.shard_size".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="size", val=shard_size)]))

    def setup_switch_id_const(self, switch_id, pipe_idx):
        table_switch_id = self.bfrt_info.table_get("MyIngress{}.get_switch_id".format(pipe_idx))
        table_switch_id.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_switch_id.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_switch_id.make_data(action_name="MyIngress{}.switch_id".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="id", val=switch_id)]))

    def setup_switch_check(self, list_of_types, curr_switch_id, ring_members, pipe_idx):
        # Check switch routing
        table_process = self.bfrt_info.table_get("MyIngress{}.check_switch_routing".format(pipe_idx))
        table_process.info.key_field_annotation_add("hdr.ring_type.switch_to_process", "bit<32>")
        table_process.info.key_field_annotation_add("hdr.ring_type.type", "bit<16>")
        table_process.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        idx = 1
        print(list_of_types)
        print(curr_switch_id)
        for type_in_list in list_of_types:
            print("List type: {}".format(type_in_list))
            table_process.entry_add(
                    self.pipe_targets[pipe_idx], 
                    [table_process.make_key([gc.KeyTuple('hdr.ring_type.switch_to_process', curr_switch_id), 
                                             gc.KeyTuple('hdr.ring_type.type', int(type_in_list))]
                                            )],
                    [table_process.make_data(action_name="MyIngress{}.set_ring_process".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="process_type", val=idx)])])
            idx += 1

        idx = 0
        for member in ring_members:
            if member == 0:
                idx += 1
                continue
            print(idx)
            for type_in_list in list_of_types:
                table_process.entry_add(
                        self.pipe_targets[pipe_idx], 
                        [table_process.make_key([gc.KeyTuple('hdr.ring_type.switch_to_process', idx), gc.KeyTuple('hdr.ring_type.type', type_in_list)])],
                        [table_process.make_data(action_name="MyIngress{}.route_next_switch".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=member)])])
            idx += 1


    ################## TABLE #######################
    def setup_local_seq(self, pipe_idx):
        table_local_seq = self.bfrt_info.table_get("MyIngress{}.cntrl_local_seq".format(pipe_idx))
        table_local_seq.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_local_seq.info.key_field_annotation_add("meta.cntrl_to_process", "bit<1>")
        table_local_seq.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Read current local sequence number
        table_local_seq.entry_add(
                self.pipe_targets[pipe_idx],
                [table_local_seq.make_key([gc.KeyTuple('meta.ring_process', 0), gc.KeyTuple('meta.cntrl_to_process', 1)])],
                [table_local_seq.make_data([], 'MyIngress{}.read_local'.format(pipe_idx))])

    def setup_pkt_it(self, pipe_idx):
        table_pkt_it = self.bfrt_info.table_get("MyIngress{}.fill_cntrl_pkt_it".format(pipe_idx))
        table_pkt_it.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_pkt_it.info.key_field_annotation_add("meta.cntrl_to_process", "bit<1>")
        table_pkt_it.info.key_field_annotation_add("meta.update_iteration", "bit<8>")
        table_pkt_it.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Get control packet iteration, do NOT update
        table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', 0), 
                                        gc.KeyTuple('meta.cntrl_to_process', 1), 
                                        gc.KeyTuple('meta.update_iteration', 0)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.get_it'.format(pipe_idx))])

        # Control packet: Gets and updates control packet iteration
        table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', 0), 
                                        gc.KeyTuple('meta.cntrl_to_process', 1), 
                                        gc.KeyTuple('meta.update_iteration', 1)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.update_it'.format(pipe_idx))])

    def setup_cntrl_next_hop(self, pipe_idx):
        table_hop_routing = self.bfrt_info.table_get("MyIngress{}.next_hop_routing".format(pipe_idx))
        table_hop_routing.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_hop_routing.info.key_field_annotation_add("meta.cntrl_to_process", "bit<1>")
        table_hop_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Get the next hop switch ID
        table_hop_routing.entry_add(
                self.pipe_targets[pipe_idx],
                [table_hop_routing.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.cntrl_to_process', 1)])],
                [table_hop_routing.make_data([], 'MyIngress{}.get_next_hop'.format(pipe_idx))])

        # Tail packet: Get the next hop switch ID
        #table_hop_routing.entry_add(
        #        self.pipe_targets[pipe_idx],
        #        [table_hop_routing.make_key([gc.KeyTuple('meta.ring_process', self.list_of_types[6]),
        #                                     gc.KeyTuple('meta.cntrl_to_process', 0)])],
        #        [table_hop_routing.make_data([], 'MyIngress{}.update_next_hop'.format(pipe_idx))])

    def setup_edit_batch_size(self, pipe_idx):
        table_edit_batch = self.bfrt_info.table_get("MyIngress{}.edit_batch_size".format(pipe_idx))
        table_edit_batch.info.key_field_annotation_add("meta.update_iteration", "bit<16>")
        table_edit_batch.info.key_field_annotation_add("meta.cntrl_to_process", "bit<1>")
        table_edit_batch.info.key_field_annotation_add("meta.ring_process", "bit<16>")

        table_edit_batch.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Set the batch size for the control packet iteration
        table_edit_batch.entry_add(
                self.pipe_targets[pipe_idx],
                [table_edit_batch.make_key([gc.KeyTuple('meta.update_iteration', 1),
                                            gc.KeyTuple('meta.cntrl_to_process', 1),
                                            gc.KeyTuple('meta.ring_process', 0)])],
                [table_edit_batch.make_data([], 'MyIngress{}.set_batch_size'.format(pipe_idx))])

    def setup_update_batches(self, pipe_idx):
        table_update_batches = self.bfrt_info.table_get("MyIngress{}.update_batches".format(pipe_idx))
        table_update_batches.info.key_field_annotation_add("meta.update_iteration", "bit<16>")
        table_update_batches.info.key_field_annotation_add("meta.cntrl_to_process", "bit<1>")
        table_update_batches.info.key_field_annotation_add("meta.ring_process", "bit<16>")

        table_update_batches.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Update control packet global seq number + add batch 
        table_update_batches.entry_add(
                self.pipe_targets[pipe_idx],
                [table_update_batches.make_key([gc.KeyTuple('meta.update_iteration', 1),
                                                gc.KeyTuple('meta.cntrl_to_process', 1),
                                                gc.KeyTuple('meta.ring_process', 0)])],
                [table_update_batches.make_data([], 'MyIngress{}.batch_add'.format(pipe_idx))])

    def setup_process_gsn(self, pipe_idx):
        table_process_gsn = self.bfrt_info.table_get("MyIngress{}.process_gsn".format(pipe_idx))
        table_process_gsn.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_process_gsn.info.key_field_annotation_add("meta.append_g_zero", "bit<8>")
        table_process_gsn.info.key_field_annotation_add("meta.update_iteration", "bit<8>")
        table_process_gsn.info.key_field_annotation_add("meta.cntrl_to_process", "bit<1>")

        table_process_gsn.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Update the highest seen global sequence number
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple('meta.update_iteration', 1),
                                             gc.KeyTuple('meta.cntrl_to_process', 1)])],
                [table_process_gsn.make_data([], 'MyIngress{}.update_gsn'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple('meta.update_iteration', 0),
                                             gc.KeyTuple('meta.cntrl_to_process', 1)])],
                [table_process_gsn.make_data([], 'MyIngress{}.update_gsn'.format(pipe_idx))])


    ################## ROUTING TABLE #######################
    # next_hop_port: Don't hardcode TODO
    def setup_routing(self, recirc_ports, wait_ports, ring_members, client_machines, pipe_idx): # TODO routing?
        table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
        table_routing.info.key_field_annotation_add("meta.route_to_client", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
        table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.subscribe", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.hops", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>") # TODO
        table_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Recirculating appends for sequencing
        #for i in range(0, len(recirc_ports)):
        #    print("Recirc port idx: {} and port {}".format(i, recirc_ports[i]))
        #    table_routing.entry_add(
        #            self.pipe_targets[pipe_idx],
        #            [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
        #                                     gc.KeyTuple('meta.route_to_client', 0),
        #                                     gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
        #                                     gc.KeyTuple('meta.recirc_port_type', 1), # Recirculation for sequencing IS type 1
        #                                     gc.KeyTuple('meta.port_idx', i),
        #                                     gc.KeyTuple('meta.subscribe', 0),
        #                                     gc.KeyTuple('meta.route_to_shard', 0),
        #                                     gc.KeyTuple('meta.hops', 0)])],
        #            [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=recirc_ports[i])])])

        ## Recirculating appends for storage emulation
        #for i in range(0, len(wait_ports)):
        #    print("Wait port idx: {} and port {}".format(i, wait_ports[i]))
        #    table_routing.entry_add(
        #            self.pipe_targets[pipe_idx],
        #            [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
        #                                     gc.KeyTuple('meta.route_to_client', 0),
        #                                     gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
        #                                     gc.KeyTuple('meta.recirc_port_type', 2), # Recirculation for sequencing IS type 1
        #                                     gc.KeyTuple('meta.port_idx', i),
        #                                     gc.KeyTuple('meta.subscribe', 0),
        #                                     gc.KeyTuple('meta.route_to_shard', 0),
        #                                     gc.KeyTuple('meta.hops', 0)])],
        #            [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=wait_ports[i])])])

        # Recirculation while waiting for acknowledgements??? TODO

        # Sending appends to shards TODO

        # Sending reads to the shards TODO

        # Sending appends to all the subscribers TODO

        # Send control packet to next hop in ring
        for i in range(0, len(ring_members)):
            print("Ring member ID: {} to port: {}".format(i, ring_members[i]))
            table_routing.entry_add(
                    self.pipe_targets[pipe_idx],
                    [table_routing.make_key([gc.KeyTuple('meta.next_hop', i),
                                             gc.KeyTuple('meta.route_to_client', 0),
                                             gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
                                             gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
                                             gc.KeyTuple('meta.port_idx', 0),
                                             gc.KeyTuple('meta.subscribe', 0),
                                             gc.KeyTuple('meta.route_to_shard', 0),
                                             gc.KeyTuple('meta.hops', 0)])],
                    [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=ring_members[i])])])

        # Send tail to next hop in the ring
        #for j in range(0, len(ring_members)):
        #    for i in range(0, len(ring_members)):
        #        print("Ring member IDs: {}".format(i, ring_members[i]))
        #        table_routing.entry_add(
        #                self.pipe_targets[pipe_idx],
        #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', j), # TODO what is this for?
        #                                         gc.KeyTuple('meta.route_to_client', 0),
        #                                         gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
        #                                         gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
        #                                         gc.KeyTuple('meta.port_idx', 0),
        #                                         gc.KeyTuple('meta.subscribe', 0),
        #                                         gc.KeyTuple('meta.route_to_shard', 0),
        #                                         gc.KeyTuple('meta.hops', i+1)])],
        #                [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=ring_members[i])])])

        ## Route to the client based on IP (NOT tail related, just a normal packet)
        #for client_ip_and_port in client_machines:
        #    table_routing.entry_add(
        #            self.pipe_targets[pipe_idx],
        #            [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
        #                                     gc.KeyTuple('meta.route_to_client', 1),
        #                                     gc.KeyTuple('meta.dst_addr', client_ip_and_port[0], prefix_len=32),
        #                                     gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
        #                                     gc.KeyTuple('meta.port_idx', 0),
        #                                     gc.KeyTuple('meta.subscribe', 0),
        #                                     gc.KeyTuple('meta.route_to_shard', 0),
        #                                     gc.KeyTuple('meta.hops', 0)])],
        #            [table_routing.make_data(action_name="MyIngress{}.eth_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])
        #
        ## For Tail packets ONLY: Route to the client based on IP if it has visited every ring member
        #for client_ip_and_port in client_machines:
        #    table_routing.entry_add(
        #            self.pipe_targets[pipe_idx],
        #            [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
        #                                     gc.KeyTuple('meta.route_to_client', 0),
        #                                     gc.KeyTuple('meta.dst_addr', client_ip_and_port[0], prefix_len=32),
        #                                     gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
        #                                     gc.KeyTuple('meta.port_idx', 0),
        #                                     gc.KeyTuple('meta.subscribe', 0),
        #                                     gc.KeyTuple('meta.route_to_shard', 0),
        #                                     gc.KeyTuple('meta.hops', len(ring_members))])],
        #            [table_routing.make_data(action_name="MyIngress{}.eth_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])

    ################ EGRESS ##################
    def setup_check_dur_table(self, lower_bound, upper_bound, pipe_idx):
        # Cntrl ID -> Send Port
        table_check_dur = self.bfrt_info.table_get("MyEgress{}.check_duration".format(pipe_idx))
        table_check_dur.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_check_dur.info.key_field_annotation_add("hdr.ring_type.raw_elapsed_time", "bit<16>")
        table_check_dur.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_check_dur.make_key([gc.KeyTuple('hdr.ring_type.raw_elapsed_time', low=lower_bound, high=upper_bound)])],
            [table_check_dur.make_data([], "MyEgress{}.update_type".format(pipe_idx))])

    def setup_active_view_table(self, pipe_idx):
        # Cntrl ID -> Send Port
        table_active_view = self.bfrt_info.table_get("MyEgress{}.update_active_view".format(pipe_idx))
        table_active_view.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_active_view.info.key_field_annotation_add("hdr.ethernet.etherType", "bit<16>")
        table_active_view.info.key_field_annotation_add("meta.ring_process", "bit<16>")

        # Control packet: If control packet is received, then view change marker gets unset...TODO need checking?
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.etherType', TYPE_CONTROL),
                                         gc.KeyTuple('meta.ring_process', 0)])],
            [table_active_view.make_data([], "MyEgress{}.unset_active_view".format(pipe_idx))])


    ############# MULTICAST TABLES ################
    def setup_shard_multicast_groups(self, shard_to_port_gp, pipe_idx):
        # Map shard IDs to their respective multicast group (each group has a collection of ports to storage server)
        table_shard = self.bfrt_info.table_get("MyIngress.get_shard_port")
        table_shard.info.key_field_annotation_add("hdr.ring_type.shard_id", "bit<32>")
        self.logger.info(shard_to_port_gp)
        for key, dev_ports in shard_to_port_gp.items():
            rid = 0x321 + self.global_group_id
            xid = 0x432 + self.global_group_id
            mbr_lags = []
            self.logger.info("RID: ", rid, " XID: ", xid, " GPID: ", self.global_group_id, " DEV: ", dev_ports)
            mc = MCTree(self, self.pipe_targets[pipe_idx], self.global_group_id)
            mc.add_node(rid, xid, dev_ports, mbr_lags)
            self.mgid_table.entry_add(self.pipe_targets[pipe_idx], [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
            table_shard.entry_add(
                self.pipe_targets[pipe_idx], 
                [table_shard.make_key([gc.KeyTuple('hdr.ring_type.shard_id', key)])],
                [table_shard.make_data(action_name="MyIngress.shard_port", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
            self.global_group_id += 1
    
    def setup_subscribe_routing_table(self, ports_to_ring_members, cpu_port, pipe_idx):
        table_sub = self.bfrt_info.table_get("MyIngress.submit_subscription")
        table_sub.info.key_field_annotation_add("hdr.sub.subscribe", "bit<32>")

        # Broadcast subscription packet to all switches
        for i in range(0, len(ports_to_ring_members)):
            if ports_to_ring_members[i] == 0:
                ports_to_ring_members[i] = cpu_port
        self.mgid_table = self.bfrt_info.table_get("$pre.mgid")
        rid = 0x321 + self.global_group_id
        xid = 0x432 + self.global_group_id
        mbr_lags = []
        self.logger.info("RID: ", rid, " XID: ", xid, " GPID: ", self.global_group_id, " DEV: ", ports_to_ring_members)
        self.general_sub_mc = MCTree(self, self.pipe_targets[pipe_idx], self.global_group_id)
        self.general_sub_group = self.global_group_id
        self.general_sub_mc.add_node(rid, xid, ports_to_ring_members, mbr_lags)
        self.mgid_table.entry_add(self.pipe_targets[pipe_idx], [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
        table_sub.entry_add(
                self.pipe_targets[pipe_idx], 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 1)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_all", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
        self.global_group_id += 1

        # Only send to local control plane
        table_sub.entry_add(
                self.pipe_targets[pipe_idx], 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 0)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_self", data_field_list_in=[gc.DataTuple(name="port", val=cpu_port)])])
    
    def update_stream_subscriber_table(self, stream_id, subscriber_port, pipe_idx):
        self.logger.info(stream_id)
        self.logger.info(subscriber_port)
        self.logger.info(self.stream_sub_dict)
        if stream_id not in self.stream_sub_dict.keys():
            table_acks = self.bfrt_info.table_get("MyIngress.route_subscriber_acks")
            table_acks.info.key_field_annotation_add("hdr.append.stream_id", "bit<32>")

            # Subscription group (non-stream & stream)
            stream_sub_mc = MCTree(self, self.pipe_targets[pipe_idx], self.global_group_id)
            stream_group_id = self.global_group_id
            if stream_id == 0:
                self.general_sub_group = stream_group_id
            rid = 0x321 + stream_group_id
            xid = 0x432 + stream_group_id
            stream_sub_mc.add_node(rid, xid, [subscriber_port], [])
            self.stream_sub_dict[stream_id] = stream_sub_mc
            self.mgid_table.entry_add(self.pipe_targets[pipe_idx], [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])

            # Update mirror table with the multicast group information4
            mirror_session_bfrt_key = self.mirror_cfg_table.make_key([gc.KeyTuple('$sid', self.sid)])
            if stream_id == 0:
                mirror_session_bfrt_data = self.mirror_cfg_table.make_data([
                    gc.DataTuple('$direction', str_val="INGRESS"),
                    gc.DataTuple('$session_enable', bool_val=True),
                    gc.DataTuple('$mcast_grp_a', self.general_sub_group),
                    gc.DataTuple('$mcast_grp_a_valid', bool_val=True),
                    gc.DataTuple('$ucast_egress_port_valid', bool_val=False),
                ], "$normal")
                self.mirror_cfg_table.entry_add(self.pipe_targets[pipe_idx], [ mirror_session_bfrt_key ], [ mirror_session_bfrt_data ])
            else:
                mirror_session_bfrt_data = self.mirror_cfg_table.make_data([
                    gc.DataTuple('$direction', str_val="INGRESS"),
                    gc.DataTuple('$session_enable', bool_val=True),
                    gc.DataTuple('$mcast_grp_a', self.general_sub_group),
                    gc.DataTuple('$mcast_grp_a_valid', bool_val=True),
                    gc.DataTuple('$mcast_grp_b', stream_group_id),
                    gc.DataTuple('$mcast_grp_b_valid', bool_val=True),
                    gc.DataTuple('$ucast_egress_port_valid', bool_val=False),
                ], "$normal")
                self.mirror_cfg_table.entry_add(self.pipe_targets[pipe_idx], [ mirror_session_bfrt_key ], [ mirror_session_bfrt_data ])

            # Table acks 
            table_acks.entry_add(
                    self.pipe_targets[pipe_idx], 
                    [table_acks.make_key([gc.KeyTuple('hdr.append.stream_id', stream_id)])],
                    [table_acks.make_data(action_name="MyIngress.forward_to_subscribers", data_field_list_in=[gc.DataTuple(name="mirror_session_id", val=self.sid)])])
            self.global_group_id += 1
            self.sid += 1
        else:
            self.stream_sub_dict[stream_id].get_first_node().addMbrs([subscriber_port], [])


