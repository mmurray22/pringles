import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
from bfruntime_client_base_tests import BfRuntimeTest
from multicast_helpers import *
from headers import *

default_p4_name = "pktgen"
CNTRL_VALID = 'hdr.cntrl.$valid'
TRIGGER_VIEW_VALID = 'hdr.trigger_view.$valid'
TAIL_VALID = 'hdr.tail.$valid'
FINALIZE_VIEW_VALID = 'hdr.finalize_view.$valid'
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
    def __init__(self, self_switch_id, physical_switch_id, bfrt_info, global_group_id, list_of_types, logger):
        self.bfrt_info = bfrt_info
        self.self_switch_id = self_switch_id
        self.physical_switch_id = physical_switch_id
        self.global_group_id = global_group_id
        self.logger = logger
        self.mgid_table = self.bfrt_info.table_get("$pre.mgid")
        self.node_table = self.bfrt_info.table_get("$pre.node")
        self.ecmp_table = self.bfrt_info.table_get("$pre.ecmp")
        self.lag_table = self.bfrt_info.table_get("$pre.lag")
        self.prune_table = self.bfrt_info.table_get("$pre.prune")
        self.mirror_cfg_table = self.bfrt_info.table_get("$mirror.cfg")
	self.side = 1
        self.list_of_types = list_of_types
        self.general_target = gc.Target(device_id=0, pipe_id=0xffff)
        self.pipe_target0 = gc.Target(device_id=0, pipe_id=0x00)
        self.pipe_target1 = gc.Target(device_id=0, pipe_id=0x01)
        self.pipe_targets = [self.pipe_target0, self.pipe_target1]
        self.multicast_groups = {}

    # Multicast group
    def add_mcast_group(self, list_of_ports, key, pipe_idx):
        mc = MCTree(self, self.pipe_targets[pipe_idx], self.global_group_id)
        rid = 0x321 + self.global_group_id
        xid = 0x432 + self.global_group_id
        mbr_lags = []

        mc.add_node(rid, xid, list_of_ports, mbr_lags)
        self.multicast_groups[key] = (self.global_group_id, mc)
        self.mgid_table.entry_add(self.pipe_targets[pipe_idx], [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
	grp_id = self.global_group_id
	self.global_group_id += 1
	print("ADDED Multicast entry: Group ID - {}, Number of ports - {}, First port - {}".format(grp_id, len(list_of_ports), list_of_ports[0]))
	return grp_id
    
    ############# CONSTANT TABLES ################
    def setup_constants_table(self, 
                              num_recirc_ports, 
                              num_wait_ports, 
                              ack_threshold,
                              number_of_shards,
                              shard_size,
                              switch_id,
                              pipe_idx):
        table_constants = self.bfrt_info.table_get("MyIngress{}.get_constants".format(pipe_idx))
        table_constants.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        
        table_constants.default_entry_set(
                self.pipe_targets[pipe_idx], 
                table_constants.make_data(action_name="MyIngress{}.constants".format(pipe_idx), 
                                       data_field_list_in=[gc.DataTuple(name="switch_id", val=switch_id),
                                                           gc.DataTuple(name="shard_size", val=shard_size),
                                                           gc.DataTuple(name="number_of_shards", val=number_of_shards),
                                                           gc.DataTuple(name="ack_threshold", val=ack_threshold),
                                                           gc.DataTuple(name="num_recirc_ports", val=num_recirc_ports),
                                                           gc.DataTuple(name="num_wait_ports", val=num_wait_ports)]))
         

    ################## TABLE #######################
    def setup_local_seq(self, pipe_idx):
        table_local_seq = self.bfrt_info.table_get("MyIngress{}.cntrl_local_seq".format(pipe_idx))
        table_local_seq.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_local_seq.info.key_field_annotation_add("meta.append_g_zero", "bit<8>")
        table_local_seq.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Read current local sequence number
        table_local_seq.entry_add(
                self.pipe_targets[pipe_idx],
                [table_local_seq.make_key([gc.KeyTuple('meta.ring_process', 0), 
					   gc.KeyTuple('meta.append_g_zero', 0),
					   gc.KeyTuple(CNTRL_VALID, 1)])],
                [table_local_seq.make_data([], 'MyIngress{}.read_local'.format(pipe_idx))])
        # Append packet: Read current local sequence number
        table_local_seq.entry_add(
                self.pipe_targets[pipe_idx],
                [table_local_seq.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND), 
					   gc.KeyTuple('meta.append_g_zero', 1),
					   gc.KeyTuple(CNTRL_VALID, 0)])],
                [table_local_seq.make_data([], 'MyIngress{}.write_local'.format(pipe_idx))])



    def setup_pkt_it(self, pipe_idx):
        table_pkt_it = self.bfrt_info.table_get("MyIngress{}.fill_cntrl_pkt_it".format(pipe_idx))
        names = sorted(table_pkt_it.info.key_dict_allname.keys())
        print("keys of %s: %s" % (table_pkt_it.info.name_get(), names))
        if CNTRL_VALID not in names:
            raise KeyError("CNTRL_VALID=%r is not a key of this table; use one of the names printed above" % CNTRL_VALID)


        table_pkt_it.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_pkt_it.info.key_field_annotation_add("meta.update_iteration", "bit<8>")
        table_pkt_it.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Get control packet iteration, do NOT update
        table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', 0), 
                                        gc.KeyTuple(CNTRL_VALID, 1), 
                                        gc.KeyTuple(TRIGGER_VIEW_VALID, 0), 
                                        gc.KeyTuple('meta.update_iteration', 0)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.get_it'.format(pipe_idx))])

        table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', 0), 
                                        gc.KeyTuple(CNTRL_VALID, 0), 
                                        gc.KeyTuple(TRIGGER_VIEW_VALID, 1), 
                                        gc.KeyTuple('meta.update_iteration', 0)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.get_complete_it'.format(pipe_idx))])


        # Control packet: Gets and updates control packet iteration
        table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', 0), 
                                        gc.KeyTuple(CNTRL_VALID, 1), 
                                        gc.KeyTuple(TRIGGER_VIEW_VALID, 0), 
                                        gc.KeyTuple('meta.update_iteration', 1)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.update_it'.format(pipe_idx))])

        # Append packet: Gets control packet iteration
        table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND), 
                                        gc.KeyTuple(CNTRL_VALID, 0), 
                                        gc.KeyTuple(TRIGGER_VIEW_VALID, 0), 
                                        gc.KeyTuple('meta.update_iteration', 0)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.get_it'.format(pipe_idx))])
	table_pkt_it.entry_add(
                self.pipe_targets[pipe_idx],
                [table_pkt_it.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND_RESP), 
                                        gc.KeyTuple(CNTRL_VALID, 0), 
                                        gc.KeyTuple(TRIGGER_VIEW_VALID, 0), 
                                        gc.KeyTuple('meta.update_iteration', 0)]
                                       )],
                [table_pkt_it.make_data([], 'MyIngress{}.get_complete_it'.format(pipe_idx))])



    def setup_cntrl_next_hop(self, pipe_idx):
        table_hop_routing = self.bfrt_info.table_get("MyIngress{}.next_hop_routing".format(pipe_idx))
        table_hop_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Get the next hop switch ID
        table_hop_routing.entry_add(
                self.pipe_targets[pipe_idx],
                [table_hop_routing.make_key([gc.KeyTuple(TAIL_VALID, 0),
                                             gc.KeyTuple(FINALIZE_VIEW_VALID, 0),
                                             gc.KeyTuple(CNTRL_VALID, 1)])],
                [table_hop_routing.make_data([], 'MyIngress{}.get_next_hop'.format(pipe_idx))])
        table_hop_routing.entry_add(
                self.pipe_targets[pipe_idx],
                [table_hop_routing.make_key([gc.KeyTuple(TAIL_VALID, 1),
                                             gc.KeyTuple(FINALIZE_VIEW_VALID, 0),
                                             gc.KeyTuple(CNTRL_VALID, 0)])],
                [table_hop_routing.make_data([], 'MyIngress{}.get_next_hop'.format(pipe_idx))])
        table_hop_routing.entry_add(
                self.pipe_targets[pipe_idx],
                [table_hop_routing.make_key([gc.KeyTuple(TAIL_VALID, 0),
                                             gc.KeyTuple(FINALIZE_VIEW_VALID, 1),
                                             gc.KeyTuple(CNTRL_VALID, 0)])],
                [table_hop_routing.make_data([], 'MyIngress{}.update_next_hop'.format(pipe_idx))])

    def setup_edit_batch_size(self, pipe_idx): # TODO DEPRECATED
        table_edit_batch = self.bfrt_info.table_get("MyIngress{}.edit_batch_size".format(pipe_idx))
        table_edit_batch.info.key_field_annotation_add("meta.update_iteration", "bit<16>")
        table_edit_batch.info.key_field_annotation_add("meta.ring_process", "bit<16>")

        table_edit_batch.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Set the batch size for the control packet iteration
        table_edit_batch.entry_add(
                self.pipe_targets[pipe_idx],
                [table_edit_batch.make_key([gc.KeyTuple('meta.update_iteration', 1),
                                            gc.KeyTuple(CNTRL_VALID, 1),
                                            gc.KeyTuple('meta.ring_process', 0)])],
                [table_edit_batch.make_data([], 'MyIngress{}.set_batch_size'.format(pipe_idx))])
        # Append packet: Set the batch size for the control packet iteration
        table_edit_batch.entry_add(
                self.pipe_targets[pipe_idx],
                [table_edit_batch.make_key([gc.KeyTuple('meta.update_iteration', 1),
                                            gc.KeyTuple(CNTRL_VALID, 0),
                                            gc.KeyTuple('meta.ring_process', TYPE_APPEND)])],
                [table_edit_batch.make_data([], 'MyIngress{}.update_batch_size'.format(pipe_idx))])



    def setup_update_batches(self, pipe_idx): # TODO: Deprecated
        table_update_batches = self.bfrt_info.table_get("MyIngress{}.update_batches".format(pipe_idx))
        table_update_batches.info.key_field_annotation_add("meta.update_iteration", "bit<16>")
        table_update_batches.info.key_field_annotation_add("meta.ring_process", "bit<16>")

        table_update_batches.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Update control packet global seq number + add batch 
        table_update_batches.entry_add(
                self.pipe_targets[pipe_idx],
                [table_update_batches.make_key([gc.KeyTuple('meta.update_iteration', 1),
                                                gc.KeyTuple(CNTRL_VALID, 1),
                                                gc.KeyTuple('meta.ring_process', 0)])],
                [table_update_batches.make_data([], 'MyIngress{}.batch_add'.format(pipe_idx))])
    
    def setup_upper_process_gsn(self, pipe_idx):
        table_process_gsn = self.bfrt_info.table_get("MyIngress{}.upper_process_gsn".format(pipe_idx))
        table_process_gsn.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_process_gsn.info.key_field_annotation_add("meta.append_g_zero", "bit<8>")

        table_process_gsn.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Update the highest seen global sequence number
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 1),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.upper_update_gsn'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.upper_assign_gsn'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND),
                                             gc.KeyTuple('meta.append_g_zero', 1),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.upper_make_gsn_zero'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND_RESP),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.upper_get_ack_readiness'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 1)])],
                [table_process_gsn.make_data([], 'MyIngress{}.upper_assign_gsn'.format(pipe_idx))])
    

    def setup_lower_process_gsn(self, pipe_idx):
        table_process_gsn = self.bfrt_info.table_get("MyIngress{}.lower_process_gsn".format(pipe_idx))
        table_process_gsn.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_process_gsn.info.key_field_annotation_add("meta.append_g_zero", "bit<8>")

        table_process_gsn.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Control packet: Update the highest seen global sequence number
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 1),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.lower_update_gsn'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.lower_assign_gsn'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND),
                                             gc.KeyTuple('meta.append_g_zero', 1),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.lower_make_gsn_zero'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND_RESP),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 0)])],
                [table_process_gsn.make_data([], 'MyIngress{}.lower_get_ack_readiness'.format(pipe_idx))])
        table_process_gsn.entry_add(
                self.pipe_targets[pipe_idx],
                [table_process_gsn.make_key([gc.KeyTuple('meta.ring_process', 0),
                                             gc.KeyTuple('meta.append_g_zero', 0),
                                             gc.KeyTuple(CNTRL_VALID, 0),
                                             gc.KeyTuple(TRIGGER_VIEW_VALID, 1)])],
                [table_process_gsn.make_data([], 'MyIngress{}.lower_assign_gsn'.format(pipe_idx))])
    
    def setup_append_first_tbl(self, pipe_idx):
        table_append_first = self.bfrt_info.table_get("MyIngress{}.append_first_tbl".format(pipe_idx))
        table_append_first.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_append_first.info.key_field_annotation_add("meta.append_g_zero", "bit<8>")

        table_append_first.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
       
        # TYPE_APPEND
        table_append_first.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_first.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND),
                                  gc.KeyTuple('meta.append_g_zero', 1)])],
                [table_append_first.make_data([], 'MyIngress{}.get_local_idx'.format(pipe_idx))])
	# TYPE_APPEND, but already sequenced
        table_append_first.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_first.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND),
                                  gc.KeyTuple('meta.append_g_zero', 0)])],
                [table_append_first.make_data([], 'MyIngress{}.wait_to_seq'.format(pipe_idx))])
        # TYPE_APPEND_WAIT
        table_append_first.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_first.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND_WAIT),
                                             gc.KeyTuple('meta.append_g_zero', 0)])],
                [table_append_first.make_data([], 'MyIngress{}.wait_append'.format(pipe_idx))])
	# TYPE_APPEND_RESP
        table_append_first.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_first.make_key([gc.KeyTuple('meta.ring_process', TYPE_APPEND_RESP),
                                             gc.KeyTuple('meta.append_g_zero', 0)])],
                [table_append_first.make_data([], 'MyIngress{}.wait_ack_append'.format(pipe_idx))])
        # TYPE_READ_RESP
        table_append_first.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_first.make_key([gc.KeyTuple('meta.ring_process', self.list_of_types[5]),
                                             gc.KeyTuple('meta.append_g_zero', 0)])],
                [table_append_first.make_data([], 'MyIngress{}.get_read_resp'.format(pipe_idx))])
    
    def setup_append_second_tbl(self, pipe_idx):
        table_append_second = self.bfrt_info.table_get("MyIngress{}.append_second_tbl".format(pipe_idx))
        table_append_second.info.key_field_annotation_add("hdr.append.exp_type", "bit<8>")

        table_append_second.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
       
        # TYPE_APPEND exp type = 1
        table_append_second.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_second.make_key([gc.KeyTuple('hdr.append.exp_type', 1)])],
                [table_append_second.make_data([], 'MyIngress{}.exp_type_one'.format(pipe_idx))])

        # TYPE_APPEND exp type = 2
        table_append_second.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_second.make_key([gc.KeyTuple('hdr.append.exp_type', 2)])],
                [table_append_second.make_data([], 'MyIngress{}.exp_type_two'.format(pipe_idx))])

        # TYPE_APPEND exp type = 3 (actual storing info - no streams)
        table_append_second.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_second.make_key([gc.KeyTuple('hdr.append.exp_type', 3)])],
                [table_append_second.make_data([], 'MyIngress{}.regular_idx'.format(pipe_idx))])

        # TYPE_APPEND exp type = 4 (actual storing info - streams)
        table_append_second.entry_add(
                self.pipe_targets[pipe_idx],
                [table_append_second.make_key([gc.KeyTuple('hdr.append.exp_type', 4)])],
                [table_append_second.make_data([], 'MyIngress{}.regular_stream'.format(pipe_idx))])
 
    def setup_ack_action_tbl(self, pipe_idx):
        table_ack_action = self.bfrt_info.table_get("MyIngress{}.ack_action_tbl".format(pipe_idx))
        table_ack_action.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_ack_action.info.key_field_annotation_add("meta.ack_ready", "bit<8>")
        table_ack_action.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
       
        # TYPE_APPEND
        table_ack_action.entry_add(
                self.pipe_targets[pipe_idx],
                [table_ack_action.make_key([gc.KeyTuple('meta.ring_process', self.list_of_types[3]),
                                            gc.KeyTuple('meta.ack_ready', 1)])],
                [table_ack_action.make_data([], 'MyIngress{}.ack_append'.format(pipe_idx))])
        
        # TYPE_READ_RESP
        table_ack_action.entry_add(
                self.pipe_targets[pipe_idx],
                [table_ack_action.make_key([gc.KeyTuple('meta.ring_process', self.list_of_types[5]),
                                            gc.KeyTuple('meta.ack_ready', 1)])],
                [table_ack_action.make_data([], 'MyIngress{}.ack_read'.format(pipe_idx))])
     
    def setup_seq_port_recirc(self, recirc_port_type, pipe_idx):
        table_port_idx = self.bfrt_info.table_get("MyIngress{}.get_port_idx".format(pipe_idx))
        table_port_idx.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")

        table_port_idx.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
       
        table_port_idx.entry_add(
                self.pipe_targets[pipe_idx],
                [table_port_idx.make_key([gc.KeyTuple('meta.recirc_port_type', recirc_port_type)])],
                [table_port_idx.make_data([], 'MyIngress{}.await_sequencing_ports'.format(pipe_idx))])

    def setup_stor_port_recirc(self, recirc_port_type, pipe_idx):
        table_port_idx = self.bfrt_info.table_get("MyIngress{}.get_port_idx".format(pipe_idx))
        table_port_idx.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")

        table_port_idx.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
       
        table_port_idx.entry_add(
                self.pipe_targets[pipe_idx],
                [table_port_idx.make_key([gc.KeyTuple('meta.recirc_port_type', recirc_port_type)])],
                [table_port_idx.make_data([], 'MyIngress{}.emulate_storage_ports'.format(pipe_idx))])

    def setup_ack_latency(self, pipe_idx):
        table_ack_lat_low = self.bfrt_info.table_get("MyIngress{}.ack_get_lower_lat".format(pipe_idx))
        table_ack_lat_low.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_ack_lat_low.info.key_field_annotation_add("meta.ack_ready", "bit<8>")
        table_ack_lat_low.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_ack_lat_up = self.bfrt_info.table_get("MyIngress{}.ack_get_upper_lat".format(pipe_idx))
        table_ack_lat_up.info.key_field_annotation_add("meta.ring_process", "bit<16>")
        table_ack_lat_up.info.key_field_annotation_add("meta.ack_ready", "bit<8>")
        table_ack_lat_up.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
       
        table_ack_lat_low.entry_add(
                self.pipe_targets[pipe_idx],
                [table_ack_lat_low.make_key([gc.KeyTuple('meta.ring_process', self.list_of_types[3]),
                                             gc.KeyTuple('meta.ack_ready', 1)])],
                [table_ack_lat_low.make_data([], 'MyIngress{}.low_latency'.format(pipe_idx))])
        table_ack_lat_up.entry_add(
                self.pipe_targets[pipe_idx],
                [table_ack_lat_up.make_key([gc.KeyTuple('meta.ring_process', self.list_of_types[3]),
                                            gc.KeyTuple('meta.ack_ready', 1)])],
                [table_ack_lat_up.make_data([], 'MyIngress{}.upper_latency'.format(pipe_idx))])
        
    def setup_process_view_messages(self, pipe_idx):
        table_view = self.bfrt_info.table_get("MyIngress{}.view_change_tbl".format(pipe_idx))
        table_view.info.key_field_annotation_add("hdr.ethernet.type", "bit<16>")

        table_view.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Trigger packet
        table_view.entry_add(
                self.pipe_targets[pipe_idx],
                [table_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_TRIGGER)])],
                [table_view.make_data([], 'MyIngress{}.set_trigger_view'.format(pipe_idx))])
        # Finalize packet
        table_view.entry_add(
                self.pipe_targets[pipe_idx],
                [table_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_ACK)])],
                [table_view.make_data([], 'MyIngress{}.set_finalize_view'.format(pipe_idx))])


    ################## ROUTING TABLE FUNCTIONS!!!!! #######################
    def setup_ring_routing(self, ring_members, pipe_idx, port_to_pipe_0, physical_switch_to_logical):
        table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
        table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>")
        table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
        table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.view_hops", "bit<8>")

        table_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Send control packet to next hop in ring OR Send (from view leader) FinalizeView to all ring members
        print("============== DIRECT SWITCH-TO-SWITCH PACKET ROUTING ==================")
        # Used by the: control packet, trigger view change, finalize view change
        for i in range(0, len(physical_switch_to_logical)):
            for j in range(0, len(physical_switch_to_logical[i])):
                port = 0
                logical_switch_id = physical_switch_to_logical[i][j]
                if i == self.physical_switch_id:
                    if j == 0:
                        port = port_to_pipe_0
                    elif j == 1:
                        port = ring_members[i]
                else:
                    port = ring_members[i]
                print("Table Entry: Switch ID: {} to port: {}".format(logical_switch_id, port))
                table_routing.entry_add(
                        self.pipe_targets[pipe_idx],
                        [table_routing.make_key([gc.KeyTuple('meta.next_hop', logical_switch_id),
                                                 gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
                                                 gc.KeyTuple('meta.recirc_port_type', 0),
                                                 gc.KeyTuple('meta.port_idx', 0),
                                                 gc.KeyTuple('meta.route_to_shard', 0),
                                                 gc.KeyTuple('meta.view_hops', 0)])],
                        [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=port)])])
   
    def setup_cpu_dummy(self, pipe_idx, cpu_port):
        table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
        table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>")
        table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
        table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.view_hops", "bit<8>")

        table_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Recirculating appends for sequencing
        table_routing.entry_add(
                self.pipe_targets[pipe_idx],
                [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
                                         gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
                                         gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1, Recirculation for storage IS type 2
                                         gc.KeyTuple('meta.port_idx', 0),
                                         gc.KeyTuple('meta.route_to_shard', 1),
                                         gc.KeyTuple('meta.view_hops', 0)])],
                [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=cpu_port)])])


    def setup_recirculation_routing(self, ports, pipe_idx, port_type):
        table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
        table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>")
        table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
        table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.view_hops", "bit<8>")

        table_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Recirculating appends for sequencing
        print("============== SEQUENCING RECIRCULATION PORTS ==================")
        for i in range(0, len(ports)):
            print("Recirc port idx: {} and port {}".format(i,ports[i]))
            table_routing.entry_add(
                    self.pipe_targets[pipe_idx],
                    [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
                                             gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
                                             gc.KeyTuple('meta.recirc_port_type', port_type), # Recirculation for sequencing IS type 1, Recirculation for storage IS type 2
                                             gc.KeyTuple('meta.port_idx', i),
                                             gc.KeyTuple('meta.route_to_shard', 0),
                                             gc.KeyTuple('meta.view_hops', 0)])],
                    [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=ports[i])])])

    def setup_view_routing(self, ring_members, pipe_idx, port_to_pipe_0, physical_switch_to_logical, cpu_port):
        table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
        table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>")
        table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
        table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.view_hops", "bit<8>")

        table_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        #### View Change Routing (StartView)
        # first time view change packet is in data plane, multicast to all relevant switches (or send packet to control plane if view leader)
        print("============== DIRECT VIEW SWITCH-TO-SWITCH PACKET ROUTING ==================")
        # Used by the: control packet, trigger view change, finalize view change
        for i in range(0, len(physical_switch_to_logical)):
            for j in range(0, len(physical_switch_to_logical[i])):
                port = 0
                logical_switch_id = physical_switch_to_logical[i][j]
                if i == self.physical_switch_id:
                    if j == pipe_idx:
                        port = cpu_port
                    else: 
                        if j == 0:
                            port = port_to_pipe_0
                        elif j == 1:
                            port = ring_members[i]
                else:
                    port = ring_members[i]

                print("Table Entry: Switch ID: {} to port: {}".format(logical_switch_id, port))
                table_routing.entry_add(
                        self.pipe_targets[pipe_idx],
                        [table_routing.make_key([gc.KeyTuple('meta.next_hop', logical_switch_id),
                                                 gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
                                                 gc.KeyTuple('meta.recirc_port_type', 0),
                                                 gc.KeyTuple('meta.port_idx', 0),
                                                 gc.KeyTuple('meta.route_to_shard', 0),
                                                 gc.KeyTuple('meta.view_hops', 1)])],
                        [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=port)])])
  

    def setup_client_routing(self, client_machines, pipe_idx): # TODO needs more thought!!! don't think this works out of the box given the topology
        table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
        table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>")
        table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
        table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
        table_routing.info.key_field_annotation_add("meta.view_hops", "bit<8>")

        table_routing.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        # Recirculation while waiting for acknowledgements??? TODO
        # Sending appends to shards TODO
        # Sending reads to the shards TODO
        # Sending appends to all the subscribers TODO

        ## Route to the client based on IP
        print("============== REGULAR FORWARDING: ROUTE TO EXTERNAL CLIENT OR STORAGE SERVER MACHINES ==================")
        for client_ip_and_port in client_machines:
            #print("For IP dst {}, mac dst addr {} and dst port {}".format(client_ip_and_port[0], client_ip_and_port[1], client_ip_and_port[2]))
            table_routing.entry_add(
                    self.pipe_targets[pipe_idx],
                    [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
                                             gc.KeyTuple('meta.dst_addr', client_ip_and_port[0], prefix_len=32),
                                             gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
                                             gc.KeyTuple('meta.port_idx', 0),
                                             gc.KeyTuple('meta.route_to_shard', 0),
                                             gc.KeyTuple('meta.view_hops', 0)])],
                    [table_routing.make_data(action_name="MyIngress{}.eth_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])
                    
    #def setup_routing(self, recirc_ports, wait_ports, ring_members, client_machines, pipe_idx, physical_cable_ports, port_to_other_pipe, cpu_port): # TODO routing?
    #    table_routing = self.bfrt_info.table_get("MyIngress{}.routing".format(pipe_idx))
    #    table_routing.info.key_field_annotation_add("meta.next_hop", "bit<8>")
    #    table_routing.info.key_field_annotation_add("meta.dst_addr", "bit<32>")
    #    table_routing.info.key_field_annotation_add("meta.recirc_port_type", "bit<2>")
    #    table_routing.info.key_field_annotation_add("meta.port_idx", "bit<8>")
    #    table_routing.info.key_field_annotation_add("meta.route_to_shard", "bit<1>")
    #    table_routing.info.key_field_annotation_add("meta.tail_hops", "bit<8>")
    #    table_routing.info.key_field_annotation_add("meta.view_hops", "bit<8>")

    #    table_routing.attribute_entry_scope_set(self.general_target,
    #            predefined_pipe_scope=True,
    #            predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

    #    # Setup the list of ports
    #    list_of_ports = all_available_switches
    #    
    #    # Send control packet to next hop in ring OR Send (from view leader) FinalizeView to all ring members
    #    print("============== DIRECT SWITCH-TO-SWITCH PACKET ROUTING ==================")
    #    # Used by the: control packet, trigger view change, finalize view change
    #    for i in range(0, len(ring_members)):
    #        #print("Switch ID: {} to port: {}".format(i, all_available_switches[i]))
    #        table_routing.entry_add(
    #                self.pipe_targets[pipe_idx],
    #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', i),
    #                                         gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                         gc.KeyTuple('meta.recirc_port_type', 0),
    #                                         gc.KeyTuple('meta.port_idx', 0),
    #                                         gc.KeyTuple('meta.route_to_shard', 0),
    #                                         gc.KeyTuple('meta.tail_hops', 0),
    #                                         gc.KeyTuple('meta.view_hops', 0)])],
    #                [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=ring_members[i])])])


    #    # Recirculating appends for sequencing
    #    print("============== SEQUENCING RECIRCULATION PORTS ==================")
    #    for i in range(0, len(recirc_ports)):
    #        #print("Recirc port idx: {} and port {}".format(i, recirc_ports[i]))
    #        table_routing.entry_add(
    #                self.pipe_targets[pipe_idx],
    #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
    #                                         gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                         gc.KeyTuple('meta.recirc_port_type', 1), # Recirculation for sequencing IS type 1
    #                                         gc.KeyTuple('meta.port_idx', i),
    #                                         gc.KeyTuple('meta.route_to_shard', 0),
    #                                         gc.KeyTuple('meta.tail_hops', 0),
    #                                         gc.KeyTuple('meta.view_hops', 0)])],
    #                [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=recirc_ports[i])])])

    #    # Recirculating appends for storage emulation
    #    print("============== STORAGE RECIRCULATION PORTS ==================")
    #    for i in range(0, len(wait_ports)):
    #        #print("Wait port idx: {} and port {}".format(i, wait_ports[i]))
    #        table_routing.entry_add(
    #                self.pipe_targets[pipe_idx],
    #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
    #                                         gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                         gc.KeyTuple('meta.recirc_port_type', 2), # Recirculation for sequencing IS type 1
    #                                         gc.KeyTuple('meta.port_idx', i),
    #                                         gc.KeyTuple('meta.route_to_shard', 0),
    #                                         gc.KeyTuple('meta.tail_hops', 0),
    #                                         gc.KeyTuple('meta.view_hops', 0)])],
    #                [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=wait_ports[i])])])

    #    #### View Change Routing (StartView)
    #    # first time view change packet is in data plane, multicast to all relevant switches (or send packet to control plane if view leader)

    #    print("============== SETUP VIEW LEADER MULTICAST ==================")
    #    key = "StartView"
    #	mc_grp_id = self.add_mcast_group(physical_cable_ports, key, pipe_idx)
    #    print("For view change multicast, group number: {}".format(mc_grp_id))
    #    table_routing.entry_add(
    #            self.pipe_targets[pipe_idx],
    #            [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
    #                                     gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                     gc.KeyTuple('meta.recirc_port_type', 0),
    #                                     gc.KeyTuple('meta.port_idx', 0),
    #                                     gc.KeyTuple('meta.route_to_shard', 0),
    #                                     gc.KeyTuple('meta.tail_hops', 0),
    #                                     gc.KeyTuple('meta.view_hops', 1)])],
    #    	[table_routing.make_data(action_name="MyIngress{}.send_multicast".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="group_id", val=mc_grp_id)])])

    #    # view change received by non-view leader who needs to send back to view leader (if not self) 
    #    print("============== RESPOND TO VIEW LEADER ==================")
    #    idx = 0
    #    for i in range(1, len(physical_cable_ports), 2): # NEED to UPDATE THIS 
    #        #print("For view change, switch ID hop: {}, port: {}".format(i, send_port))
    #        send_port = physical_cable_ports[idx]
    #        idx += 1
    #        table_routing.entry_add(
    #                self.pipe_targets[pipe_idx],
    #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', i+1), # Always going to be an odd switch ID
    #                                         gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                         gc.KeyTuple('meta.recirc_port_type', 0),
    #                                         gc.KeyTuple('meta.port_idx', 0),
    #                                         gc.KeyTuple('meta.route_to_shard', 0),
    #                                         gc.KeyTuple('meta.tail_hops', 0),
    #                                         gc.KeyTuple('meta.view_hops', 2)])],
    #                [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=send_port)])])

    #    # updated view change packet received by view leader, forward to control plane
    #    print("For view change phase, switch ID: {}, port: {}".format(i, cpu_port))
    #    table_routing.entry_add(
    #            self.pipe_targets[pipe_idx],
    #            [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
    #                                     gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                     gc.KeyTuple('meta.recirc_port_type', 0),
    #                                     gc.KeyTuple('meta.port_idx', 0),
    #                                     gc.KeyTuple('meta.route_to_shard', 0),
    #                                     gc.KeyTuple('meta.tail_hops', 0),
    #                                     gc.KeyTuple('meta.view_hops', 3)])],
    #            [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=cpu_port)])])

    #    # Recirculation while waiting for acknowledgements??? TODO

    #    # Sending appends to shards TODO

    #    # Sending reads to the shards TODO

    #    # Sending appends to all the subscribers TODO

    #    # Send tail to next hop in the ring
    #    print("============== TAIL FORWARDING RULES ==================")
    #    for j in range(0, len(ring_members)):
    #        for i in range(0, len(ring_members)):
    #            #print("Next hop {} and number of hops {} and destination port {}".format(j, i, all_available_switches[i]))
    #            table_routing.entry_add(
    #                    self.pipe_targets[pipe_idx],
    #                    [table_routing.make_key([gc.KeyTuple('meta.next_hop', j),
    #                                             gc.KeyTuple('meta.dst_addr', 0, prefix_len=32),
    #                                             gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
    #                                             gc.KeyTuple('meta.port_idx', 0),
    #                                             gc.KeyTuple('meta.route_to_shard', 0),
    #                                             gc.KeyTuple('meta.tail_hops', i+1),
    #                                             gc.KeyTuple('meta.view_hops', 0)])],
    #                    [table_routing.make_data(action_name="MyIngress{}.send_to".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=all_available_switches[i])])])

    #    ## Route to the client based on IP (NOT tail related, just a normal packet)
    #    print("============== REGULAR FORWARDING: ROUTE TO EXTERNAL CLIENT OR STORAGE SERVER MACHINES ==================")
    #    for client_ip_and_port in client_machines:
    #        #print("For IP dst {}, mac dst addr {} and dst port {}".format(client_ip_and_port[0], client_ip_and_port[1], client_ip_and_port[2]))
    #        table_routing.entry_add(
    #                self.pipe_targets[pipe_idx],
    #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
    #                                         gc.KeyTuple('meta.dst_addr', client_ip_and_port[0], prefix_len=32),
    #                                         gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
    #                                         gc.KeyTuple('meta.port_idx', 0),
    #                                         gc.KeyTuple('meta.route_to_shard', 0),
    #                                         gc.KeyTuple('meta.tail_hops', 0),
    #                                         gc.KeyTuple('meta.view_hops', 0)])],
    #                [table_routing.make_data(action_name="MyIngress{}.eth_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])
    #    
    #    # For Tail packets ONLY: Route to the client based on IP if it has visited every ring member
    #    print("============== TAIL FORWARDING: ROUTE TO EXTERNAL CLIENT OR STORAGE SERVER MACHINES ==================")
    #    for client_ip_and_port in client_machines:
    #        #print("For IP dst {}, mac dst addr {} and dst port {}".format(client_ip_and_port[0], client_ip_and_port[1], client_ip_and_port[2]))
    #        table_routing.entry_add(
    #                self.pipe_targets[pipe_idx],
    #                [table_routing.make_key([gc.KeyTuple('meta.next_hop', 0),
    #                                         gc.KeyTuple('meta.dst_addr', client_ip_and_port[0], prefix_len=32),
    #                                         gc.KeyTuple('meta.recirc_port_type', 0), # Recirculation for sequencing IS type 1
    #                                         gc.KeyTuple('meta.port_idx', 0),
    #                                         gc.KeyTuple('meta.route_to_shard', 0),
    #                                         gc.KeyTuple('meta.tail_hops', len(ring_members)),
    #                                         gc.KeyTuple('meta.view_hops', 0)])],
    #                [table_routing.make_data(action_name="MyIngress{}.eth_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])

    ################ EGRESS ##################
    def setup_check_dur_table(self, lower_bound, upper_bound, pipe_idx):
        # Cntrl ID -> Send Port
        table_check_dur = self.bfrt_info.table_get("MyEgress{}.check_duration".format(pipe_idx))
        table_check_dur.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_check_dur.info.key_field_annotation_add("meta.elapsed_time", "bit<16>")
        table_check_dur.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_check_dur.make_key([gc.KeyTuple('meta.elapsed_time', low=lower_bound, high=upper_bound)])],
            [table_check_dur.make_data([], "MyEgress{}.update_type".format(pipe_idx))])
   
    def setup_view_number_info(self, pipe_idx):
        table_active_view = self.bfrt_info.table_get("MyEgress{}.get_view_number_info".format(pipe_idx))
        table_active_view.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_active_view.info.key_field_annotation_add("hdr.ethernet.type", "bit<16>")

        # Control packet: If control packet is received, then view change marker gets unset
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_CONTROL)])],
            [table_active_view.make_data([], "MyEgress{}.check_cntrl_view_change".format(pipe_idx))])
	
	# Trigger packet: If trigger packet is received, then view change marker gets set (but NOT view change status)
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_TRIGGER)])],
            [table_active_view.make_data([], "MyEgress{}.check_accept_view_change".format(pipe_idx))])

	# FinalizeView packet: If view packet is received, then view change marker gets set
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_ACK)])],
            [table_active_view.make_data([], "MyEgress{}.update_view_number".format(pipe_idx))])

    def setup_active_view_table(self, pipe_idx):
        table_active_view = self.bfrt_info.table_get("MyEgress{}.update_active_view".format(pipe_idx))
        table_active_view.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_active_view.info.key_field_annotation_add("hdr.ethernet.type", "bit<16>")
        table_active_view.info.key_field_annotation_add("meta.accept_view_change", "bit<16>")

        # Control packet: If control packet is received, then view change marker gets unset
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_CONTROL),
                                         gc.KeyTuple('meta.accept_view_change', 0)])],
            [table_active_view.make_data([], "MyEgress{}.unset_active_view".format(pipe_idx))])
	
	# Trigger packet: If trigger packet is received, then view change marker gets set (but NOT view change status)
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_TRIGGER),
                                         gc.KeyTuple('meta.accept_view_change', 1)])],
            [table_active_view.make_data([], "MyEgress{}.trigger_active_view".format(pipe_idx))])

        # Induce view packet
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_INDUCE),
                                         gc.KeyTuple('meta.accept_view_change', 0)])],
            [table_active_view.make_data([], "MyEgress{}.induce_active_view".format(pipe_idx))])
        # Every other packet
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_IP),
                                         gc.KeyTuple('meta.accept_view_change', 0)])],
            [table_active_view.make_data([], "MyEgress{}.get_active_view".format(pipe_idx))])

    def setup_view_logistics_table(self, pipe_idx):
        table_active_view = self.bfrt_info.table_get("MyEgress{}.view_logistics".format(pipe_idx))
        table_active_view.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_active_view.info.key_field_annotation_add("hdr.ethernet.type", "bit<16>")
        table_active_view.info.key_field_annotation_add("meta.accept_view_change", "bit<16>")

        # Control packet: If control packet is received, then view change marker gets unset
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_CONTROL),
                                         gc.KeyTuple('meta.accept_view_change', 0)])],
            [table_active_view.make_data([], "MyEgress{}.log_end_ts".format(pipe_idx))])
	
	# Trigger packet: If trigger packet is received, then view change marker gets set (but NOT view change status)
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('hdr.ethernet.type', TYPE_VIEW_TRIGGER),
                                         gc.KeyTuple('meta.accept_view_change', 1)])],
            [table_active_view.make_data([], "MyEgress{}.log_start_ts".format(pipe_idx))])
        
    def setup_check_view_change_table(self, pipe_idx):
        table_active_view = self.bfrt_info.table_get("MyEgress{}.check_view_change".format(pipe_idx))
        table_active_view.attribute_entry_scope_set(self.general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_active_view.info.key_field_annotation_add("meta.view_change_active", "bit<8>")
        table_active_view.info.key_field_annotation_add("hdr.ethernet.type", "bit<16>")

        # Packets to drop
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('meta.view_change_active', 1),
                                         gc.KeyTuple('hdr.ethernet.type', TYPE_IP)])],
            [table_active_view.make_data([], "MyEgress{}.drop".format(pipe_idx))])
        table_active_view.entry_add(
            self.pipe_targets[pipe_idx], 
            [table_active_view.make_key([gc.KeyTuple('meta.view_change_active', 1),
                                         gc.KeyTuple('hdr.ethernet.type', TYPE_CONTROL)])],
            [table_active_view.make_data([], "MyEgress{}.drop".format(pipe_idx))])
     
    def setup_seq_mtr_table(self, rate_pps, burst_pkts, pipe_idx):
        table_mtr = self.bfrt_info.table_get("MyIngress{}.seq_admission".format(pipe_idx))
        #table_mtr.attribute_entry_scope_set(self.general_target,
        #        predefined_pipe_scope=True,
        #        predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        # Packets to drop
        table_mtr.entry_add(
            self.general_target, #self.pipe_targets[pipe_idx], 
            [table_mtr.make_key([gc.KeyTuple('$METER_INDEX', 0)])],
            [table_mtr.make_data([gc.DataTuple('$METER_SPEC_CIR_PPS', int(rate_pps)),
                          	  gc.DataTuple('$METER_SPEC_PIR_PPS', int(rate_pps)),
                          	  gc.DataTuple('$METER_SPEC_CBS_PKTS', int(burst_pkts)),
                          	  gc.DataTuple('$METER_SPEC_PBS_PKTS', int(burst_pkts))])])
              
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


