from multicast_helpers import *
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2

class MulticastTables:
    def initialize(self, bfrt_info, global_group_id, logger):
        self.global_group_id = global_group_id
        self.logger = logger
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        self.node_table = bfrt_info.table_get("$pre.node")
        self.ecmp_table = bfrt_info.table_get("$pre.ecmp")
        self.lag_table = bfrt_info.table_get("$pre.lag")
        self.prune_table = bfrt_info.table_get("$pre.prune")
        self.mirror_cfg_table = bfrt_info.table_get("$mirror.cfg")

    def setup_shard_multicast_groups(self, target, bfrt_info, shard_to_port_gp):
        # Map shard IDs to their respective multicast group (each group has a collection of ports to storage server)
        table_shard = bfrt_info.table_get("MyIngress.get_shard_port")
        table_shard.info.key_field_annotation_add("hdr.ring_type.shard_id", "bit<32>")
        self.logger.info(shard_to_port_gp)
        for key, dev_ports in shard_to_port_gp.items():
            rid = 0x321 + self.global_group_id
            xid = 0x432 + self.global_group_id
            mbr_lags = []
            self.logger.info("RID: ", rid, " XID: ", xid, " GPID: ", self.global_group_id, " DEV: ", dev_ports)
            mc = MCTree(self, target, self.global_group_id)
            mc.add_node(rid, xid, dev_ports, mbr_lags)
            self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
            table_shard.entry_add(
                target, 
                [table_shard.make_key([gc.KeyTuple('hdr.ring_type.shard_id', key)])],
                [table_shard.make_data(action_name="MyIngress.shard_port", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
            self.global_group_id += 1
    
    def setup_subscribe_routing_table(self, target, bfrt_info, ports_to_ring_members, cpu_port):
        # Subscription 
        table_sub = bfrt_info.table_get("MyIngress.submit_subscription")
        table_sub.info.key_field_annotation_add("hdr.sub.subscribe", "bit<32>")

        # Broadcast subscription packet to all switches
        for i in range(0, len(ports_to_ring_members)):
            if ports_to_ring_members[i] == 0:
                ports_to_ring_members[i] = cpu_port
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        rid = 0x321 + self.global_group_id
        xid = 0x432 + self.global_group_id
        mbr_lags = []
        self.logger.info("RID: ", rid, " XID: ", xid, " GPID: ", self.global_group_id, " DEV: ", ports_to_ring_members)
        self.general_sub_mc = MCTree(self, target, self.global_group_id)
        self.general_sub_group = self.global_group_id
        self.general_sub_mc.add_node(rid, xid, ports_to_ring_members, mbr_lags)
        self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
        table_sub.entry_add(
                target, 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 1)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_all", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
        self.global_group_id += 1

        # Only send to local control plane
        table_sub.entry_add(
                target, 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 0)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_self", data_field_list_in=[gc.DataTuple(name="port", val=cpu_port)])])
    
    def update_stream_subscriber_table(self, bfrt_info, target, stream_id, subscriber_port):
        self.logger.info(stream_id)
        self.logger.info(subscriber_port)
        self.logger.info(self.stream_sub_dict)
        if stream_id not in self.stream_sub_dict.keys():
            table_acks = bfrt_info.table_get("MyIngress.route_subscriber_acks")
            table_acks.info.key_field_annotation_add("hdr.append.stream_id", "bit<32>")

            # Subscription group (non-stream & stream)
            stream_sub_mc = MCTree(self, target, self.global_group_id)
            stream_group_id = self.global_group_id
            if stream_id == 0:
                self.general_sub_group = stream_group_id
            rid = 0x321 + stream_group_id
            xid = 0x432 + stream_group_id
            stream_sub_mc.add_node(rid, xid, [subscriber_port], [])
            self.stream_sub_dict[stream_id] = stream_sub_mc
            self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])

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
                self.mirror_cfg_table.entry_add(target, [ mirror_session_bfrt_key ], [ mirror_session_bfrt_data ])
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
                self.mirror_cfg_table.entry_add(target, [ mirror_session_bfrt_key ], [ mirror_session_bfrt_data ])

            # Table acks 
            table_acks.entry_add(
                    target, 
                    [table_acks.make_key([gc.KeyTuple('hdr.append.stream_id', stream_id)])],
                    [table_acks.make_data(action_name="MyIngress.forward_to_subscribers", data_field_list_in=[gc.DataTuple(name="mirror_session_id", val=self.sid)])])
            self.global_group_id += 1
            self.sid += 1
        else:
            self.stream_sub_dict[stream_id].get_first_node().addMbrs([subscriber_port], [])


