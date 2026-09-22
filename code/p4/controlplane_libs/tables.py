import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2

class P4Tables:
    @staticmethod
    def setup_client_response_table(target, bfrt_info, client_ips): # Entry in client_ips: [IP, MAC, PORT]
        # ipv4_lpm
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        for client_ip_and_port in client_ips:
            table_ipv4.entry_add(
                    target, 
                    [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', client_ip_and_port[0], prefix_len=32)])],
                    [table_ipv4.make_data(action_name="MyIngress.ipv4_forward", data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])

    @staticmethod
    def setup_circulate_table(bfrt_info, general_target, pipe_target, recirc_ports, pipe_idx):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        # Set all pipes to be in different scopes. Also known as Single scope
        table_circulate = bfrt_info.table_get("MyIngress{}.circulate_table".format(pipe_idx))
        table_circulate.attribute_entry_scope_set(general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_circulate.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        for i in range(0, len(recirc_ports)):
            print("Recirc port idx: {} and port {}".format(i, recirc_ports[i]))
            table_circulate.entry_add(
                    pipe_target, 
                    [table_circulate.make_key([gc.KeyTuple('meta.port_idx', i)])],
                    [table_circulate.make_data(action_name="MyIngress{}.circulate_port".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=recirc_ports[i])])])
    @staticmethod
    def setup_wait_table(bfrt_info, general_target, pipe_target, wait_ports, pipe_idx):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        # Set all pipes to be in different scopes. Also known as Single scope
        table_wait = bfrt_info.table_get("MyIngress{}.wait_table".format(pipe_idx))
        table_wait.attribute_entry_scope_set(general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        table_wait.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        for i in range(0, len(wait_ports)):
            print("Wait port idx: {} and port {}".format(i, wait_ports[i]))
            table_wait.entry_add(
                    pipe_target, 
                    [table_wait.make_key([gc.KeyTuple('meta.port_idx', i)])],
                    [table_wait.make_data(action_name="MyIngress{}.wait_port".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=wait_ports[i])])])
    
    @staticmethod
    def setup_recirc_port_table(bfrt_info, general_target, pipe_target, tot_num_recirc_ports,pipe_idx):
        table_recirc = bfrt_info.table_get("MyIngress{}.num_recirc_port_table".format(pipe_idx))
        table_recirc.attribute_entry_scope_set(general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_recirc.default_entry_set(
                pipe_target, 
                table_recirc.make_data(action_name="MyIngress{}.get_num_recirc".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="num_recirc_ports", val=tot_num_recirc_ports)]))
            
    @staticmethod
    def setup_wait_port_table(bfrt_info, general_target, pipe_target, tot_num_wait_ports, pipe_idx):
        table_wait = bfrt_info.table_get("MyIngress{}.num_wait_port_table".format(pipe_idx))
        table_wait.attribute_entry_scope_set(general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        table_wait.default_entry_set(
                pipe_target, 
                table_wait.make_data(action_name="MyIngress{}.get_num_wait".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="num_wait_ports", val=tot_num_wait_ports)]))

    @staticmethod
    def setup_ack_const(target, bfrt_info, ack_threshold):
        table_ack = bfrt_info.table_get("MyIngress.get_ack_threshold")
        table_ack.entry_add(
                target, 
                [table_ack.make_data(action_name="MyIngress.ack_threshold", data_field_list_in=[gc.DataTuple(name="threshold", val=ack_threshold)])])

    @staticmethod
    def setup_num_shard_const(target, bfrt_info, num_shards):
        table_shard = bfrt_info.table_get("MyIngress.get_num_shards")
        table_shard.entry_add(
                target, 
                [table_shard.make_data(action_name="MyIngress.num_shards", data_field_list_in=[gc.DataTuple(name="shards", val=num_shards)])])

    @staticmethod
    def setup_shard_size_const(target, bfrt_info, shard_size):
        table_shard = bfrt_info.table_get("MyIngress.get_shard_size")
        table_shard.entry_add(
                target, 
                [table_shard.make_data(action_name="MyIngress.shard_size", data_field_list_in=[gc.DataTuple(name="size", val=shard_size)])])

    @staticmethod
    def setup_switch_check(target, bfrt_info, size_of_ring, ports_to_ring_members):
        # Check switch routing
        table_process = bfrt_info.table_get("MyIngress.check_switch_routing")
        table_process.info.key_field_annotation_add("hdr.ring_type.switch_to_process", "bit<32>")
        print("Ports to ring members:")
        print(ports_to_ring_members)
        for i in range(0, size_of_ring):
            if ports_to_ring_members[i] == 0:
                continue; # This is the index of the current switch
            switch_id = i
            table_process.entry_add(
                target, 
                [table_process.make_key([gc.KeyTuple('hdr.ring_type.switch_to_process', switch_id)])],
                [table_process.make_data(action_name="MyIngress.route_next_switch", data_field_list_in=[gc.DataTuple(name="port", val=ports_to_ring_members[i])])])

    @staticmethod
    def setup_cntrl_table(bfrt_info, general_target, pipe_target, in_cntrl, out_cntrl, cntrl_port, pipe_idx):
        # Cntrl ID -> Send Port
        table_cntrl = bfrt_info.table_get("MyIngress{}.cntrl_id_to_ip".format(pipe_idx))
        table_cntrl.attribute_entry_scope_set(general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "bit<32>")
        table_cntrl.entry_add(
                pipe_target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])],
                [table_cntrl.make_data(action_name="MyIngress{}.cntrl_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port), gc.DataTuple(name="pkt_id", val=out_cntrl)])])
    
    @staticmethod
    def setup_check_dur_table(bfrt_info, general_target, pipe_target, lower_bound, upper_bound, pipe_idx):
        # Cntrl ID -> Send Port
        table_check_dur = bfrt_info.table_get("MyEgress{}.check_duration".format(pipe_idx))
        table_check_dur.attribute_entry_scope_set(general_target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        table_check_dur.info.key_field_annotation_add("hdr.ring_type.raw_elapsed_time", "bit<16>")
        table_check_dur.entry_add(
            pipe_target, 
            [table_check_dur.make_key([gc.KeyTuple('hdr.ring_type.raw_elapsed_time', low=lower_bound, high=upper_bound)])],
            [table_check_dur.make_data([], "MyEgress{}.update_type".format(pipe_idx))])

    @staticmethod
    def setup_view_check(target, bfrt_info, view):
        # Ring View
        table_view = bfrt_info.table_get("MyIngress.check_cntrl_view")
        table_view.info.key_field_annotation_add("hdr.cntrl.ring_view", "bit<16>")
        table_view.info.data_field_annotation_add("view", "MyIngress.update_cntrl_view", "bit<16>")
        upper_end = view - 1
        updated_view = view
        print(upper_end)
        print(updated_view)
        print("View checked!")
        table_view.entry_add(
                target, 
                [table_view.make_key([gc.KeyTuple('hdr.cntrl.ring_view', low=1, high=upper_end)])],
                [table_view.make_data(action_name="MyIngress.update_cntrl_view", data_field_list_in=[gc.DataTuple(name="view", val=updated_view)])])
        return view

    @staticmethod
    def setup_tail_table(target, bfrt_info, size_of_ring, cntrl_port):
        # Tail
        table_tail = bfrt_info.table_get("MyIngress.process_tail")
        table_tail.info.key_field_annotation_add("hdr.tail.hops", "bit<32>")
        table_tail.entry_add(
                target, 
                [table_tail.make_key([gc.KeyTuple('hdr.tail.hops', low=0, high=(size_of_ring-1))])], # size_of_ring - 1?
                [table_tail.make_data(action_name="MyIngress.forward_tail", data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port)])])

    # Acknowledgement tables (primarily handle subscription responses - for both streams & non-streams)
    @staticmethod
    def setup_subscriber_acks_table(target, bfrt_info):
        # Subscriber acks
        table_acks = bfrt_info.table_get("MyIngress.route_subscriber_acks")
        table_acks.info.key_field_annotation_add("hdr.append.stream_id", "bit<32>")
