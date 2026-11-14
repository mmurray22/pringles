import ptf
import ptf.dataplane as dataplane
import bfrt_grpc.client as gc
import logging

class PortSetup:
    def initialize_ports(self, bfrt_info, logger):
        self.port_table = bfrt_info.table_get("$PORT")
        self.port_hdl_info_table = bfrt_info.table_get("$PORT_HDL_INFO")
        self.port_fp_idx_info_table = bfrt_info.table_get("$PORT_FP_IDX_INFO")
        self.port_str_info_table = bfrt_info.table_get("$PORT_STR_INFO")
	self.dataplane = ptf.dataplane_instance
        self.dataplane.flush()
        self.logger = logger
	self.ports = []

    def port_setup(self, target, port, use_loopback, port_speed, port_fec):
        self.logger.info("Test Port cfg table add and read operations")
        self.logger.info("PortCfgTest: Adding entry for port %d", port)
        if use_loopback:
            self.port_table.entry_add(
                target,
                [self.port_table.make_key([gc.KeyTuple('$DEV_PORT', port)])],
                [self.port_table.make_data([gc.DataTuple('$SPEED', str_val=port_speed),
                                            gc.DataTuple('$FEC', str_val=port_fec),
                                            gc.DataTuple('$TX_MTU', 9000),
                                            gc.DataTuple('$RX_MTU', 9000),
                                            gc.DataTuple('$PORT_ENABLE', bool_val=True),
                                            gc.DataTuple('$LOOPBACK_MODE', str_val="BF_LPBK_MAC_NEAR")])])
        else:
            self.port_table.entry_add(
                target,
                [self.port_table.make_key([gc.KeyTuple('$DEV_PORT', port)])],
                [self.port_table.make_data([gc.DataTuple('$SPEED', str_val=port_speed),
                                            gc.DataTuple('$FEC', str_val=port_fec),
                                            gc.DataTuple('$TX_MTU', 9000),
                                            gc.DataTuple('$RX_MTU', 9000),
                                            gc.DataTuple('$PORT_ENABLE', bool_val=True)])])
					    #gc.DataTuple('$N_LANES', 4)])])

    def store_all_ports(self, loop_ports, no_loop_ports):
    	self.ports = sorted(list(loop_ports | no_loop_ports))
    
    def port_stats_snapshot(self, bfrt_info, target):
        table = bfrt_info.table_get("$PORT_STAT")
        snap = {}
        for p in self.ports:
            try:
                resp = table.entry_get(target, [table.make_key([gc.KeyTuple('$DEV_PORT', p)])], {"from_hw": True})
                data, _ = next(resp)
                d = data.to_dict()
                snap[p] = dict((k.lstrip('$'), v) for k, v in d.items() if 'Frames' in k)
            except Exception as e:
                snap[p] = {'error': repr(e)}
        return snap
 
 
    def port_stats_report(self, before, after):
        changed = 0
        for p in sorted(after):
            a, b = after[p], before.get(p, {})
            if 'error' in a:
                print("port {} could not read: {}".format(p, a['error']))
                continue
            deltas = []
            for k in sorted(a):
                if k in ('FramesReceivedOK', 'FramesTransmittedOK', 'FramesReceivedAll', 'FramesTransmittedAll'):
                    d = a[k] - b.get(k, 0)
                    if d:
                        deltas.append("%s +%d" % (k, d))
            if deltas:
                changed += 1
                print("port {}  {}".format(p, ",  ".join(deltas)))
        if not changed:
            print("no port counter changed -- nothing entered or left any of the ports you listed")
