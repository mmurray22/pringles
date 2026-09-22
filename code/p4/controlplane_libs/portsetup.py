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
    

        
