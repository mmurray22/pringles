#!/usr/bin/python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import Controller, RemoteController
from mininet.log import info, setLogLevel
from mininet.cli import CLI

from p4_mininet import P4Switch, P4Host

class RingController( Controller ):
    def start( net ):
        "Start Controller"
        net.pox = '/home/micahmurray/pox/pox.py'
        net.cmd( self.pox, '--verbose samples.pretty_log openflow.spanning_tree &' )

    def stop( net ):
        "Stop Controller"
        net.cmd( 'kill %' + self.pox )


# controllers = { 'ringcntrl': RingController }

if __name__ == '__main__':
    setLogLevel('info')
    net = Mininet(host=P4Host, switch=P4Switch, controller=RemoteController)

    # Create four switches
    simple_router_json_path = '../targets/simple_router/simple_router.json'
    json_path = '/home/micahmurray/pringles/code/p4/sequencing_only.json'
    sw_path = '../build/targets/simple_switch/simple_switch'
    s1 = net.addSwitch('s1', 
                        sw_path = sw_path,
                        json_path = json_path,
                        thrift_port = 9090,
                        pcap_dump = False,
                        enable_debugger= False)
    s2 = net.addSwitch('s2', 
                        sw_path = sw_path,
                        json_path = json_path,
                        thrift_port = 9091,
                        pcap_dump = False,
                        enable_debugger= False)
    s3 = net.addSwitch('s3', 
                        sw_path = sw_path,
                        json_path = json_path,
                        thrift_port = 9190,
                        pcap_dump = False,
                        enable_debugger= False)
    s4 = net.addSwitch('s4', 
                        sw_path = sw_path,
                        json_path = json_path,
                        thrift_port = 9191,
                        pcap_dump = False,
                        enable_debugger= False)

    # Create four hosts
    h1 = net.addHost('h1', mac='00:00:00:00:00:01', ip='10.0.0.1/24')
    h2 = net.addHost('h2', mac='00:00:00:00:00:02', ip='10.0.0.2/24' )
    h3 = net.addHost('h3', mac='00:00:00:00:00:03', ip='10.0.0.3/24')
    h4 = net.addHost('h4', mac='00:00:00:00:00:04', ip='10.0.0.4/24' )

    c7 = net.addController('c7', controller=RemoteController, ip='127.0.0.1', port=6633)
    
    # Connect the hosts to the switches
    net.addLink(h1, s1)
    net.addLink(h2, s2)
    net.addLink(h3, s3)
    net.addLink(h4, s4)

    # Connect the switches to each other (the "multihop" link)
    net.addLink(s1, s2)
    net.addLink(s2, s3)
    net.addLink(s3, s4)
    net.addLink(s1, s4)
    
    # Start the network
    net.build()
    net.start()
    CLI( net )

    # Configuration commands
    s1.cmd("sh ovs-ofctl add-flow s1 priority=1,arp,actions=flood")
    s1.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.1.0/24,actions=output:1")
    s1.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.2.0/24,actions=output:2")
    s1.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.3.0/24,actions=output:3")
    s1.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.4.0/24,actions=output:3")

    #s1.cmd("ovs-ofctl mod-table s1 cntrl_id_to_ip ") # ??

    s2.cmd("sh ovs-ofctl add-flow s1 priority=1,arp,actions=flood")
    s2.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.1.0/24,actions=output:3")
    s2.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.2.0/24,actions=output:1")
    s2.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.3.0/24,actions=output:2")
    s2.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.4.0/24,actions=output:2")

    s3.cmd("sh ovs-ofctl add-flow s1 priority=1,arp,actions=flood")
    s3.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.1.0/24,actions=output:3")
    s3.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.2.0/24,actions=output:3")
    s3.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.3.0/24,actions=output:1")
    s3.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.4.0/24,actions=output:2")

    s4.cmd("sh ovs-ofctl add-flow s1 priority=1,arp,actions=flood")
    s4.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.1.0/24,actions=output:2")
    s4.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.2.0/24,actions=output:2")
    s4.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.3.0/24,actions=output:2")
    s4.cmd("ovs-ofctl add-flow s1 priority=10,ip,nw_dst=10.0.4.0/24,actions=output:1")
    
    net.stop()
