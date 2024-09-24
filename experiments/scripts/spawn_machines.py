# Python 3
# Script to spawn machines on Cloudlab
# Currently supports 1 platform: 
# Plans to extend to Azure and AWS

# Import the Portal object
import geni.portal as portal
# Import the ProtoGENI library
import geni.rspec.pg as rspec

# Put unfo like usernames, ssh_keys, etc. in this yaml file (not uploaded to Github)
personal_info_yaml = "../config/personal.yaml"

def setup_cloudlab(config_yaml_dict):
   request = portal.context.makeRequestRSpec()
   common_str = "node"
   nodes = []
   total_num_nodes = 0
   for j in range(0, 3):
       node_type = None
       if j == 0:
           node_type = config_yaml_dict.storage
       elif j == 1:
           node_type = config_yaml_dict.client
       else:
           node_type = config_yaml_dict.sequencer
       for i in range(total_num_nodes, node_type.num):
           node_str = common_str + str(i)
           node = request.RawPC(node_str)
           node.hardware_type = node_type.type
           node.image = config_yaml_dict.machine_image 
           # TODO: Option to also customize bandwidth, memory
           nodes.append(node)
   # TODO Create network linkes between all nodes (NOTE: Can be fewer links, doing A2A for simplicity)
   # Desired line: [for each pair of unique nodes (node1, node2): link1 = request.Link(members = [node1, node2]]
   portal.context.printRequestRSpec()


#def setup_aws():
   # TODO

#def setup_azure():
   # TODO
