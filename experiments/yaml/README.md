Here are all the required fields for the client yaml file:

- log_level: This speciies how many log statements should be printed
- send_port: Port the program will send out of
- recv_port: Port the program will receive on
- send_threads: The number of threads used for sending [INACTIVE]
- socket_type: Indicates whether "UDP" or "RAW" sockets will be used
- self_ip: The IP of the machine the client is running on
- interface: Interface the client will 
- batch_size: Number of bytes allowed in a batch [INACTIVE]
- batch_on: Indicates whether batching is enabled or not
- num_pkt_types: Number of packet types the program will use
- packet_types: Comprised of a list "ips" which in turn correspond to a packet type defined in the program
	- ips: [List of IPs that the packet type should be sent to]
- sequencer_type: Integer [0, 1] indicating the type of sequencer the client is communicating with
- trace_file: A file containing a list of predetermined operations and payloads
- experiment_duration: How long the experiment runs for
- payload_size: How large the payload should be

Here are all the required fields for the storage yaml file:

- log_level: This speciies how many log statements should be printed
- send_port: Port the program will send out of
- recv_port: Port the program will receive on
- send_threads: The number of threads used for sending [INACTIVE]
- socket_type: Indicates whether "UDP" or "RAW" sockets will be used
- self_ip: The IP of the machine the client is running on
- interface: Interface the client will 
- batch_size: Number of bytes allowed in a batch [INACTIVE]
- batch_on: Indicates whether batching is enabled or not
- num_pkt_types: Number of packet types the program will use
- packet_types: Comprised of a list "ips" which in turn correspond to a packet type defined in the program
	- ips: [List of IPs that the packet type should be sent to]
- storage_type: Integer [0, 3) indicating the type of storage server the client is communicating with
- shard_id: an ID of the shard the storage server is part of
- shard_switch_id: an ID of the switch the storage server should be interacting with

