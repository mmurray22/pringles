Here are all the required fields for the client yaml file:

- loglevel: This speciies how many log statements should be printed
- send_port: Port the program will send out of
- recv_port: Port the program will receive on
- send_threads: The number of threads used for sending [INACTIVE]
- socket_type: Indicates whether "UDP" or "RAW" sockets will be used
- self_ip: The IP of the machine the client is running on
- interface: Interface the client will 
- batch_size: Number of bytes allowed in a batch [INACTIVE]
- batch_on: Indicates whether batching is enabled or not
- packet_types: Comprised of a list of "type" fields paired with "ips"
	- type: Packet type
	- ips: [List of IPs that the packet type should be sent to]
- protocol_type: Type of logging protocol being run [INACTIVE]
- sequencer_type: Integer [0, 1] indicating the type of sequencer the client is communicating with
- storage_type: Integer [0, 3) indicating the type of storage server the client is communicating with 
- trace_file: A file containing a list of predetermined operations and payloads
