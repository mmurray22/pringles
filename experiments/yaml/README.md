Here are all the required fields for the yaml file:

- loglevel: This speciies how many log statements should be printed
- storage_ip_list: This is the list of IPs which act as storage servers
- threads: number of threads for sending
- socket: type of socket which should be created
    Can put either "UDP" or "RAW"

For Corfu, you must also include:
- seq_ip: This is the IP of the sequencing machine

Other optional fields:
- trace_file: A file containing a list of predetermined operations
