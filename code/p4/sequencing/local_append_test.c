#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <net/ethernet.h>
//#include <netpacket/packet.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <time.h>
#include <pthread.h>
#include <linux/if_packet.h>

#ifndef PACKET_IGNORE_OUTGOING
#define PACKET_IGNORE_OUTGOING 23
#endif
#define TYPE_APPEND 0x0860

// Custom Append Header - Must be packed to prevent compiler padding
struct __attribute__((packed)) append_hdr {
    uint32_t cid;
    uint32_t nonce;
    uint32_t g_idx;
    uint32_t batch_size;
    uint32_t shard_id;
    uint32_t ring_view;
    uint32_t  status;
    uint32_t  cntrl_pkt_it;
};

struct __attribute__((packed)) control_pkt_hdr {
    uint32_t global_seq_no;
    uint8_t ring_view[4];
    uint8_t  pkt_id[4];
};

// Structs to hold arguments for the threads
struct sniff_args {
    char interface[IFNAMSIZ];
    uint8_t srcAddr[6];
};

struct client_args {
    char interface[IFNAMSIZ];
    uint8_t dstMac[6];
    uint8_t srcMac[6];
    char dstIp[16];
    int timeout;
};

void* sniff_thread_func(void* arg) {
    struct sniff_args *a = (struct sniff_args*)arg;
    run_tofino_sniff(a->interface, a->srcAddr);
    return NULL;
}

// Wrapper for the Client Thread
void* client_thread_func(void* arg) {
    struct client_args *a = (struct client_args*)arg;
    run_client_no_sniff(a->interface, a->dstMac, a->srcMac, a->dstIp, a->timeout);
    return NULL;
}

void run_tofino_sniff(const char *interface, const uint8_t *tofinoSrcAddr) {
    int sock = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    if (sock < 0) {
        perror("Socket creation failed!");
	return;
    }

    int ifindex = if_nametoindex(interface);
    if (ifindex == 0) {
        perror("Interface not found");
        close(sock);
        return;
    }

    // 3. Bind the socket to the specific interface
    struct sockaddr_ll sa;
    memset(&sa, 0, sizeof(sa));
    sa.sll_family = AF_PACKET;
    sa.sll_ifindex = ifindex;
    sa.sll_protocol = htons(ETH_P_ALL);

    if (bind(sock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("Bind to interface failed");
        close(sock);
        return;
    }

    printf("[*] L2 Socket bound to %s (index %d). Listening...\n", interface, ifindex);

    struct timeval timeout = {5, 0}; // 2 seconds
    uint8_t buffer[2048];
    uint32_t num_packets = 0;
    uint32_t g_idx = 0;
    uint32_t status = 0;
    uint32_t nonce = 0;

    printf("[*] L2 Socket Open and Listening...\n");

    while (1) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(sock, &read_fds);

        int ready = select(sock + 1, &read_fds, NULL, NULL, &timeout);
        if (ready > 0) {
	    ssize_t len = 0;
            while ((len = recv(sock, buffer, sizeof(buffer), 0)) > 0) {
                struct ether_header *eth = (struct ether_header *)buffer;

                // Filter by Source MAC
                if (ntohs(eth->ether_type) == TYPE_APPEND) { //memcmp(eth->ether_shost, tofinoSrcAddr, 6) == 0) {
                    // Check if EtherType matches APPEND or if it's after an IP header
                    //if (ntohs(eth->ether_type) == TYPE_APPEND) {
	            //printf("Append packet found!");
                    struct append_hdr *app = (struct append_hdr *)(buffer + sizeof(struct ether_header) + sizeof(struct iphdr));
                    status = ntohl(app->status); // Remember to handle network byte order
                    num_packets+=1; // TODO: ???
	            if (status >= 3) {
                        g_idx = ntohl(app->g_idx); // Remember to handle network byte order
                        nonce = ntohl(app->nonce); // Remember to handle network byte order
                	    printf("Program timed out! Total packets: %u, Highest g_idx: %u, Status: %u, Nonce: %u\n", num_packets, g_idx, status, nonce);
                    }
                }
	    }
        } else {

            break;
        }
    }
    close(sock);
}

void run_client_no_sniff(const char *interface, uint8_t *dstMac, uint8_t *srcMac, const char *dstIp, int send_timeout) {
    int sock = socket(AF_PACKET, SOCK_RAW, IPPROTO_RAW);
    const char *payload = "Hello world!";
    int total_len = sizeof(struct ether_header) + sizeof(struct iphdr) + sizeof(struct append_hdr) + strlen(payload);

    // Bind to interface
    struct sockaddr_ll sa;
    memset(&sa, 0, sizeof(sa));
    sa.sll_ifindex = if_nametoindex(interface);
    sa.sll_family = AF_PACKET;

    // Buffer construction
    uint8_t pkt_buffer[total_len];
    memset(pkt_buffer, 0, sizeof(pkt_buffer));

    struct ether_header *eth = (struct ether_header *)pkt_buffer;
    memcpy(eth->ether_dhost, dstMac, 6);
    memcpy(eth->ether_shost, srcMac, 6);
    eth->ether_type = htons(TYPE_APPEND);

    struct iphdr *ip = (struct iphdr *)(pkt_buffer + sizeof(struct ether_header));
    ip->version = 4;
    ip->ihl = 5;
    ip->ttl = 64;
    ip->protocol = 0x99; // Custom protocol
    ip->daddr = inet_addr(dstIp);

    struct append_hdr *app = (struct append_hdr *)(pkt_buffer + sizeof(struct ether_header) + sizeof(struct iphdr));
    uint32_t nonce = 1;
    app->cid = htonl(0);
    app->nonce = htonl(nonce);
    app->g_idx = htonl(0);
    app->batch_size = htonl(0);
    app->shard_id = htonl(0);
    app->ring_view = htonl(0);
    app->status = htonl(1);
    app->cntrl_pkt_it = htonl(1);
    
    memcpy((uint8_t*)app + sizeof(struct append_hdr), payload, strlen(payload));

    // Start sending packets
    time_t end_time = time(NULL) + send_timeout;


    while (time(NULL) < end_time) {
        //printf("Nonce: %u\n", ntohl(app->nonce));
        sendto(sock, pkt_buffer, total_len, 0, (struct sockaddr *)&sa, sizeof(sa));
	nonce += 1;
        app->nonce = htonl(nonce);
        //printf("Nonce: %u\n", ntohl(app->nonce));
        //sendto(sock, pkt_buffer, total_len, 0, (struct sockaddr *)&sa, sizeof(sa));
	//nonce += 1;
        //app->nonce = htonl(nonce);
        //printf("Nonce: %u\n", ntohl(app->nonce));
        //sendto(sock, pkt_buffer, total_len, 0, (struct sockaddr *)&sa, sizeof(sa));
	//nonce += 1;
        //app->nonce = htonl(nonce);
        //printf("Nonce: %u\n", ntohl(app->nonce));
	//break;
    }
    printf("Nonce: %u\n", nonce);
    close(sock);
}

int main() {
    pthread_t sniff_tid, client_tid;

    // 1. Setup Sniff Arguments
    struct sniff_args s_args = {
        .interface = "enp5s0", // Change to your interface
        .srcAddr = {0x11, 0x11, 0x11, 0x11, 0x11, 0x11} 
    };

    // 2. Setup Client Arguments
    struct client_args c_args = {
        .interface = "enp5s0",
        .dstMac = {0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
        .srcMac = {0x22, 0x22, 0x22, 0x22, 0x22, 0x22},
        .dstIp = "100.99.98.97",
        .timeout = 2
    };

    printf("[MAIN] Starting Sniff Thread...\n");
    if (pthread_create(&sniff_tid, NULL, sniff_thread_func, &s_args) != 0) {
        perror("Failed to create sniff thread");
        return 1;
    }

    // 3. Wait for 2 seconds as requested
    printf("[MAIN] Waiting 2 seconds for sniffer to stabilize...\n");
    sleep(2);

    printf("[MAIN] Starting Client Thread...\n");
    if (pthread_create(&client_tid, NULL, client_thread_func, &c_args) != 0) {
        perror("Failed to create client thread");
        return 1;
    }

    // 4. Join threads (Wait for them to finish)
    pthread_join(sniff_tid, NULL);
    pthread_join(client_tid, NULL);

    printf("[MAIN] All threads finished. Exiting.\n");
    return 0;
}
