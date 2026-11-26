/*
 * rw_seq.c — DPDK software ring member for Ringworld.
 *
 * WHAT THIS IS
 *   A software version of ONE logical Ringworld ring member, i.e. what one
 *   Tofino pipe does in 64_bit_test.p4. It mirrors the experiment you run on
 *   the switch: exp_type = 2 (appends generated inside the sequencer, storage
 *   emulated), ack_threshold = 0.
 *
 *   Run one process per ring member. Two members (two machines) = the same
 *   ring you have today with pipe 0 and pipe 1.
 *
 * THE PROTOCOL IN ONE PARAGRAPH
 *   A single control packet (the "token") circulates around the ring carrying
 *   the global sequence number G = the last GSN handed out anywhere. Appends
 *   that arrive at a member between token visits form a "batch". When the
 *   token arrives with G, the member gives its batch of c appends the GSNs
 *   G+1 .. G+c and forwards the token with G+c. A stamped append is not yet
 *   safe to acknowledge: it must wait until the token has gone all the way
 *   around the ring once more, which guarantees every lower GSN has been
 *   handed out by someone. At that point (next token visit, GSN <= G) it is
 *   acked, the replicated tail advances, and its latency is recorded.
 *
 * HOW THE P4 MECHANISMS MAP TO THIS FILE
 *   P4 (Tofino)                              This file
 *   ---------------------------------------  ---------------------------------
 *   local_seq_no register (batch counter)    n_admitted - seq_head
 *   map_from_{upper,lower}_gsn[iteration]    stamping done at token arrival,
 *                                            so no per-iteration map needed
 *   recirculate until token passes           pending batch in the record ring
 *   recirculate through wait ports until     FIFO of stamped batches (bfifo)
 *     GSN <= map[latest complete iteration]
 *   ack_array + highest_*_replicated         release loop in handle_token()
 *   latency_lower/higher registers           latency histogram + sums
 *   upper/lower 32-bit GSN halves            one uint64_t (wire format stays
 *                                            split; see ringworld_hdrs.h)
 *   Tofino packet generator (timer)          internal generator, --gen-rate
 *   seq_admission meter                      not modeled; record ring full =
 *                                            drop (counted in "drops")
 *
 *   The GSNs assigned are identical to the switch's. One timing difference:
 *   the switch stamps an append on its next recirculation pass after the
 *   token goes by; here the whole batch is stamped at the moment the token
 *   arrives.
 *
 * RELATION TO DPDK's basicfwd (examples/skeleton/basicfwd.c)
 *   Only the plumbing comes from basicfwd: EAL init, mbuf pool creation, and
 *   port setup (port_init below is basicfwd's port_init, reduced to one port
 *   with one RX and one TX queue). basicfwd forwards between port pairs
 *   (0<->1); here we receive and transmit on the same single port, because a
 *   CloudLab node has one experiment port. Everything protocol-related is new.
 *
 * CODE MAP (top to bottom)
 *   config ............ command-line options (struct cfg)
 *   state ............. the record ring, the batch FIFO, statistics
 *   helpers ........... latency histogram, admit()
 *   handle_token() .... THE PROTOCOL: release, stamp, compute G_out
 *   token TX .......... forward_token(), build_token()
 *   process_rx() ...... parse a received packet: token / append / other
 *   stats printing
 *   port_init() ....... DPDK port setup (from basicfwd)
 *   arguments
 *   main() ............ setup, then the polling loop
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdbool.h>

#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>

#include "ringworld_hdrs.h"

/* DPDK sizing (same values as basicfwd). */
#define RX_RING_SIZE   1024        /* NIC receive descriptors */
#define TX_RING_SIZE   1024        /* NIC transmit descriptors */
#define NUM_MBUFS      8191        /* packet buffers in the pool */
#define MBUF_CACHE     250         /* per-core buffer cache */
#define BURST_SIZE     32          /* max packets fetched per rx_burst call */

#define GEN_BURST_MAX  64          /* max appends the generator creates per loop pass */
#define BATCH_FIFO_SZ  (1u << 16)  /* capacity of the stamped-batch FIFO (power of 2) */

/* Latency histogram: 10 ns buckets below 1 ms, 1 us buckets from 1 ms to
 * 100 ms, one overflow bucket above that. Used for percentiles. */
#define HIST_FINE_BUCKETS   100000
#define HIST_COARSE_BUCKETS 99000
#define HIST_BUCKETS        (HIST_FINE_BUCKETS + HIST_COARSE_BUCKETS)

/* =====================================================================
 * CONFIG — filled from the command line (see usage() for descriptions)
 * ===================================================================== */
struct cfg {
    uint16_t port;            /* DPDK port id to use */
    uint8_t  switch_id;       /* logical switch id of this member (for logs/trace) */
    struct rte_ether_addr my_mac;    /* source MAC on forwarded tokens */
    struct rte_ether_addr next_mac;  /* destination MAC: the next ring member */
    bool     have_my_mac, have_next_mac;
    bool     inject;          /* this member creates the first token */
    bool     self_ring;       /* 1-member ring: token never leaves the process */
    double   inject_delay_s;  /* wait this long before injecting */
    uint64_t seed;            /* initial GSN carried by the injected token */
    uint16_t view;            /* ring_view field of the injected token */
    double   gen_rate;        /* internal generator, appends/s; 0 = off */
    double   duration_s;      /* run time; 0 = until Ctrl-C */
    double   warmup_s;        /* initial period excluded from statistics */
    double   stats_interval_s;/* how often to print per-interval stats */
    double   idle_exit_s;     /* exit after this long with no RX (pcap tests); 0 = off */
    uint32_t rec_cap_log2;    /* record ring capacity = 2^this appends */
    const char *trace_path;   /* if set, write a per-event trace for check_trace.py */
};

static struct cfg C = {
    .port = 0, .switch_id = 1, .inject = false, .self_ring = false,
    .inject_delay_s = 1.0, .seed = 0, .view = 1, .gen_rate = 0,
    .duration_s = 0, .warmup_s = 0, .stats_interval_s = 1.0,
    .idle_exit_s = 0, .rec_cap_log2 = 22, .trace_path = NULL,
};

/* =====================================================================
 * STATE
 *
 * The record ring
 * ---------------
 * Every admitted append gets a record index 0, 1, 2, ... in arrival order
 * and one slot in rec_tsc[] holding its admission timestamp (TSC cycles).
 * rec_tsc is a ring buffer: slot = index & rec_mask.
 *
 * Three monotonically increasing counters split the records into regions:
 *
 *      ack_head            seq_head             n_admitted
 *         |                   |                     |
 *   ...acked... | stamped, waiting for ack | pending (not yet stamped) |
 *
 *   [seq_head, n_admitted)  = the current batch: admitted since the last token
 *                             visit; gets stamped at the next visit.
 *   [ack_head, seq_head)    = stamped with a GSN, waiting for the token to go
 *                             around the ring once more.
 *   below ack_head          = acked; their slots can be reused.
 *
 * The batch FIFO
 * --------------
 * Within one batch the GSNs are contiguous (G+1 .. G+c), so instead of
 * storing a GSN per append we store one entry per stamped batch: the GSN and
 * record index of its first not-yet-acked append, and how many are left.
 * Batches are acked in FIFO order because GSNs only increase.
 * ===================================================================== */
struct batch {
    uint64_t gsn_next;   /* GSN of the first not-yet-acked append in the batch */
    uint64_t idx_next;   /* record index of that append */
    uint64_t n_left;     /* appends in this batch not yet acked */
};

static volatile bool force_quit;     /* set by Ctrl-C / SIGTERM, or on token loss */
static bool token_lost;              /* token could not be sent: ring is broken */

static uint64_t *rec_tsc;            /* admission timestamp per append (ring buffer) */
static uint64_t  rec_mask;           /* ring size - 1 */
static uint64_t  n_admitted;         /* total appends admitted so far */
static uint64_t  seq_head;           /* first record not yet stamped */
static uint64_t  ack_head;           /* first record not yet acked */

static struct batch bfifo[BATCH_FIFO_SZ];
static uint32_t bf_head;             /* oldest stamped batch (next to be acked) */
static uint32_t bf_tail;             /* where the next stamped batch goes */

static uint64_t tail_gsn;            /* highest acked ("replicated") GSN */
static bool     have_tail;           /* false until the first ack */

/* ---- Statistics: whole-run totals ---- */
static uint64_t st_tokens;           /* token visits handled */
static uint64_t st_acks;             /* appends acked */
static uint64_t st_drops_full;       /* appends dropped: record ring full */
static uint64_t st_rx_other;         /* received packets that were neither token nor append */
static uint64_t st_bad_canary;       /* tokens/appends rejected for a wrong canary byte */
static uint64_t st_tx_fail;          /* token transmissions that failed (ring stalls!) */
static uint64_t lat_sum_total;       /* sum of measured latencies, ns */
static uint64_t lat_n_total;         /* number of measured latencies */
static uint64_t lat_max_total;       /* worst measured latency, ns */
static uint64_t hist_total[HIST_BUCKETS + 1];

/* ---- Statistics: current print interval (reset after each print) ---- */
static uint64_t st_tokens_win;
static uint64_t st_acks_win;
static uint64_t st_lat_sum_win;
static uint64_t st_rot_sum_win;      /* sum of times between token visits, cycles */
static uint64_t st_rot_n_win;
static uint64_t hist_win[HIST_BUCKETS + 1];

static uint64_t last_token_tsc;      /* when the token last visited (for rotation time) */
static double   tsc_per_ns;          /* CPU timestamp counter ticks per nanosecond */
static bool     measuring;           /* true once warmup is over */

static FILE *trace_fp;               /* per-event trace output, or NULL */

static void on_signal(int sig) { (void)sig; force_quit = true; }

/* =====================================================================
 * HELPERS
 * ===================================================================== */
static inline uint64_t tsc_to_ns(uint64_t t) { return (uint64_t)((double)t / tsc_per_ns); }

/* Map a latency in ns to its histogram bucket. */
static inline uint64_t hist_bucket(uint64_t ns)
{
    if (ns < 1000000) return ns / 10;                         /* 10 ns buckets */
    uint64_t b = HIST_FINE_BUCKETS + (ns - 1000000) / 1000;   /* 1 us buckets  */
    return b > HIST_BUCKETS ? HIST_BUCKETS : b;               /* overflow      */
}

/* Lower edge of a bucket in ns (inverse of hist_bucket). */
static inline uint64_t hist_value(uint64_t b)
{
    if (b < HIST_FINE_BUCKETS) return b * 10;
    return 1000000 + (b - HIST_FINE_BUCKETS) * 1000;   /* overflow reads as 100 ms */
}

static inline void hist_add(uint64_t ns)
{
    uint64_t b = hist_bucket(ns);
    hist_win[b]++;
    hist_total[b]++;
}

/* p-th percentile (0..1) of a histogram holding n samples. */
static uint64_t hist_pct(const uint64_t *h, uint64_t n, double p)
{
    if (n == 0) return 0;
    uint64_t target = (uint64_t)(p * (double)n);
    if (target >= n) target = n - 1;
    uint64_t acc = 0;
    for (uint32_t b = 0; b <= HIST_BUCKETS; b++) {
        acc += h[b];
        if (acc > target) return hist_value(b);
    }
    return hist_value(HIST_BUCKETS);
}

/*
 * Admit one append into the current (pending) batch: record its admission
 * time. This is the equivalent of APPEND_PRE -> APPEND on the switch, where
 * write_local_seq_no gives the packet its local index in the batch.
 *
 * If the record ring is full (too many appends waiting), drop the append.
 * That only happens under overload; it plays the role of the switch's meter.
 */
static inline bool admit(uint64_t now)
{
    if (n_admitted - ack_head > rec_mask) {
        st_drops_full++;
        return false;
    }
    rec_tsc[n_admitted & rec_mask] = now;
    n_admitted++;
    return true;
}

/* =====================================================================
 * THE PROTOCOL — called once per token visit.
 *
 *   g_in : GSN carried by the arriving token
 *   now  : arrival time (TSC)
 *   returns g_out, the GSN the token leaves with
 * ===================================================================== */
static uint64_t handle_token(uint64_t g_in, uint64_t now)
{
    /*
     * Step 1: RELEASE (ack) every stamped append whose GSN <= g_in.
     *
     * The token carries the highest GSN handed out anywhere in the ring. If
     * an append's GSN is <= g_in, the token has travelled the full ring since
     * that append was stamped, so every lower GSN is also assigned: the order
     * is final and the append can be acknowledged. This is the switch's
     * "GSN <= map[latest complete iteration]" check (compare_*_gsn).
     *
     * We walk the batch FIFO from the oldest batch. Since GSNs increase
     * through the FIFO, we stop at the first append that isn't covered yet.
     */
    while (bf_head != bf_tail) {
        struct batch *b = &bfifo[bf_head & (BATCH_FIFO_SZ - 1)];

        while (b->n_left > 0 && b->gsn_next <= g_in) {
            /* Latency = admission -> ack-ready, same points the switch measures. */
            uint64_t lat = now - rec_tsc[b->idx_next & rec_mask];

            if (measuring) {
                uint64_t ns = tsc_to_ns(lat);
                hist_add(ns);
                st_acks_win++;
                st_lat_sum_win += ns;
                lat_sum_total += ns;
                lat_n_total++;
                if (ns > lat_max_total) lat_max_total = ns;
            }
            if (trace_fp)
                fprintf(trace_fp, "A,%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
                        C.switch_id, b->gsn_next, b->idx_next, tsc_to_ns(lat));

            /* ack_threshold = 0: the first time it is ready, it is acked. */
            tail_gsn = b->gsn_next;     /* replicated tail moves forward */
            have_tail = true;
            st_acks++;
            ack_head++;                 /* its record slot can now be reused */

            b->gsn_next++;
            b->idx_next++;
            b->n_left--;
        }

        if (b->n_left == 0)
            bf_head++;                  /* batch fully acked: drop it, check the next */
        else
            break;                      /* rest of this batch isn't covered yet */
    }

    /*
     * Step 2: STAMP the pending batch.
     *
     * The c appends admitted since the last visit get GSNs g_in+1 .. g_in+c.
     * Their record indices are seq_head .. n_admitted-1, in the same order.
     * On the switch this is: read_local_seq_no (batch size, reset to 0), store
     * map[iteration] = G, and each append later computes G + local_seq_cntr.
     */
    uint64_t c = n_admitted - seq_head;
    if (c > 0) {
        if (bf_tail - bf_head >= BATCH_FIFO_SZ) {
            /* Can't happen in practice: only a batch or two is ever outstanding. */
            fprintf(stderr, "FATAL: batch FIFO overflow\n");
            force_quit = true;
            return g_in;
        }
        struct batch *nb = &bfifo[bf_tail & (BATCH_FIFO_SZ - 1)];
        nb->gsn_next = g_in + 1;
        nb->idx_next = seq_head;
        nb->n_left   = c;
        bf_tail++;
        seq_head = n_admitted;          /* new appends start a new pending batch */
    }

    /* Step 3: the token leaves carrying the new highest GSN (switch egress:
     * lower_global_seq_no += temp_local_seq_no, with carry into the upper half). */
    uint64_t g_out = g_in + c;

    if (trace_fp)
        fprintf(trace_fp, "T,%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
                C.switch_id, g_in, c, g_out);

    /* Statistics: token visits, and rotation time = time since the last visit. */
    st_tokens++;
    if (measuring) {
        st_tokens_win++;
        if (last_token_tsc) {
            st_rot_sum_win += now - last_token_tsc;
            st_rot_n_win++;
        }
    }
    last_token_tsc = now;
    return g_out;
}

/* =====================================================================
 * TOKEN TRANSMIT
 * ===================================================================== */

/*
 * Rewrite a token packet in place and send it to the next ring member.
 * We reuse the received mbuf, so only the GSN and Ethernet addresses change.
 */
static void forward_token(struct rte_mbuf *m, uint64_t g_out)
{
    struct rte_ether_hdr *eth = rte_pktmbuf_mtod(m, struct rte_ether_hdr *);
    struct rw_control *ctl = (struct rw_control *)(eth + 1);

    rw_gsn_set(&ctl->upper_global_seq_no, &ctl->lower_global_seq_no, g_out);
    ctl->temp_local_seq_no = 0;                       /* already folded into g_out */
    rte_ether_addr_copy(&C.next_mac, &eth->dst_addr); /* L2 switch delivers by dst MAC */
    rte_ether_addr_copy(&C.my_mac, &eth->src_addr);

    /*
     * There is exactly one token. rte_eth_tx_burst returns the number of
     * packets the driver accepted: 0 means the NIC never got the packet and we
     * still own the mbuf, so retrying cannot create a duplicate token.
     * (A return of 1 means "accepted for transmission", not "delivered"; loss
     * after that point is not visible here.)
     *
     * We send the token immediately rather than batching TX, because every
     * nanosecond the token spends here adds to every append's latency.
     */
    for (int tries = 0; tries < 1000; tries++) {
        if (rte_eth_tx_burst(C.port, 0, &m, 1) == 1)
            return;
    }

    /*
     * Could not send the token: the ring has lost it. On the Tofino this is
     * where a timeout would trigger a view change. This software member does
     * not implement view change, so fail loudly and stop: a run with a lost
     * token is invalid and must be repeated.
     */
    st_tx_fail++;
    rte_pktmbuf_free(m);
    token_lost = true;
    force_quit = true;
    fprintf(stderr,
            "\n"
            "*****************************************************************\n"
            "*** FATAL: TOKEN LOST — member %u could not transmit the token\n"
            "*** (GSN %" PRIu64 ") after 1000 attempts. The ring is broken;\n"
            "*** on the switch this would induce a view change. Exiting.\n"
            "*** THIS RUN IS INVALID.\n"
            "*****************************************************************\n",
            C.switch_id, g_out);
}

/*
 * Build a brand-new token (used once, by the --inject member). Layout:
 *   Ethernet (type 0x0820) | rw_control | canary (0xA5) | zero padding to 60 B
 * Same packet as send_cntrl_packet() in test.py.
 */
static struct rte_mbuf *build_token(struct rte_mempool *mp, uint64_t gsn)
{
    struct rte_mbuf *m = rte_pktmbuf_alloc(mp);
    if (!m) return NULL;

    const uint16_t len = RTE_ETHER_MIN_LEN - RTE_ETHER_CRC_LEN;   /* 60 bytes */
    char *p = rte_pktmbuf_append(m, len);
    memset(p, 0, len);

    struct rte_ether_hdr *eth = (struct rte_ether_hdr *)p;
    rte_ether_addr_copy(&C.next_mac, &eth->dst_addr);
    rte_ether_addr_copy(&C.my_mac, &eth->src_addr);
    eth->ether_type = htons(RW_ETH_CONTROL);

    struct rw_control *ctl = (struct rw_control *)(eth + 1);
    rw_gsn_set(&ctl->upper_global_seq_no, &ctl->lower_global_seq_no, gsn);
    ctl->temp_local_seq_no = 0;
    ctl->ring_view = htons(C.view);

    struct rw_canary *can = (struct rw_canary *)(ctl + 1);
    can->magic = RW_CANARY_VALUE;
    return m;
}

/* =====================================================================
 * RECEIVE PATH — classify and handle one received packet.
 *
 * Returns true if the mbuf was consumed (the token is re-sent, so it must not
 * be freed); false means the caller frees it.
 * ===================================================================== */
static bool process_rx(struct rte_mbuf *m, uint64_t now)
{
    uint32_t len = rte_pktmbuf_data_len(m);
    uint8_t *p = rte_pktmbuf_mtod(m, uint8_t *);

    if (len < sizeof(struct rte_ether_hdr)) {
        st_rx_other++;
        return false;
    }
    struct rte_ether_hdr *eth = (struct rte_ether_hdr *)p;
    uint16_t et = ntohs(eth->ether_type);

    /* ---- Case 1: the token (control packet, EtherType 0x0820) ---- */
    if (et == RW_ETH_CONTROL) {
        uint32_t need = sizeof(*eth) + sizeof(struct rw_control) + sizeof(struct rw_canary);
        if (len < need) {
            st_rx_other++;
            return false;
        }
        struct rw_control *ctl = (struct rw_control *)(eth + 1);
        struct rw_canary *can = (struct rw_canary *)(ctl + 1);

        /* Same check as the switch egress: wrong canary byte -> drop. */
        if (can->magic != RW_CANARY_VALUE) {
            st_bad_canary++;
            return false;
        }

        /* Incoming GSN. If a sender left a count in temp_local_seq_no, fold it
         * in, exactly like the switch egress does before forwarding. */
        uint64_t g_in = rw_gsn_get(ctl->upper_global_seq_no, ctl->lower_global_seq_no)
                        + ntohl(ctl->temp_local_seq_no);

        uint64_t g_out = handle_token(g_in, now);
        forward_token(m, g_out);
        return true;
    }

    /* ---- Case 2: an append from a client (IPv4 / UDP / ring_type APPEND_PRE) ---- */
    if (et == RW_ETH_IPV4) {
        if (len < sizeof(*eth) + sizeof(struct rte_ipv4_hdr)) {
            st_rx_other++;
            return false;
        }
        struct rte_ipv4_hdr *ip = (struct rte_ipv4_hdr *)(eth + 1);
        uint32_t ihl = (ip->version_ihl & 0x0f) * 4;          /* IPv4 header length */
        if (ip->next_proto_id != IPPROTO_UDP || ihl < 20) {
            st_rx_other++;
            return false;
        }

        /* Offset of ring_type = Ethernet + IPv4 + UDP. */
        uint32_t off = sizeof(*eth) + ihl + sizeof(struct rte_udp_hdr);
        if (len < off + sizeof(struct rw_ring_type)) {
            st_rx_other++;
            return false;
        }
        struct rw_ring_type *rt = (struct rw_ring_type *)(p + off);

        if (ntohs(rt->type) == RW_TYPE_APPEND_PRE) {
            uint32_t need = off + sizeof(*rt) + sizeof(struct rw_append) + sizeof(struct rw_canary);
            if (len < need) {
                st_rx_other++;
                return false;
            }
            struct rw_canary *can =
                (struct rw_canary *)(p + off + sizeof(*rt) + sizeof(struct rw_append));
            if (can->magic != RW_CANARY_VALUE) {
                st_bad_canary++;
                return false;
            }
            admit(now);
            return false;   /* exp_type 2 never sends the append on, so free it */
        }
    }

    /* ---- Case 3: anything else is ignored ---- */
    st_rx_other++;
    return false;
}

/* =====================================================================
 * STATISTICS OUTPUT
 * ===================================================================== */

/* One line per stats interval. "pending"/"waiting" are the two record-ring
 * regions; if they keep growing, this member can't keep up with the load. */
static void print_window(double secs)
{
    uint64_t n = st_acks_win;
    double avg_ns = n ? (double)st_lat_sum_win / (double)n : 0;
    double rot_us = st_rot_n_win ? tsc_to_ns(st_rot_sum_win / st_rot_n_win) / 1000.0 : 0;

    printf("[stats] tokens/s=%.0f  acks/s=%.3fM  lat avg=%.0fns p50=%" PRIu64 "ns p99=%" PRIu64
           "ns  rotation=%.2fus  pending=%" PRIu64 " waiting=%" PRIu64 " drops=%" PRIu64 "\n",
           st_tokens_win / secs, n / secs / 1e6, avg_ns,
           hist_pct(hist_win, n, 0.50), hist_pct(hist_win, n, 0.99), rot_us,
           n_admitted - seq_head, seq_head - ack_head, st_drops_full);
    fflush(stdout);

    st_tokens_win = st_acks_win = st_lat_sum_win = st_rot_sum_win = st_rot_n_win = 0;
    memset(hist_win, 0, sizeof(hist_win));
}

/* Whole-run summary (after warmup). */
static void print_final(double measured_s)
{
    printf("\n===== final (member %u) =====\n", C.switch_id);
    if (token_lost)
        printf("*** TOKEN LOST: run aborted, results below are INVALID ***\n");
    printf("tokens=%" PRIu64 " admitted=%" PRIu64 " acked=%" PRIu64 " drops_full=%" PRIu64
           " bad_canary=%" PRIu64 " rx_other=%" PRIu64 " tx_fail=%" PRIu64 "\n",
           st_tokens, n_admitted, st_acks, st_drops_full, st_bad_canary, st_rx_other, st_tx_fail);
    if (have_tail)
        printf("replicated tail GSN=%" PRIu64 "\n", tail_gsn);
    if (lat_n_total) {
        printf("measured %.1fs: throughput=%.3f M acks/s\n", measured_s,
               measured_s > 0 ? lat_n_total / measured_s / 1e6 : 0);
        printf("latency ns: avg=%.0f p50=%" PRIu64 " p90=%" PRIu64 " p99=%" PRIu64
               " p99.9=%" PRIu64 " max=%" PRIu64 "\n",
               (double)lat_sum_total / lat_n_total,
               hist_pct(hist_total, lat_n_total, 0.50), hist_pct(hist_total, lat_n_total, 0.90),
               hist_pct(hist_total, lat_n_total, 0.99), hist_pct(hist_total, lat_n_total, 0.999),
               lat_max_total);
    }
    fflush(stdout);
}

/* =====================================================================
 * PORT SETUP — basicfwd's port_init, reduced to 1 RX + 1 TX queue.
 * ===================================================================== */
static int port_init(uint16_t port, struct rte_mempool *mp)
{
    struct rte_eth_conf conf;
    memset(&conf, 0, sizeof(conf));            /* default config, no offloads */
    uint16_t nb_rxd = RX_RING_SIZE, nb_txd = TX_RING_SIZE;
    struct rte_eth_dev_info info;
    int r;

    if (!rte_eth_dev_is_valid_port(port)) return -1;
    if ((r = rte_eth_dev_info_get(port, &info)) != 0) return r;

    /* 1 RX queue, 1 TX queue. */
    if ((r = rte_eth_dev_configure(port, 1, 1, &conf)) != 0) return r;

    /* Let the driver round descriptor counts to what the NIC supports. */
    if ((r = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd)) != 0) return r;

    /* Received packets land in buffers from our mempool, on the NIC's NUMA node. */
    if ((r = rte_eth_rx_queue_setup(port, 0, nb_rxd, rte_eth_dev_socket_id(port), NULL, mp)) < 0)
        return r;

    struct rte_eth_txconf txconf = info.default_txconf;
    txconf.offloads = conf.txmode.offloads;
    if ((r = rte_eth_tx_queue_setup(port, 0, nb_txd, rte_eth_dev_socket_id(port), &txconf)) < 0)
        return r;

    if ((r = rte_eth_dev_start(port)) < 0) return r;

    /* Accept frames for any MAC. Best effort: some virtual devices don't support it. */
    rte_eth_promiscuous_enable(port);
    return 0;
}

/* =====================================================================
 * ARGUMENTS
 * ===================================================================== */
static void usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s [EAL args] -- [options]\n"
        "  --id N             logical switch id of this member (default 1)\n"
        "  --port N           DPDK port id (default 0)\n"
        "  --my-mac MAC       source MAC for tokens (default: port MAC)\n"
        "  --next-mac MAC     MAC of the next ring member (required unless --self-ring)\n"
        "  --inject           this member creates the first token\n"
        "  --inject-delay S   seconds to wait before injecting (default 1)\n"
        "  --seed N           initial GSN of the injected token (default 0)\n"
        "  --view N           ring view in the injected token (default 1)\n"
        "  --self-ring        1-member ring: token stays in-process (testing)\n"
        "  --gen-rate R       internal append generator, appends/s (default 0 = off;\n"
        "                     starts after the first token is seen, like the Tofino)\n"
        "  --duration S       run time in seconds (default: until Ctrl-C)\n"
        "  --warmup S         seconds excluded from stats (default 0)\n"
        "  --stats S          stats print interval (default 1)\n"
        "  --idle-exit S      exit after S seconds without RX (pcap tests)\n"
        "  --rec-cap-log2 N   record ring capacity 2^N (default 22)\n"
        "  --trace FILE       write per-token/per-ack trace (slow; tests only)\n", prog);
}

static int parse_args(int argc, char **argv)
{
    static struct option opts[] = {
        {"id",           required_argument, 0, 'i'},
        {"port",         required_argument, 0, 'p'},
        {"my-mac",       required_argument, 0, 'm'},
        {"next-mac",     required_argument, 0, 'n'},
        {"inject",       no_argument,       0, 'j'},
        {"inject-delay", required_argument, 0, 'J'},
        {"seed",         required_argument, 0, 's'},
        {"view",         required_argument, 0, 'v'},
        {"self-ring",    no_argument,       0, 'r'},
        {"gen-rate",     required_argument, 0, 'g'},
        {"duration",     required_argument, 0, 'd'},
        {"warmup",       required_argument, 0, 'w'},
        {"stats",        required_argument, 0, 'S'},
        {"idle-exit",    required_argument, 0, 'x'},
        {"rec-cap-log2", required_argument, 0, 'c'},
        {"trace",        required_argument, 0, 't'},
        {"help",         no_argument,       0, 'h'},
        {0, 0, 0, 0}};
    int o;
    while ((o = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (o) {
        case 'i': C.switch_id = (uint8_t)atoi(optarg); break;
        case 'p': C.port = (uint16_t)atoi(optarg); break;
        case 'm':
            if (rte_ether_unformat_addr(optarg, &C.my_mac)) return -1;
            C.have_my_mac = true;
            break;
        case 'n':
            if (rte_ether_unformat_addr(optarg, &C.next_mac)) return -1;
            C.have_next_mac = true;
            break;
        case 'j': C.inject = true; break;
        case 'J': C.inject_delay_s = atof(optarg); break;
        case 's': C.seed = strtoull(optarg, NULL, 0); break;
        case 'v': C.view = (uint16_t)atoi(optarg); break;
        case 'r': C.self_ring = true; break;
        case 'g': C.gen_rate = atof(optarg); break;
        case 'd': C.duration_s = atof(optarg); break;
        case 'w': C.warmup_s = atof(optarg); break;
        case 'S': C.stats_interval_s = atof(optarg); break;
        case 'x': C.idle_exit_s = atof(optarg); break;
        case 'c': C.rec_cap_log2 = (uint32_t)atoi(optarg); break;
        case 't': C.trace_path = optarg; break;
        default:  return -1;
        }
    }
    if (!C.self_ring && !C.have_next_mac) {
        fprintf(stderr, "--next-mac is required unless --self-ring\n");
        return -1;
    }
    if (C.rec_cap_log2 < 10 || C.rec_cap_log2 > 30) return -1;
    return 0;
}

/* =====================================================================
 * MAIN
 * ===================================================================== */
int main(int argc, char **argv)
{
    /* ---------- Setup (EAL, mempool, port: as in basicfwd) ---------- */

    /* DPDK's EAL consumes its own arguments (before "--"); ours come after. */
    int ret = rte_eal_init(argc, argv);
    if (ret < 0) rte_exit(EXIT_FAILURE, "EAL init failed\n");
    argc -= ret;
    argv += ret;
    if (parse_args(argc, argv) < 0) {
        usage(argv[0]);
        rte_exit(EXIT_FAILURE, "bad arguments\n");
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    if (rte_eth_dev_count_avail() == 0) rte_exit(EXIT_FAILURE, "no ports available\n");

    struct rte_mempool *mp = rte_pktmbuf_pool_create("MBUF_POOL", NUM_MBUFS, MBUF_CACHE, 0,
                                                     RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    if (!mp) rte_exit(EXIT_FAILURE, "cannot create mbuf pool\n");
    if (port_init(C.port, mp) != 0) rte_exit(EXIT_FAILURE, "cannot init port %u\n", C.port);
    if (!C.have_my_mac) rte_eth_macaddr_get(C.port, &C.my_mac);

    /* Record ring: one timestamp per append. Ordinary memory (the NIC never touches it). */
    rec_mask = (1ull << C.rec_cap_log2) - 1;
    rec_tsc = calloc(rec_mask + 1, sizeof(uint64_t));
    if (!rec_tsc) rte_exit(EXIT_FAILURE, "cannot allocate record ring\n");

    if (C.trace_path) {
        trace_fp = fopen(C.trace_path, "w");
        if (!trace_fp) rte_exit(EXIT_FAILURE, "cannot open trace file\n");
        fprintf(trace_fp, "# T,member,g_in,count,g_out | A,member,gsn,record_idx,latency_ns\n");
    }

    /* All timing uses the CPU timestamp counter (TSC): cheap and high resolution. */
    const uint64_t hz = rte_get_tsc_hz();
    tsc_per_ns = (double)hz / 1e9;

    char mac_s[RTE_ETHER_ADDR_FMT_SIZE], nmac_s[RTE_ETHER_ADDR_FMT_SIZE] = "(self)";
    rte_ether_format_addr(mac_s, sizeof(mac_s), &C.my_mac);
    if (C.have_next_mac) rte_ether_format_addr(nmac_s, sizeof(nmac_s), &C.next_mac);
    printf("member %u on port %u, lcore %u, my_mac=%s next=%s, tsc=%.3f GHz\n",
           C.switch_id, C.port, rte_lcore_id(), mac_s, nmac_s, hz / 1e9);

    /* A core on the wrong NUMA node adds latency to every packet. */
    if (rte_eth_dev_socket_id(C.port) >= 0 &&
        rte_eth_dev_socket_id(C.port) != (int)rte_socket_id())
        printf("WARNING: port is on NUMA node %d but this core is on node %u\n",
               rte_eth_dev_socket_id(C.port), rte_socket_id());

    /* ---------- Loop state ---------- */
    const uint64_t t_start = rte_rdtsc();
    const uint64_t t_inject = t_start + (uint64_t)(C.inject_delay_s * hz);
    const uint64_t stats_ivl = (uint64_t)(C.stats_interval_s * hz);
    uint64_t t_last_stats = 0;       /* last stats print */
    uint64_t t_last_rx = t_start;    /* last time anything was received (for --idle-exit) */
    uint64_t t_measure_start = 0;    /* end of warmup */
    uint64_t t_first_token = 0;      /* first token seen: starts generator and warmup clock */
    uint64_t gen_start = 0;          /* when the generator started */
    uint64_t gen_done = 0;           /* appends generated so far */
    bool injected = !C.inject;       /* nothing to inject unless --inject */
    bool self_token = false;         /* --self-ring: token currently held in-process */
    uint64_t self_token_g = 0;       /* --self-ring: GSN of that token */
    struct rte_mbuf *bufs[BURST_SIZE];

    /*
     * ---------- The polling loop ----------
     * One pass: maybe inject, receive and handle a burst, maybe generate
     * appends, maybe print stats. The core spins continuously (no sleeping);
     * that's standard for DPDK and what keeps latency low.
     */
    while (!force_quit) {
        uint64_t now = rte_rdtsc();

        /* (a) Inject the first token. The injector handles it locally first, as
         *     if it had just arrived (on the switch, the CPU sends it into the pipe). */
        if (!injected && now >= t_inject) {
            injected = true;
            if (C.self_ring) {
                self_token = true;
                self_token_g = C.seed;
            } else {
                struct rte_mbuf *tok = build_token(mp, C.seed);
                if (!tok) rte_exit(EXIT_FAILURE, "cannot allocate token\n");
                if (!t_first_token) t_first_token = now;
                forward_token(tok, handle_token(C.seed, now));
            }
            printf("injected token, seed GSN=%" PRIu64 "\n", C.seed);
        }

        /* (b) Receive a burst and handle each packet. */
        uint16_t nb = rte_eth_rx_burst(C.port, 0, bufs, BURST_SIZE);
        if (nb) t_last_rx = now;
        for (uint16_t i = 0; i < nb; i++) {
            /* Note the first token: the generator and warmup clock start then. */
            if (!t_first_token &&
                rte_pktmbuf_data_len(bufs[i]) >= sizeof(struct rte_ether_hdr) &&
                ntohs(rte_pktmbuf_mtod(bufs[i], struct rte_ether_hdr *)->ether_type)
                    == RW_ETH_CONTROL)
                t_first_token = now;

            if (!process_rx(bufs[i], now))
                rte_pktmbuf_free(bufs[i]);
        }

        /* (c) 1-member ring (testing only): the token comes straight back. */
        if (self_token) {
            if (!t_first_token) t_first_token = now;
            self_token_g = handle_token(self_token_g, now);
        }

        /*
         * (d) Internal generator: the Tofino packet generator equivalent.
         *     Rate-based: by time `now` we should have generated
         *     (now - gen_start) * rate appends; create the difference, at most
         *     GEN_BURST_MAX per pass so RX isn't starved. Like the switch, it
         *     only starts once the token is circulating.
         */
        if (C.gen_rate > 0 && t_first_token) {
            if (!gen_start) gen_start = now;
            uint64_t due = (uint64_t)((double)(now - gen_start) * C.gen_rate / (double)hz);
            uint64_t k = due > gen_done ? due - gen_done : 0;
            if (k > GEN_BURST_MAX) k = GEN_BURST_MAX;
            for (uint64_t j = 0; j < k; j++)
                admit(now);
            gen_done += k;
            /* If the core can't keep up with the requested rate, don't let the
             * backlog grow forever (it would burst later and distort latency). */
            if (due > gen_done + 1000000) gen_done = due - 1000000;
        }

        /* (e) Warmup ends warmup_s after the first token: start measuring. */
        if (!measuring && t_first_token &&
            now - t_first_token >= (uint64_t)(C.warmup_s * hz)) {
            measuring = true;
            t_measure_start = now;
            t_last_stats = now;
        }

        /* (f) Periodic stats line. */
        if (measuring && C.stats_interval_s > 0 && now - t_last_stats >= stats_ivl) {
            print_window((double)(now - t_last_stats) / hz);
            t_last_stats = now;
        }

        /* (g) Stop conditions. */
        if (C.duration_s > 0 && now - t_start >= (uint64_t)(C.duration_s * hz)) break;
        if (C.idle_exit_s > 0 && injected && now - t_last_rx >= (uint64_t)(C.idle_exit_s * hz)) break;
    }

    /* ---------- Shutdown ---------- */
    uint64_t t_end = rte_rdtsc();
    print_final(t_measure_start ? (double)(t_end - t_measure_start) / hz : 0);
    if (trace_fp) fclose(trace_fp);
    rte_eth_dev_stop(C.port);
    rte_eth_dev_close(C.port);   /* flushes the pcap output file in tests */
    free(rec_tsc);
    rte_eal_cleanup();

    /* Nonzero exit status so experiment scripts notice the failed run. */
    return token_lost ? 2 : 0;
}
