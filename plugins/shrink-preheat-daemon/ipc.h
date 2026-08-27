#ifndef IPC_H
#define IPC_H

#include <stddef.h>
#include <stdint.h>
#include "pool.h"

/* ── IPC message types ───────────────────────────────── */

#define MSG_HELLO       0
#define MSG_HELLO_ACK   1
#define MSG_ACQUIRE     2
#define MSG_ACQUIRE_ACK 3
#define MSG_RELEASE     4
#define MSG_RELEASE_ACK 5
#define MSG_STATUS      6
#define MSG_STATUS_ACK  7
#define MSG_ATTACH      8
#define MSG_ATTACH_ACK  9
#define MSG_LAYOUT      10
#define MSG_LAYOUT_ACK  11
#define MSG_ACQUIRE_BATCH     12
#define MSG_ACQUIRE_BATCH_ACK 13
#define MSG_ATTACH_BATCH      14
#define MSG_ATTACH_BATCH_ACK  15

/* ── Wire formats (fixed size) ───────────────────────── */

/* Client -> daemon: 24 bytes */
struct preheat_request {
    uint32_t type;
    uint32_t vm_id;
    uint32_t slot_id;
    uint32_t generation;
    uint64_t lease_id;
};

/* Daemon -> client: 48 bytes */
struct preheat_response {
    uint32_t type;
    uint32_t status;
    uint32_t slot_id;
    uint32_t generation;
    uint64_t offset;
    uint64_t slot_size;
    uint64_t lease_id;
    uint64_t ttl_ns;
};

struct preheat_batch_lease {
    uint32_t slot_id;
    uint32_t generation;
    uint64_t lease_id;
};

/*
 * Backend metadata is encoded in existing fields to keep wire size stable:
 * HELLO_ACK:
 *   generation = backend kind
 *   offset     = backend simulated (0/1)
 *   lease_id   = backend flags
 *   ttl_ns     = backend latency_ns
 * STATUS_ACK:
 *   ttl_ns low 32 bits  = total_slots
 *   ttl_ns high 32 bits = backend kind
 *   lease_id low 32 bits  = cooldown_count
 *   lease_id high 32 bits = backend simulated (0/1)
 */
static inline uint64_t ipc_status_encode_ttl(uint32_t total_slots, uint32_t backend_kind)
{
    return ((uint64_t)backend_kind << 32) | (uint64_t)total_slots;
}

static inline uint64_t ipc_status_encode_lease(uint32_t cooldown_count, uint32_t backend_simulated)
{
    return ((uint64_t)backend_simulated << 32) | (uint64_t)cooldown_count;
}

/*
 * LAYOUT_ACK:
 *   slot_id   = total_slots
 *   generation = backend kind
 *   offset    = header_size
 *   slot_size = slot_size
 *   lease_id  = map_size
 *   ttl_ns    = backing alignment
 */

static inline uint32_t ipc_status_total_slots(uint64_t ttl_ns)
{
    return (uint32_t)(ttl_ns & 0xffffffffULL);
}

static inline uint32_t ipc_status_backend_kind(uint64_t ttl_ns)
{
    return (uint32_t)(ttl_ns >> 32);
}

static inline uint32_t ipc_status_cooldown_count(uint64_t lease_id)
{
    return (uint32_t)(lease_id & 0xffffffffULL);
}

static inline uint32_t ipc_status_backend_simulated(uint64_t lease_id)
{
    return (uint32_t)(lease_id >> 32);
}

/* ── Per-client state ────────────────────────────────── */

#define MAX_CLIENT_SLOTS 64

struct client_slot_lease {
    uint32_t slot_id;
    uint32_t generation;
    uint64_t lease_id;
    uint32_t attached;
};

struct client_state {
    int      fd;
    uint32_t vm_id;
    int      memfd_sent;
    struct client_slot_lease allocated_slots[MAX_CLIENT_SLOTS];
    int      allocated_count;
};

/* ── Functions ───────────────────────────────────────── */

int  ipc_create_listen_socket(const char *sock_path);

/* Send response (plain, no fd) */
int  ipc_send_response(int client_fd, const struct preheat_response *resp);
int  ipc_send_all(int client_fd, const void *buffer, size_t length);
int  ipc_recv_all(int client_fd, void *buffer, size_t length);

/* Send HELLO_ACK with SCM_RIGHTS fd passing */
int  ipc_send_hello_ack(int client_fd, int memfd,
                        uint32_t total_slots, uint64_t slot_size,
                        const struct backend_info *backend);

/* Receive request (blocking) */
int  ipc_recv_request(int client_fd, struct preheat_request *req);

static inline void ipc_hello_set_backend(struct preheat_response *resp,
                                         const struct backend_info *backend)
{
    resp->generation = backend->kind;
    resp->offset = backend->simulated;
    resp->lease_id = backend->flags;
    resp->ttl_ns = backend->latency_ns;
}

static inline uint32_t ipc_hello_backend_kind(const struct preheat_response *resp)
{
    return resp->generation;
}

static inline uint32_t ipc_hello_backend_simulated(const struct preheat_response *resp)
{
    return (uint32_t)resp->offset;
}

static inline uint64_t ipc_hello_backend_flags(const struct preheat_response *resp)
{
    return resp->lease_id;
}

static inline uint64_t ipc_hello_backend_latency_ns(const struct preheat_response *resp)
{
    return resp->ttl_ns;
}

#endif /* IPC_H */
