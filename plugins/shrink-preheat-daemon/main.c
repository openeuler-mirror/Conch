#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <errno.h>

#include "pool.h"
#include "ipc.h"
#include "backing_store.h"

/* ── Globals ─────────────────────────────────────────── */

#define MAX_CLIENTS   256
#define MAX_EVENTS    32
#define DEFAULT_SOCK  "/tmp/shrink_preheat.sock"

static struct pool         g_pool;
static struct client_state g_clients[MAX_CLIENTS];
static int                 g_client_count = 0;
static volatile int        g_running = 1;

/* ── Signal handler ──────────────────────────────────── */

static void sig_handler(int sig)
{
    (void)sig;
    g_running = 0;
}

/* ── Client management ───────────────────────────────── */

static struct client_state *find_client(int fd)
{
    for (int i = 0; i < g_client_count; i++) {
        if (g_clients[i].fd == fd)
            return &g_clients[i];
    }
    return NULL;
}

static struct client_state *add_client(int fd)
{
    if (g_client_count >= MAX_CLIENTS) {
        fprintf(stderr, "main: max clients reached\n");
        return NULL;
    }
    struct client_state *c = &g_clients[g_client_count++];
    memset(c, 0, sizeof(*c));
    c->fd = fd;
    return c;
}

static void remove_client(struct client_state *c)
{
    close(c->fd);
    /* Shift array */
    int idx = c - g_clients;
    if (idx < g_client_count - 1) {
        memmove(&g_clients[idx], &g_clients[idx + 1],
                sizeof(struct client_state) * (g_client_count - 1 - idx));
    }
    g_client_count--;
}

static int client_find_lease(const struct client_state *c, uint32_t slot_id,
                             uint32_t generation, uint64_t lease_id)
{
    for (int i = 0; i < c->allocated_count; i++) {
        const struct client_slot_lease *lease = &c->allocated_slots[i];
        if (lease->slot_id == slot_id && lease->generation == generation &&
            lease->lease_id == lease_id)
            return i;
    }
    return -1;
}

static void client_remove_lease(struct client_state *c, int index)
{
    if (index < 0 || index >= c->allocated_count)
        return;
    c->allocated_slots[index] = c->allocated_slots[--c->allocated_count];
}

/* ── Handle disconnect ───────────────────────────────── */

static void handle_disconnect(struct client_state *c)
{
    printf("main: VM %u disconnected, reclaiming %d slots\n",
           c->vm_id, c->allocated_count);
    for (int i = 0; i < c->allocated_count; i++) {
        const struct client_slot_lease *lease = &c->allocated_slots[i];
        int rc = pool_force_reclaim(&g_pool, c->vm_id, lease->slot_id,
                                    lease->generation, lease->lease_id);
        printf("main: Force reclaim slot %u from VM %u: status=%d attached=%u\n",
               lease->slot_id, c->vm_id, rc, lease->attached);
    }
    remove_client(c);
}

/* ── Handle incoming message ─────────────────────────── */

static void handle_message(struct client_state *c)
{
    struct preheat_request req;
    if (ipc_recv_request(c->fd, &req) < 0) {
        handle_disconnect(c);
        return;
    }

    struct preheat_response resp;
    memset(&resp, 0, sizeof(resp));

    switch (req.type) {
    case MSG_HELLO:
        c->vm_id = req.vm_id;
        printf("main: HELLO from VM %u\n", c->vm_id);
        if (ipc_send_hello_ack(c->fd, g_pool.memfd,
                               g_pool.total_slots, g_pool.slot_size,
                               &g_pool.backend) < 0) {
            fprintf(stderr, "main: Failed to send HELLO_ACK to VM %u\n",
                    c->vm_id);
            handle_disconnect(c);
            return;
        }
        c->memfd_sent = 1;
        printf("main: HELLO_ACK sent to VM %u (fd passed, backend=%s)\n",
               c->vm_id, backing_store_type_name(g_pool.store_type));
        break;

    case MSG_ACQUIRE: {
        uint32_t slot_id, generation;
        uint64_t offset, lease_id, ttl_ns;
        int rc;

        if (c->allocated_count >= MAX_CLIENT_SLOTS) {
            resp.type = MSG_ACQUIRE_ACK;
            resp.status = STATUS_QUOTA_EXCEEDED;
            ipc_send_response(c->fd, &resp);
            printf("main: ACQUIRE from VM %u: CLIENT_SLOT_LIMIT allocated=%d tracking_limit=%d\n",
                   c->vm_id, c->allocated_count, MAX_CLIENT_SLOTS);
            break;
        }
        if ((uint32_t)c->allocated_count >= g_pool.max_leases_per_vm) {
            resp.type = MSG_ACQUIRE_ACK;
            resp.status = STATUS_QUOTA_EXCEEDED;
            ipc_send_response(c->fd, &resp);
            printf("main: ACQUIRE from VM %u: QUOTA_EXCEEDED allocated=%d limit=%u\n",
                   c->vm_id, c->allocated_count, g_pool.max_leases_per_vm);
            break;
        }
        rc = pool_acquire(&g_pool, c->vm_id,
                          &slot_id, &generation, &offset, &lease_id, &ttl_ns);
        resp.type = MSG_ACQUIRE_ACK;
        if (rc != STATUS_OK) {
            resp.status = rc;
            printf("main: ACQUIRE from VM %u failed status=%u free_hot=%u inflight=%u\n",
                   c->vm_id, rc, atomic_load(&g_pool.free_hot_count),
                   atomic_load(&g_pool.inflight_count));
        } else {
            resp.status     = STATUS_OK;
            resp.slot_id    = slot_id;
            resp.generation = generation;
            resp.offset     = offset;
            resp.slot_size  = g_pool.slot_size;
            resp.lease_id   = lease_id;
            resp.ttl_ns     = ttl_ns;

            /* Track in client's allocated list */
            c->allocated_slots[c->allocated_count++] = (struct client_slot_lease) {
                .slot_id = slot_id,
                .generation = generation,
                .lease_id = lease_id,
                .attached = 0,
            };
            printf("main: ACQUIRE from VM %u: slot=%u epoch=%u lease=%lu ttl_ns=%lu offset=%lu\n",
                   c->vm_id, slot_id, generation, (unsigned long)lease_id,
                   (unsigned long)ttl_ns, (unsigned long)offset);
        }
        ipc_send_response(c->fd, &resp);
        break;
    }

    case MSG_ACQUIRE_BATCH: {
        struct preheat_response leases[MAX_CLIENT_SLOTS];
        uint32_t count = req.slot_id;
        uint32_t acquired = 0;
        int rc = STATUS_OK;

        memset(leases, 0, sizeof(leases));
        resp.type = MSG_ACQUIRE_BATCH_ACK;
        if (count == 0 || count > STRATOVIRT_CXL_SLOT_COUNT ||
            c->allocated_count + (int)count > MAX_CLIENT_SLOTS ||
            (uint32_t)c->allocated_count + count > g_pool.max_leases_per_vm) {
            resp.status = STATUS_QUOTA_EXCEEDED;
            ipc_send_response(c->fd, &resp);
            break;
        }

        for (acquired = 0; acquired < count; acquired++) {
            struct preheat_response *lease = &leases[acquired];

            rc = pool_acquire(&g_pool, c->vm_id, &lease->slot_id,
                              &lease->generation, &lease->offset,
                              &lease->lease_id, &lease->ttl_ns);
            if (rc != STATUS_OK)
                break;
            lease->type = MSG_ACQUIRE_BATCH_ACK;
            lease->status = STATUS_OK;
            lease->slot_size = g_pool.slot_size;
        }
        if (rc == STATUS_OK) {
            for (uint32_t i = 1; i < count; i++) {
                if (leases[i].offset != leases[i - 1].offset + g_pool.slot_size) {
                    rc = STATUS_POOL_BUSY;
                    break;
                }
            }
        }
        if (rc != STATUS_OK) {
            for (uint32_t i = 0; i < acquired; i++)
                pool_release(&g_pool, c->vm_id, leases[i].slot_id,
                             leases[i].generation, leases[i].lease_id);
            resp.status = rc;
            ipc_send_response(c->fd, &resp);
            printf("main: ACQUIRE_BATCH from VM %u failed count=%u status=%d\n",
                   c->vm_id, count, rc);
            break;
        }

        for (uint32_t i = 0; i < count; i++) {
            c->allocated_slots[c->allocated_count++] = (struct client_slot_lease) {
                .slot_id = leases[i].slot_id,
                .generation = leases[i].generation,
                .lease_id = leases[i].lease_id,
                .attached = 0,
            };
        }
        resp.status = STATUS_OK;
        resp.slot_id = count;
        if (ipc_send_response(c->fd, &resp) < 0 ||
            ipc_send_all(c->fd, leases, count * sizeof(leases[0])) < 0) {
            handle_disconnect(c);
            return;
        }
        printf("main: ACQUIRE_BATCH from VM %u count=%u first_slot=%u last_slot=%u\n",
               c->vm_id, count, leases[0].slot_id, leases[count - 1].slot_id);
        break;
    }

    case MSG_RELEASE: {
        int index = client_find_lease(c, req.slot_id, req.generation, req.lease_id);
        int rc = index < 0 ? STATUS_LEASE_STALE :
            pool_release(&g_pool, c->vm_id, req.slot_id, req.generation, req.lease_id);
        resp.type   = MSG_RELEASE_ACK;
        resp.status = rc;
        if (rc == STATUS_OK) {
            client_remove_lease(c, index);
            printf("main: RELEASE from VM %u: slot=%u OK\n",
                   c->vm_id, req.slot_id);
        } else {
            printf("main: RELEASE from VM %u: slot=%u FAILED (status=%u)\n",
                   c->vm_id, req.slot_id, rc);
        }
        ipc_send_response(c->fd, &resp);
        break;
    }

    case MSG_ATTACH: {
        int index = client_find_lease(c, req.slot_id, req.generation, req.lease_id);
        int rc = index < 0 ? STATUS_LEASE_STALE :
            pool_attach(&g_pool, c->vm_id, req.slot_id, req.generation, req.lease_id);
        resp.type   = MSG_ATTACH_ACK;
        resp.status = rc;
        if (rc == STATUS_OK) {
            c->allocated_slots[index].attached = 1;
            printf("main: ATTACH from VM %u: slot=%u OK\n",
                   c->vm_id, req.slot_id);
        } else {
            printf("main: ATTACH from VM %u: slot=%u FAILED (status=%u)\n",
                   c->vm_id, req.slot_id, rc);
        }
        ipc_send_response(c->fd, &resp);
        break;
    }

    case MSG_ATTACH_BATCH: {
        struct preheat_batch_lease leases[MAX_CLIENT_SLOTS];
        int indexes[MAX_CLIENT_SLOTS];
        uint32_t count = req.slot_id;
        int rc = STATUS_OK;

        resp.type = MSG_ATTACH_BATCH_ACK;
        if (count == 0 || count > STRATOVIRT_CXL_SLOT_COUNT) {
            handle_disconnect(c);
            return;
        }
        if (ipc_recv_all(c->fd, leases, count * sizeof(leases[0])) < 0) {
            handle_disconnect(c);
            return;
        }
        for (uint32_t i = 0; i < count; i++) {
            indexes[i] = client_find_lease(c, leases[i].slot_id,
                                           leases[i].generation,
                                           leases[i].lease_id);
            if (indexes[i] < 0) {
                rc = STATUS_LEASE_STALE;
                break;
            }
        }
        for (uint32_t i = 0; rc == STATUS_OK && i < count; i++) {
            rc = pool_attach(&g_pool, c->vm_id, leases[i].slot_id,
                             leases[i].generation, leases[i].lease_id);
            if (rc == STATUS_OK)
                c->allocated_slots[indexes[i]].attached = 1;
        }

        resp.status = rc;
        resp.slot_id = rc == STATUS_OK ? count : 0;
        ipc_send_response(c->fd, &resp);
        printf("main: ATTACH_BATCH from VM %u count=%u status=%d\n",
               c->vm_id, count, rc);
        break;
    }

    case MSG_STATUS:
        resp.type       = MSG_STATUS_ACK;
        resp.status     = STATUS_OK;
        resp.slot_id    = atomic_load(&g_pool.free_hot_count);
        resp.generation = atomic_load(&g_pool.inflight_count);
        resp.offset     = atomic_load(&g_pool.alloc_count);
        resp.slot_size  = atomic_load(&g_pool.reheat_count);
        resp.lease_id   = ipc_status_encode_lease(atomic_load(&g_pool.cooldown_count),
                                                  g_pool.backend.simulated);
        resp.ttl_ns     = ipc_status_encode_ttl(g_pool.total_slots, g_pool.backend.kind);
        ipc_send_response(c->fd, &resp);
        printf("main: STATUS → free_hot=%u inflight=%u alloc=%lu reheat=%lu cooldown=%u total=%u backend=%s simulated=%u\n",
               resp.slot_id, resp.generation,
               (unsigned long)resp.offset, (unsigned long)resp.slot_size,
               ipc_status_cooldown_count(resp.lease_id),
               ipc_status_total_slots(resp.ttl_ns),
               backing_store_type_name((enum pool_store_type)ipc_status_backend_kind(resp.ttl_ns)),
               ipc_status_backend_simulated(resp.lease_id));
        printf("main: STATUS attached=%u\n", atomic_load(&g_pool.attached_count));
        break;

    case MSG_LAYOUT:
        resp.type       = MSG_LAYOUT_ACK;
        resp.status     = STATUS_OK;
        resp.slot_id    = g_pool.total_slots;
        resp.generation = g_pool.backend.kind;
        resp.offset     = g_pool.header_size;
        resp.slot_size  = g_pool.slot_size;
        resp.lease_id   = g_pool.map_size;
        resp.ttl_ns     = g_pool.backing_alignment;
        ipc_send_response(c->fd, &resp);
        printf("main: LAYOUT -> VM %u total_slots=%u slot_size=%zu header=%zu map_size=%zu align=%zu backend=%s\n",
               c->vm_id, g_pool.total_slots, g_pool.slot_size,
               g_pool.header_size, g_pool.map_size, g_pool.backing_alignment,
               backing_store_type_name(g_pool.store_type));
        break;

    default:
        fprintf(stderr, "main: Unknown message type %u from VM %u\n",
                req.type, c->vm_id);
        resp.type   = MSG_RELEASE_ACK;
        resp.status = STATUS_ERROR;
        ipc_send_response(c->fd, &resp);
        break;
    }
}

/* ── Usage ───────────────────────────────────────────── */

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [OPTIONS]\n"
        "  --slots=N          Number of buffer slots (default: %d)\n"
        "  --slot-size=BYTES  Size of each slot in bytes (default: %lu)\n"
        "  --sock=PATH        Unix socket path (default: %s)\n"
        "  --store=TYPE       Backing store: memfd|file|device-dax (default: memfd)\n"
        "  --store-path=PATH  Backing path (required for --store=file/device-dax)\n"
        "  --max-leases-per-vm=N  Per-VM lease cap (default: at least 16)\n"
        "  --max-inflight=N   Inflight lease cap (default: at least 16)\n"
        "  -h, --help         Show this help\n",
        prog, DEFAULT_TOTAL_SLOTS,
        (unsigned long)DEFAULT_SLOT_SIZE, DEFAULT_SOCK);
}

/* ── Parse size with suffix (M, G) ───────────────────── */

static size_t parse_size(const char *s)
{
    char *end;
    size_t val = strtoull(s, &end, 10);
    if (*end == 'M' || *end == 'm')
        val *= 1024UL * 1024;
    else if (*end == 'G' || *end == 'g')
        val *= 1024UL * 1024 * 1024;
    return val;
}

/* ── Main ────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    uint32_t    total_slots = DEFAULT_TOTAL_SLOTS;
    size_t      slot_size   = DEFAULT_SLOT_SIZE;
    const char *sock_path   = DEFAULT_SOCK;
    enum pool_store_type store_type = POOL_STORE_MEMFD;
    const char *store_path = NULL;
    uint32_t max_leases_per_vm = 0;
    uint32_t max_inflight = 0;
    struct pool_init_opts pool_opts;

    static struct option long_opts[] = {
        {"slots",     required_argument, NULL, 's'},
        {"slot-size", required_argument, NULL, 'z'},
        {"sock",      required_argument, NULL, 'k'},
        {"store",     required_argument, NULL, 'b'},
        {"store-path", required_argument, NULL, 'p'},
        {"max-leases-per-vm", required_argument, NULL, 'l'},
        {"max-inflight", required_argument, NULL, 'i'},
        {"help",      no_argument,       NULL, 'h'},
        {NULL, 0, NULL, 0},
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 's': total_slots = atoi(optarg); break;
        case 'z': slot_size   = parse_size(optarg); break;
        case 'k': sock_path   = optarg; break;
        case 'b':
            if (strcmp(optarg, "memfd") == 0) {
                store_type = POOL_STORE_MEMFD;
            } else if (strcmp(optarg, "file") == 0) {
                store_type = POOL_STORE_FILE;
            } else if (strcmp(optarg, "device-dax") == 0 ||
                       strcmp(optarg, "devdax") == 0) {
                store_type = POOL_STORE_DEVDAX;
            } else {
                fprintf(stderr, "Unknown --store value '%s' (expected memfd|file|device-dax)\n", optarg);
                return 1;
            }
            break;
        case 'p':
            store_path = optarg;
            break;
        case 'l':
            max_leases_per_vm = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'i':
            max_inflight = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'h':
        default:
            usage(argv[0]);
            return (opt == 'h') ? 0 : 1;
        }
    }

    if (store_type == POOL_STORE_FILE && (!store_path || store_path[0] == '\0')) {
        fprintf(stderr, "--store=file requires --store-path\n");
        return 1;
    }
    if (store_type == POOL_STORE_DEVDAX && (!store_path || store_path[0] == '\0')) {
        fprintf(stderr, "--store=device-dax requires --store-path (e.g. /dev/dax0.0)\n");
        return 1;
    }
    memset(&pool_opts, 0, sizeof(pool_opts));
    pool_opts.store_type = store_type;
    pool_opts.store_path = store_path;
    pool_opts.max_leases_per_vm = max_leases_per_vm;
    pool_opts.max_inflight = max_inflight;

    printf("shrink-preheat-daemon: slots=%u slot_size=%zuMB sock=%s store=%s%s%s\n",
           total_slots, slot_size / (1024 * 1024), sock_path,
           backing_store_type_name(store_type),
           store_path ? " store_path=" : "",
           store_path ? store_path : "");

    /* Signals */
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    /* Initialize pool */
    if (pool_init_with_opts(&g_pool, total_slots, slot_size, &pool_opts) < 0) {
        fprintf(stderr, "Failed to initialize pool\n");
        return 1;
    }

    /* Create listening socket */
    int listen_fd = ipc_create_listen_socket(sock_path);
    if (listen_fd < 0) {
        fprintf(stderr, "Failed to create listen socket at %s\n", sock_path);
        pool_destroy(&g_pool);
        return 1;
    }
    printf("main: Listening on %s\n", sock_path);

    /* epoll setup */
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        perror("epoll_create1");
        close(listen_fd);
        pool_destroy(&g_pool);
        return 1;
    }

    struct epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    /* Event loop */
    struct epoll_event events[MAX_EVENTS];
    printf("main: Entering event loop\n");

    while (g_running) {
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, 1000);
        if (nfds < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == listen_fd) {
                /* New connection */
                int client_fd = accept(listen_fd, NULL, NULL);
                if (client_fd < 0) {
                    perror("accept");
                    continue;
                }
                struct client_state *c = add_client(client_fd);
                if (!c) {
                    close(client_fd);
                    continue;
                }
                ev.events  = EPOLLIN | EPOLLHUP | EPOLLRDHUP;
                ev.data.fd = client_fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev);
                printf("main: New connection fd=%d\n", client_fd);
            } else if (events[i].events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                /* Disconnect */
                struct client_state *c = find_client(fd);
                if (c) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    handle_disconnect(c);
                }
            } else if (events[i].events & EPOLLIN) {
                /* Incoming message */
                struct client_state *c = find_client(fd);
                if (c) {
                    handle_message(c);
                    /* Client may have been removed in handle_message */
                    if (!find_client(fd)) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    }
                }
            }
        }
    }

    printf("main: Shutting down...\n");

    /* Cleanup */
    for (int i = g_client_count - 1; i >= 0; i--) {
        close(g_clients[i].fd);
    }
    close(epfd);
    close(listen_fd);
    unlink(sock_path);
    pool_destroy(&g_pool);

    printf("main: Done.\n");
    return 0;
}
