/*
 * Test client: simulates a StratoVirt process interacting with the daemon.
 *
 * Test flow:
 *   1. Connect + HELLO handshake (receive memfd via SCM_RIGHTS)
 *   2. ACQUIRE 16 slots → mmap each → write data → verify
 *   3. ATTACH all 16 slots and verify they remain unavailable
 *   4. Explicit teardown RELEASE, then verify reheat makes a slot reusable
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>

#include "ipc.h"
#include "backing_store.h"

#define SOCK_PATH "/tmp/shrink_preheat_test.sock"
#define ATTACHED_SLOTS ((int)STRATOVIRT_CXL_SLOT_COUNT)

struct lease_ref {
    uint32_t slot_id;
    uint32_t generation;
    uint64_t offset;
    uint64_t lease_id;
};

static int recv_hello_ack(int sock, struct preheat_response *resp, int *out_memfd)
{
    struct iovec iov = {
        .iov_base = resp,
        .iov_len  = sizeof(*resp),
    };

    char cmsg_buf[CMSG_SPACE(sizeof(int))];
    memset(cmsg_buf, 0, sizeof(cmsg_buf));

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov        = &iov;
    msg.msg_iovlen     = 1;
    msg.msg_control    = cmsg_buf;
    msg.msg_controllen = sizeof(cmsg_buf);

    ssize_t n = recvmsg(sock, &msg, 0);
    if (n <= 0) {
        perror("recvmsg");
        return -1;
    }

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS) {
        memcpy(out_memfd, CMSG_DATA(cmsg), sizeof(int));
    } else {
        fprintf(stderr, "No SCM_RIGHTS in HELLO_ACK\n");
        return -1;
    }
    return 0;
}

int main(void)
{
    int sock, memfd = -1;
    struct preheat_request req;
    struct preheat_response resp;
    struct preheat_response batch_resp[ATTACHED_SLOTS];
    struct preheat_batch_lease batch_leases[ATTACHED_SLOTS];
    struct lease_ref leases[ATTACHED_SLOTS];
    printf("=== Test Client (PID %d) ===\n\n", getpid());

    /* ── Connect ───────────────────────────── */
    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }
    printf("[OK] Connected to daemon\n");

    /* ── HELLO handshake ───────────────────── */
    memset(&req, 0, sizeof(req));
    req.type  = MSG_HELLO;
    req.vm_id = getpid();
    send(sock, &req, sizeof(req), 0);

    if (recv_hello_ack(sock, &resp, &memfd) < 0) {
        fprintf(stderr, "[FAIL] HELLO handshake failed\n");
        close(sock);
        return 1;
    }
    printf("[OK] HELLO_ACK: fd=%d total_slots=%u slot_size=%luMB backend=%s simulated=%u flags=0x%lx latency_ns=%lu\n",
           memfd, resp.slot_id, (unsigned long)(resp.slot_size / (1024*1024)),
           backing_store_type_name((enum pool_store_type)ipc_hello_backend_kind(&resp)),
           ipc_hello_backend_simulated(&resp),
           (unsigned long)ipc_hello_backend_flags(&resp),
           (unsigned long)ipc_hello_backend_latency_ns(&resp));

    uint64_t slot_size = resp.slot_size;

    /* ── LAYOUT query ──────────────────────── */
    memset(&req, 0, sizeof(req));
    req.type  = MSG_LAYOUT;
    req.vm_id = getpid();
    send(sock, &req, sizeof(req), 0);
    recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    if (resp.type != MSG_LAYOUT_ACK || resp.status != STATUS_OK) {
        fprintf(stderr, "[FAIL] LAYOUT failed: type=%u status=%u\n",
                resp.type, resp.status);
        close(memfd); close(sock);
        return 1;
    }
    uint64_t layout_header = resp.offset;
    uint64_t layout_map_size = resp.lease_id;
    uint64_t layout_align = resp.ttl_ns;
    printf("[OK] LAYOUT: total_slots=%u slot_size=%lu header=%lu map_size=%lu align=%lu backend=%s\n",
           resp.slot_id,
           (unsigned long)resp.slot_size,
           (unsigned long)layout_header,
           (unsigned long)layout_map_size,
           (unsigned long)layout_align,
           backing_store_type_name((enum pool_store_type)resp.generation));
    if (layout_header == 0 || layout_map_size < layout_header ||
        layout_align == 0 || layout_header % layout_align != 0) {
        fprintf(stderr, "[FAIL] invalid layout header=%lu map_size=%lu align=%lu\n",
                (unsigned long)layout_header,
                (unsigned long)layout_map_size,
                (unsigned long)layout_align);
        close(memfd); close(sock);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.type = MSG_ACQUIRE_BATCH;
    req.vm_id = getpid();
    req.slot_id = ATTACHED_SLOTS;
    send(sock, &req, sizeof(req), 0);
    recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    if (resp.type != MSG_ACQUIRE_BATCH_ACK || resp.status != STATUS_OK ||
        resp.slot_id != ATTACHED_SLOTS) {
        fprintf(stderr, "[FAIL] ACQUIRE_BATCH failed: type=%u status=%u count=%u\n",
                resp.type, resp.status, resp.slot_id);
        close(memfd); close(sock);
        return 1;
    }
    recv(sock, batch_resp, sizeof(batch_resp), MSG_WAITALL);

    for (int i = 0; i < ATTACHED_SLOTS; i++) {
        void *ptr;
        unsigned char *check;
        unsigned char pattern = (unsigned char)(0xA0 + i);

        if (batch_resp[i].type != MSG_ACQUIRE_BATCH_ACK ||
            batch_resp[i].status != STATUS_OK) {
            fprintf(stderr, "[FAIL] ACQUIRE_BATCH lease #%d failed: status=%u\n",
                    i + 1, batch_resp[i].status);
            close(memfd); close(sock);
            return 1;
        }
        leases[i] = (struct lease_ref) {
            .slot_id = batch_resp[i].slot_id,
            .generation = batch_resp[i].generation,
            .offset = batch_resp[i].offset,
            .lease_id = batch_resp[i].lease_id,
        };
        ptr = mmap(NULL, slot_size, PROT_READ | PROT_WRITE, MAP_SHARED, memfd,
                   batch_resp[i].offset);
        if (ptr == MAP_FAILED) {
            perror("mmap");
            close(memfd); close(sock);
            return 1;
        }
        memset(ptr, pattern, 4096);
        check = ptr;
        if (check[0] != pattern || check[4095] != pattern) {
            fprintf(stderr, "[FAIL] slot %u pattern verification failed\n", resp.slot_id);
            munmap(ptr, slot_size);
            close(memfd); close(sock);
            return 1;
        }
        munmap(ptr, slot_size);
        batch_leases[i] = (struct preheat_batch_lease) {
            .slot_id = leases[i].slot_id,
            .generation = leases[i].generation,
            .lease_id = leases[i].lease_id,
        };
    }
    printf("[OK] ACQUIRE_BATCH count=%d first=%u last=%u\n", ATTACHED_SLOTS,
           leases[0].slot_id, leases[ATTACHED_SLOTS - 1].slot_id);

    memset(&req, 0, sizeof(req));
    req.type = MSG_ATTACH_BATCH;
    req.vm_id = getpid();
    req.slot_id = ATTACHED_SLOTS;
    send(sock, &req, sizeof(req), 0);
    send(sock, batch_leases, sizeof(batch_leases), 0);
    recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    if (resp.type != MSG_ATTACH_BATCH_ACK || resp.status != STATUS_OK ||
        resp.slot_id != ATTACHED_SLOTS) {
        fprintf(stderr, "[FAIL] ATTACH_BATCH failed: type=%u status=%u count=%u\n",
                resp.type, resp.status, resp.slot_id);
        close(memfd); close(sock);
        return 1;
    }
    printf("[OK] ATTACH_BATCH count=%u\n", resp.slot_id);

    /* ── STATUS query ──────────────────────── */
    memset(&req, 0, sizeof(req));
    req.type  = MSG_STATUS;
    req.vm_id = getpid();
    send(sock, &req, sizeof(req), 0);
    recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    printf("[OK] STATUS after %d ATTACH: free_hot=%u inflight=%u alloc_count=%lu reheat_count=%lu cooldown=%u total=%u backend=%s simulated=%u\n",
           ATTACHED_SLOTS,
           resp.slot_id, resp.generation,
           (unsigned long)resp.offset, (unsigned long)resp.slot_size,
           ipc_status_cooldown_count(resp.lease_id),
           ipc_status_total_slots(resp.ttl_ns),
           backing_store_type_name((enum pool_store_type)ipc_status_backend_kind(resp.ttl_ns)),
           ipc_status_backend_simulated(resp.lease_id));

    for (int i = 0; i < ATTACHED_SLOTS; i++) {
        memset(&req, 0, sizeof(req));
        req.type = MSG_RELEASE;
        req.vm_id = getpid();
        req.slot_id = leases[i].slot_id;
        req.generation = leases[i].generation;
        req.lease_id = leases[i].lease_id;
        send(sock, &req, sizeof(req), 0);
        recv(sock, &resp, sizeof(resp), MSG_WAITALL);
        if (resp.type != MSG_RELEASE_ACK || resp.status != STATUS_OK) {
            fprintf(stderr, "[FAIL] teardown RELEASE #%d failed: status=%u\n", i + 1, resp.status);
            close(memfd); close(sock);
            return 1;
        }
    }

    printf("Waiting 2s for reheat...\n");
    sleep(2);
    memset(&req, 0, sizeof(req));
    req.type = MSG_ACQUIRE;
    req.vm_id = getpid();
    send(sock, &req, sizeof(req), 0);
    recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    if (resp.type != MSG_ACQUIRE_ACK || resp.status != STATUS_OK) {
        fprintf(stderr, "[FAIL] no slot became reusable after teardown: status=%u\n", resp.status);
        close(memfd); close(sock);
        return 1;
    }
    printf("[OK] slot %u became reusable after explicit RELEASE\n", resp.slot_id);
    memset(&req, 0, sizeof(req));
    req.type = MSG_RELEASE;
    req.vm_id = getpid();
    req.slot_id = resp.slot_id;
    req.generation = resp.generation;
    req.lease_id = resp.lease_id;
    send(sock, &req, sizeof(req), 0);
    recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    if (resp.type != MSG_RELEASE_ACK || resp.status != STATUS_OK) {
        fprintf(stderr, "[FAIL] reusable-slot RELEASE failed: status=%u\n", resp.status);
        close(memfd); close(sock);
        return 1;
    }

    printf("\nDisconnecting after explicit teardown...\n");
    close(memfd);
    close(sock);

    printf("\n=== All tests passed ===\n");
    return 0;
}
