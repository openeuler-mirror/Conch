#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <errno.h>

#include "ipc.h"

/* ── Create & bind listening Unix socket ─────────────── */

static int ipc_remove_stale_socket(const char *sock_path)
{
    struct stat st;

    if (lstat(sock_path, &st) < 0) {
        if (errno == ENOENT)
            return 0;
        perror("lstat");
        return -1;
    }

    if (!S_ISSOCK(st.st_mode)) {
        fprintf(stderr, "ipc: %s exists and is not a socket\n", sock_path);
        errno = EEXIST;
        return -1;
    }

    if (unlink(sock_path) < 0) {
        perror("unlink");
        return -1;
    }

    printf("ipc: Removed stale socket %s\n", sock_path);
    return 0;
}

int ipc_create_listen_socket(const char *sock_path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    /* Remove stale socket file before bind */
    if (ipc_remove_stale_socket(sock_path) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno == EADDRINUSE) {
            if (ipc_remove_stale_socket(sock_path) == 0 &&
                bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
                goto bound;
            }
        }
        perror("bind");
        close(fd);
        return -1;
    }

bound:
    if (listen(fd, 16) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    return fd;
}

/* ── Send plain response (no fd passing) ─────────────── */

int ipc_send_all(int client_fd, const void *buffer, size_t length)
{
    const char *cursor = buffer;

    while (length) {
        ssize_t n = send(client_fd, cursor, length, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("ipc_send_all");
            return -1;
        }
        if (n == 0)
            return -1;
        cursor += n;
        length -= (size_t)n;
    }
    return 0;
}

int ipc_recv_all(int client_fd, void *buffer, size_t length)
{
    char *cursor = buffer;

    while (length) {
        ssize_t n = recv(client_fd, cursor, length, 0);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno != ECONNRESET)
                perror("ipc_recv_all");
            return -1;
        }
        if (n == 0)
            return -1;
        cursor += n;
        length -= (size_t)n;
    }
    return 0;
}

int ipc_send_response(int client_fd, const struct preheat_response *resp)
{
    return ipc_send_all(client_fd, resp, sizeof(*resp));
}

/* ── Send HELLO_ACK with SCM_RIGHTS fd passing ──────── */

int ipc_send_hello_ack(int client_fd, int memfd,
                       uint32_t total_slots, uint64_t slot_size,
                       const struct backend_info *backend)
{
    struct preheat_response resp;
    memset(&resp, 0, sizeof(resp));
    resp.type      = MSG_HELLO_ACK;
    resp.status    = STATUS_OK;
    resp.slot_id   = total_slots;  /* repurpose: total_slots info */
    resp.slot_size = slot_size;
    if (backend)
        ipc_hello_set_backend(&resp, backend);
    resp.lease_id |= BACKEND_FLAG_BATCH_OPS;

    struct iovec iov = {
        .iov_base = &resp,
        .iov_len  = sizeof(resp),
    };

    /* Ancillary data for SCM_RIGHTS */
    char cmsg_buf[CMSG_SPACE(sizeof(int))];
    memset(cmsg_buf, 0, sizeof(cmsg_buf));

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov        = &iov;
    msg.msg_iovlen     = 1;
    msg.msg_control    = cmsg_buf;
    msg.msg_controllen = sizeof(cmsg_buf);

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type  = SCM_RIGHTS;
    cmsg->cmsg_len   = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &memfd, sizeof(int));

    ssize_t n = sendmsg(client_fd, &msg, MSG_NOSIGNAL);
    if (n < 0) {
        perror("ipc_send_hello_ack sendmsg");
        return -1;
    }
    return 0;
}

/* ── Receive request ─────────────────────────────────── */

int ipc_recv_request(int client_fd, struct preheat_request *req)
{
    return ipc_recv_all(client_fd, req, sizeof(*req));
}
