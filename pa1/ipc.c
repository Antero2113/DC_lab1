#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "ipc.h"
#include "proc.h"

#define WOULD_BLOCK (-2)

static const struct timespec poll_delay = {0, 1000000L};

static int write_full(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, p + done, len - done);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        done += (size_t)n;
    }
    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, p + done, len - done);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (done == 0)
                    return WOULD_BLOCK;
                continue;
            }
            return -1;
        }
        if (n == 0)
            return 1;
        done += (size_t)n;
    }
    return 0;
}

int send(void *self, local_id dst, const Message *msg)
{
    Proc *p = (Proc *)self;
    if (p == NULL || msg == NULL)
        return -1;
    if (dst < 0 || dst >= (local_id)p->n || dst == p->id || p->wr[dst] < 0)
        return -1;
    if (write_full(p->wr[dst], &msg->s_header, sizeof(msg->s_header)) != 0)
        return -1;
    if (msg->s_header.s_payload_len > 0 &&
        write_full(p->wr[dst], msg->s_payload, msg->s_header.s_payload_len) != 0)
        return -1;
    return 0;
}

int send_multicast(void *self, const Message *msg)
{
    Proc *p = (Proc *)self;
    if (p == NULL || msg == NULL)
        return -1;
    for (local_id dst = 0; dst < (local_id)p->n; dst++) {
        if (dst == p->id)
            continue;
        if (send(self, dst, msg) != 0)
            return -1;
    }
    return 0;
}

int receive(void *self, local_id from, Message *msg)
{
    Proc *p = (Proc *)self;
    if (p == NULL || msg == NULL)
        return -1;
    if (from < 0 || from >= (local_id)p->n || from == p->id || p->rd[from] < 0)
        return -1;
    for (;;) {
        int r = read_full(p->rd[from], &msg->s_header, sizeof(msg->s_header));
        if (r == WOULD_BLOCK) {
            nanosleep(&poll_delay, NULL);
            continue;
        }
        if (r != 0)
            return -1;
        break;
    }
    if (msg->s_header.s_magic != MESSAGE_MAGIC ||
        msg->s_header.s_payload_len > MAX_PAYLOAD_LEN)
        return -1;
    for (;;) {
        int r = read_full(p->rd[from], msg->s_payload, msg->s_header.s_payload_len);
        if (r == WOULD_BLOCK) {
            nanosleep(&poll_delay, NULL);
            continue;
        }
        if (r != 0)
            return -1;
        break;
    }
    return 0;
}

int receive_any(void *self, Message *msg)
{
    Proc *p = (Proc *)self;
    if (p == NULL || msg == NULL)
        return -1;
    for (;;) {
        for (local_id from = 0; from < (local_id)p->n; from++) {
            if (from == p->id || p->rd[from] < 0)
                continue;
            int r = read_full(p->rd[from], &msg->s_header, sizeof(msg->s_header));
            if (r == WOULD_BLOCK)
                continue;
            if (r != 0)
                return -1;
            if (msg->s_header.s_magic != MESSAGE_MAGIC ||
                msg->s_header.s_payload_len > MAX_PAYLOAD_LEN)
                return -1;
            for (;;) {
                r = read_full(p->rd[from], msg->s_payload, msg->s_header.s_payload_len);
                if (r == WOULD_BLOCK) {
                    nanosleep(&poll_delay, NULL);
                    continue;
                }
                if (r != 0)
                    return -1;
                break;
            }
            p->last_from = from;
            return 0;
        }
        nanosleep(&poll_delay, NULL);
    }
}
