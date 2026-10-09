#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common.h"
#include "ipc.h"
#include "pa1.h"
#include "proc.h"

static FILE *events_log_file;
static FILE *pipes_log_file;

static void log_event(char *buf, size_t size, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(buf, size, fmt, ap);
    va_end(ap);
    if (len <= 0)
        return;
    fputs(buf, stdout);
    fflush(stdout);
    if (events_log_file != NULL) {
        fputs(buf, events_log_file);
        fflush(events_log_file);
    }
}

static void log_pipe(const char *fmt, ...)
{
    if (pipes_log_file == NULL)
        return;
    va_list ap;
    va_start(ap, fmt);
    vfprintf(pipes_log_file, fmt, ap);
    va_end(ap);
    fflush(pipes_log_file);
}

static void build_message(Message *msg, MessageType type, const char *payload)
{
    memset(msg, 0, sizeof(*msg));
    msg->s_header.s_magic = MESSAGE_MAGIC;
    msg->s_header.s_type = (int16_t)type;
    msg->s_header.s_local_time = 0;
    msg->s_header.s_payload_len = (uint16_t)strlen(payload);
    memcpy(msg->s_payload, payload, msg->s_header.s_payload_len);
}

static int child_phase_started(Proc *self)
{
    char buf[MAX_PAYLOAD_LEN];
    Message msg;

    log_event(buf, sizeof(buf), log_started_fmt, (int)self->id, (int)getpid(), (int)getppid());
    build_message(&msg, STARTED, buf);
    if (send_multicast(self, &msg) != 0)
        return -1;
    for (local_id from = 1; from < (local_id)self->n; from++) {
        if (from == self->id)
            continue;
        if (receive(self, from, &msg) != 0 || msg.s_header.s_type != STARTED)
            return -1;
    }
    log_event(buf, sizeof(buf), log_received_all_started_fmt, (int)self->id);
    return 0;
}

static int child_phase_done(Proc *self)
{
    char buf[MAX_PAYLOAD_LEN];
    Message msg;

    log_event(buf, sizeof(buf), log_done_fmt, (int)self->id);
    build_message(&msg, DONE, buf);
    if (send_multicast(self, &msg) != 0)
        return -1;
    for (local_id from = 1; from < (local_id)self->n; from++) {
        if (from == self->id)
            continue;
        if (receive(self, from, &msg) != 0 || msg.s_header.s_type != DONE)
            return -1;
    }
    log_event(buf, sizeof(buf), log_received_all_done_fmt, (int)self->id);
    return 0;
}

static int parent_phase(Proc *self)
{
    Message msg;

    for (local_id from = 1; from < (local_id)self->n; from++) {
        if (receive(self, from, &msg) != 0 || msg.s_header.s_type != STARTED)
            return -1;
    }
    for (local_id from = 1; from < (local_id)self->n; from++) {
        if (receive(self, from, &msg) != 0 || msg.s_header.s_type != DONE)
            return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    long x = -1;
    int opt;
    int is_child = 0;
    pid_t pids[MAX_IDS] = {0};
    int rd[MAX_IDS][MAX_IDS];
    int wr[MAX_IDS][MAX_IDS];

    opterr = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        if (opt == 'p')
            x = strtol(optarg, NULL, 10);
    }
    if (x < 1 || x > MAX_PROCESS_ID)
        return 1;

    local_id n = (local_id)(x + 1);
    events_log_file = fopen(events_log, "a");
    pipes_log_file = fopen(pipes_log, "a");

    for (local_id i = 0; i < n; i++)
        for (local_id j = 0; j < n; j++)
            rd[i][j] = wr[i][j] = -1;

    for (local_id src = 0; src < n; src++) {
        for (local_id dst = 0; dst < n; dst++) {
            int fds[2];
            if (src == dst)
                continue;
            if (pipe(fds) != 0)
                return 1;
            fcntl(fds[0], F_SETFL, O_NONBLOCK);
            rd[dst][src] = fds[0];
            wr[src][dst] = fds[1];
            log_pipe("Pipe from %d to %d: read end fd %d, write end fd %d\n",
                     (int)src, (int)dst, fds[0], fds[1]);
        }
    }

    Proc self;
    memset(&self, 0, sizeof(self));
    self.n = n;
    self.id = PARENT_ID;

    for (local_id id = 1; id < n; id++) {
        pid_t pid = fork();
        if (pid < 0)
            return 1;
        if (pid == 0) {
            self.id = id;
            is_child = 1;
            break;
        }
        pids[id] = pid;
    }

    for (local_id owner = 0; owner < n; owner++) {
        for (local_id peer = 0; peer < n; peer++) {
            if (owner == peer)
                continue;
            if (owner != self.id) {
                if (rd[owner][peer] >= 0)
                    close(rd[owner][peer]);
                if (wr[owner][peer] >= 0)
                    close(wr[owner][peer]);
                rd[owner][peer] = -1;
                wr[owner][peer] = -1;
            }
        }
    }
    for (local_id peer = 0; peer < n; peer++) {
        self.rd[peer] = (peer == self.id) ? -1 : rd[self.id][peer];
        self.wr[peer] = (peer == self.id) ? -1 : wr[self.id][peer];
    }
    if (!is_child) {
        for (local_id peer = 0; peer < n; peer++) {
            if (self.wr[peer] >= 0) {
                close(self.wr[peer]);
                self.wr[peer] = -1;
            }
        }
    }
    for (local_id peer = 0; peer < n; peer++) {
        if (peer != self.id && self.rd[peer] >= 0)
            log_pipe("Process %d keeps read end fd %d for messages from process %d\n",
                     (int)self.id, self.rd[peer], (int)peer);
        if (is_child && peer != self.id && self.wr[peer] >= 0)
            log_pipe("Process %d keeps write end fd %d for messages to process %d\n",
                     (int)self.id, self.wr[peer], (int)peer);
    }

    int result;
    if (is_child)
        result = (child_phase_started(&self) == 0 && child_phase_done(&self) == 0) ? 0 : 1;
    else
        result = (parent_phase(&self) == 0) ? 0 : 1;

    if (is_child)
        return result;

    for (local_id id = 1; id < n; id++) {
        int status;
        while (waitpid(pids[id], &status, 0) != pids[id]) {
            if (errno != EINTR)
                break;
        }
    }
    for (local_id peer = 0; peer < n; peer++) {
        if (self.rd[peer] >= 0)
            close(self.rd[peer]);
        if (self.wr[peer] >= 0)
            close(self.wr[peer]);
    }
    if (pipes_log_file != NULL)
        fclose(pipes_log_file);
    if (events_log_file != NULL)
        fclose(events_log_file);
    return result;
}
