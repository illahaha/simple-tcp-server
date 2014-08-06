/*
 * Copyright (c) 2014, Thorben Hasenpusch <thorben.hasenpusch@gmail.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdnoreturn.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <signal.h>

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/event.h>
#include <sys/sysctl.h>

#include <netinet/in.h>

#include <arpa/inet.h>

static noreturn void fatal(const char *msg);
static noreturn void fatal2(const char *msg);
static noreturn void fatal3(const char *msg, int err);

static unsigned short strtous(const char *restrict str, char **restrict endptr, int base);
static int strtoi(const char *restrict str, char **restrict endptr, int base);
static unsigned short str_to_ushort(const char *str, int base);
static int str_to_int(const char *str, int base);

static int get_num_cpus(void);
static int get_somaxconn(void);

static void *handle_signals(void *data);

static void *worker_thread(void *data);

struct peer_data {
    struct sockaddr_storage sockaddr;
    void *addr_ptr; // points to somewhere in sockaddr
    int is_connected; // XXX: should be atomic
    int fd;
    in_port_t port;
    char ascii_addr[INET6_ADDRSTRLEN];
};

struct worker_data {
    int kqueue_fd;
    int num_events;
    int signal_pipe_fd; // Got SIGINT/SIGTERM -> terminate self
    int term_pipe_fd; // Notify main thread of graceful termination
};

struct signal_handling_thread_data {
    int pipe_fd; // Notify worker threads of signal
    sigset_t sigset;
};

int main(int argc, const char *argv[])
{
    if (argc < 2)
        fatal2("Missing port number as argument");

    int somaxconn = get_somaxconn();
    int backlog = somaxconn;
    if (argc == 3) {
        backlog = str_to_int(argv[2], 0);

        if (backlog == 0) {
            fprintf(stderr, "Backlog has to be bigger than 0.\n");
            return 1;
        }

        if (backlog > somaxconn) {
            fprintf(stderr, "Backlog exceeds kern.ipc.somaxconn, truncating...\n");
            backlog = somaxconn;
        }
    }

    // Prefer IPv6, but fallback to IPv4 when not available

    int sockfd;
    struct sockaddr_storage sockaddr;
    socklen_t len;

    sockfd = socket(AF_INET6, SOCK_STREAM, 0);
    if (sockfd == -1) {
        if (errno != EAFNOSUPPORT)
            fatal("socket()");

        // No IPv6, so...

        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1)
            fatal("socket()");

        *(struct sockaddr_in *)&sockaddr = (struct sockaddr_in){
            .sin_family = AF_INET,
            .sin_addr = {
                .s_addr = INADDR_ANY
            },
            .sin_port = htons(str_to_ushort(argv[1], 0))
        };

        len = sizeof(struct sockaddr_in);
    } else {
        *(struct sockaddr_in6 *)&sockaddr = (struct sockaddr_in6){
            .sin6_family = AF_INET6,
            .sin6_addr = in6addr_any,
            .sin6_port = htons(str_to_ushort(argv[1], 0))
        };

        len = sizeof(struct sockaddr_in6);
    }

    if (bind(sockfd, (struct sockaddr *)&sockaddr, len) == -1)
        fatal("bind()");

    if (listen(sockfd, backlog) == -1)
        fatal("listen()");

    int max_num_active_conn = backlog;

    struct peer_data *peer_data = calloc((size_t)max_num_active_conn, sizeof(*peer_data));
    if (peer_data == NULL)
        fatal("calloc()");

    // Stuff for handling signals in its own thread
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    if (pthread_sigmask(SIG_SETMASK, &set, NULL) == -1)
        fatal("pthread_sigmask()");

    int signal_pipe_fds[2];
    if (pipe(signal_pipe_fds) == -1)
        fatal("pipe()");

    struct signal_handling_thread_data sig_handling_data = {
        .sigset = set,
        .pipe_fd = signal_pipe_fds[1]
    };

    pthread_t signal_handling_thread;
    int err = pthread_create(&signal_handling_thread, NULL, handle_signals, &sig_handling_data);
    if (err != 0)
        fatal3("pthread_create()", err);

    err = pthread_detach(signal_handling_thread);
    if (err != 0)
        fatal3("pthread_detach()", err);

    // Now set up the kqueue for worker threads

    int worker_queue_fd = kqueue();
    if (worker_queue_fd == -1)
        fatal("kqueue()");

    struct kevent events[2]; // gets reused throughout

    // Workers will get notfied of arrived signals through pipe
    EV_SET(&events[0], signal_pipe_fds[0], EVFILT_READ, EV_ADD, 0, 0, NULL);

    if (kevent(worker_queue_fd, events, 1, NULL, 0, NULL) == -1)
        fatal("kevent()");

    int term_pipe_fds[2];
    if (pipe(term_pipe_fds) == -1)
        fatal("pipe()");

    int num_threads = MIN(get_num_cpus(), backlog);

    struct worker_data *worker_data = calloc((size_t)num_threads, sizeof(*worker_data));
    if (worker_data == NULL)
        fatal("calloc()");

    pthread_t *threads = calloc((size_t)num_threads, sizeof(*threads));
    if (threads == NULL)
        fatal("calloc()");

    for (int i = 0; i < num_threads; ++i) {
        worker_data[i].kqueue_fd = worker_queue_fd;
        worker_data[i].num_events = max_num_active_conn + 1;
        worker_data[i].signal_pipe_fd = signal_pipe_fds[0];
        worker_data[i].term_pipe_fd = term_pipe_fds[1];

        err = pthread_create(&threads[i], NULL, worker_thread, &worker_data[i]);
        if (err != 0)
            fatal3("pthread_create()", err);

        err = pthread_detach(threads[i]);
        if (err != 0)
            fatal3("pthread_detach()", err);
    }

    int accept_queue_fd = kqueue();
    if (accept_queue_fd == -1)
        fatal("kqueue()");

    EV_SET(&events[0], sockfd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    EV_SET(&events[1], signal_pipe_fds[0], EVFILT_READ, EV_ADD, 0, 0, NULL);

    if (kevent(accept_queue_fd, events, 2, NULL, 0, NULL) == -1)
        fatal("kevent()");

    int idx = 0;

    for (;;) {
        struct kevent revents[2];

        int events_triggered = kevent(accept_queue_fd, NULL, 0, revents, 2, NULL);
        if (events_triggered == -1)
            fatal("kevent()");

        for (int i = 0; i < events_triggered; ++i) {
            assert(revents[i].filter == EVFILT_READ);

            int fd = (int)revents[i].ident;

            if (fd == signal_pipe_fds[0]) {
                // Got a signal. Now spin up a timer and
                // wait for worker threads to exit cleanly.
                goto wait_for_thread_termination;
            }

            // Got new connection

            assert(!peer_data[idx].is_connected);

            socklen_t size = sizeof(peer_data[idx].sockaddr);
            int new_fd = accept(sockfd, (struct sockaddr *)&peer_data[idx].sockaddr, &size);
            if (new_fd == -1)
                fatal("accept()");

            int af = peer_data[idx].sockaddr.ss_family;
            switch (af) {
            case AF_INET:
                peer_data[idx].addr_ptr = &((struct sockaddr_in *)&peer_data[idx].sockaddr)->sin_addr;
                peer_data[idx].port = ((struct sockaddr_in *)&peer_data[idx].sockaddr)->sin_port;
                break;
            case AF_INET6:
                peer_data[idx].addr_ptr = &((struct sockaddr_in6 *)&peer_data[idx].sockaddr)->sin6_addr;
                peer_data[idx].port = ((struct sockaddr_in6 *)&peer_data[idx].sockaddr)->sin6_port;
                break;
            }

            inet_ntop(af, peer_data[idx].addr_ptr, peer_data[idx].ascii_addr, sizeof(peer_data[idx].ascii_addr));

            printf("[INFO] New connection from %s:%u\n", peer_data[idx].ascii_addr, peer_data[idx].port);

            peer_data[idx].is_connected = 1;
            peer_data[idx].fd = new_fd;

            EV_SET(&events[0], new_fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, &peer_data[idx]);

            if (kevent(worker_queue_fd, events, 1, NULL, 0, NULL) == -1)
                fatal("kevent()");

            // Look for another free peer
            int tmp = idx;
            for (int j = idx; j < max_num_active_conn; ++j)
                if (!peer_data[j].is_connected)
                    idx = j;

            if (tmp != idx)
                continue;

            for (int j = 0; j < idx; ++j)
                if (!peer_data[j].is_connected)
                    idx = j;

            if (tmp != idx)
                continue;

            // No free slots, reallocate...
            peer_data = realloc(peer_data, (size_t)max_num_active_conn * 2);
            if (peer_data == NULL)
                fatal("realloc()");
            max_num_active_conn *= 2;
            idx = max_num_active_conn / 2;
        }
    }

wait_for_thread_termination:;
    // We could reuse one of the other 2 queues, but I think this is cleaner.
    int thread_term_queue_fd = kqueue();
    if (thread_term_queue_fd == -1)
        fatal("kqueue()");

    struct kevent revents[2];

    EV_SET(&events[0], term_pipe_fds[0], EVFILT_READ, EV_ADD, 0, 0, NULL);
    EV_SET(&events[1], 0, EVFILT_TIMER, EV_ADD, NOTE_SECONDS, 5, NULL);

    if (kevent(thread_term_queue_fd, events, 2, NULL, 0, NULL) == -1)
        fatal("kevent()");

    // Each thread sends 1 byte through the pipe to signal successful
    // termination. If we got num_threads amount of bytes through the pipe,
    // all threads exited and we can gracefully shutdown.
    int term_pipe_rem_size = num_threads;
    unsigned char *buf = malloc((size_t)num_threads);
    if (buf == NULL)
        fatal("malloc()");

    printf("Waiting for worker threads to quit...\n");

    for (;;) {
        int num = kevent(thread_term_queue_fd, events, 2, revents, 2, NULL);
        if (num == -1)
            fatal("kevent()");

        for (int i = 0; i < num; ++i) {
            if (revents[i].filter == EVFILT_TIMER) {
                printf("Some threads got stuck. Killing those...\n");

                for (int j = 0; j < num_threads; ++j)
                    pthread_cancel(threads[j]);

                goto end;
            }

            // Some thread(s) shut down

            int thread_fd = (int)revents[i].ident;
            size_t bytes_to_read = (size_t)revents[i].data;

            assert(bytes_to_read <= (size_t)num_threads);

            if (read(thread_fd, buf, bytes_to_read) == -1)
                fatal("read()");

            term_pipe_rem_size -= bytes_to_read;
            if (term_pipe_rem_size == 0)
                goto end;
        }
    }

end:;
    printf("Shutting down...\n");

    for (int i = 0; i < backlog; ++i)
        if (peer_data[i].is_connected)
            shutdown(peer_data[i].fd, SHUT_RDWR);

    return 0;
}

void *handle_signals(void *data)
{
    struct signal_handling_thread_data *info = data;

    int cleared_sig;
    int err = sigwait(&info->sigset, &cleared_sig);
    if (err != 0)
        fatal3("sigwait()", err);

    assert(cleared_sig == SIGINT || cleared_sig == SIGTERM);

    printf("[INFO] Received SIGINT/SIGTERM\n");

    unsigned char dummy;

    if (write(info->pipe_fd, &dummy, sizeof(dummy)) == -1)
        fatal("write()");

    return NULL;
}

void *worker_thread(void *data)
{
    struct worker_data *info = data;
    int kqueue_fd = info->kqueue_fd;
    int num_revents = info->num_events;

    struct kevent *revents = calloc((size_t)num_revents, sizeof(*revents));
    if (revents == NULL)
        fatal("calloc()");

    size_t bufsiz = 8192; // Should be big enough for most things
    unsigned char *buffer = malloc(bufsiz);
    if (buffer == NULL)
        fatal("malloc()");

    int re_add_event = 0;
    struct kevent *event_ptr = NULL;

    for (;;) {
        int events_recvd = kevent(kqueue_fd, event_ptr, re_add_event,
                                  revents, num_revents, NULL);
        if (events_recvd == -1)
            fatal("kevent()");

        for (int i = 0; i < events_recvd; ++i) {
            if (revents[i].flags & EV_ERROR)
                fatal3("Error processing events", (int)revents[i].data);

            assert(revents[i].filter = EVFILT_READ);

            int fd = (int)revents[i].ident;

            if (fd == info->signal_pipe_fd) {
                printf("[INFO] Worker thread shutting down\n");

                unsigned char t;
                if (write(info->term_pipe_fd, &t, sizeof(t)) == -1)
                    fatal("write()");

                return NULL;
            }

            struct peer_data *peer_data = revents[i].udata;
            size_t bytes_to_read = (size_t)revents[i].data;

            if (revents[i].data == 0 && revents[i].flags & EV_EOF) {
                printf("Lost connection with %s:%u\n", peer_data->ascii_addr,
                       peer_data->port);

                if (close(fd) == -1)
                    fatal("close()");

                peer_data->is_connected = 0;
                re_add_event = 0;
                event_ptr = NULL;
                continue;
            }

            printf("[INFO] %s:%u sent %zu bytes\n", peer_data->ascii_addr,
                   peer_data->port, bytes_to_read);

            if (bytes_to_read > bufsiz) {
                bufsiz = bytes_to_read;
                buffer = realloc(buffer, bufsiz);
                if (buffer == NULL)
                    fatal("realloc()");
            }

            ssize_t bytes_read = read(fd, buffer, bytes_to_read);
            if (bytes_read == -1)
                fatal("read()");

            printf("[BEGIN DATA]\n");

            if (write(STDOUT_FILENO, buffer, (size_t)bytes_read) == -1)
                fatal("write()");

            printf("\n[END DATA]\n");

            struct kevent event[1];
            EV_SET(&event[0], revents[i].ident, revents[i].filter, revents[i].flags,
                   revents[i].fflags, revents[i].data, revents[i].udata);
            event_ptr = event;
            re_add_event = 1;
        }
    }
}


void fatal(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

void fatal2(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}

void fatal3(const char *msg, int err)
{
    fprintf(stderr, "%s: %s\n", msg, strerror(err));
    exit(EXIT_FAILURE);
}

unsigned short strtous(const char *restrict str, char **restrict endptr,
                       int base)
{
    errno_t err = errno;

    errno = 0;
    unsigned long ret = strtoul(str, endptr, base);
    if (errno != 0)
        return USHRT_MAX;

    if (ret > USHRT_MAX) {
        errno = ERANGE;
        return USHRT_MAX;
    }

    errno = err;

    return (unsigned short)ret;
}

int strtoi(const char *restrict str, char **restrict endptr, int base)
{
    errno_t err = errno;

    errno = 0;
    long ret = strtol(str, endptr, base);
    if (errno != 0)
        return INT_MAX;

    if (ret > INT_MAX) {
        errno = ERANGE;
        return INT_MAX;
    }

    if (ret < INT_MIN) {
        errno = ERANGE;
        return INT_MIN;
    }

    errno = err;

    return (int)ret;
}

unsigned short str_to_ushort(const char *str, int base)
{
    char *endptr;
    unsigned short ret = strtous(str, &endptr, base);
    if (errno != 0)
        fatal("strtoul()");
    if (*endptr != '\0')
        fatal2("String to unsigned short conversion: encountered garbage");
    return ret;
}

int str_to_int(const char *str, int base)
{
    char *endptr;
    int ret = strtoi(str, &endptr, base);
    if (errno != 0)
        fatal("strtol()");
    if (*endptr != '\0')
        fatal2("String to int conversion: encountered garbage");
    return ret;
}

int get_num_cpus(void)
{
    int num_cpus;
    size_t size = sizeof(num_cpus);
    if (sysctlbyname("hw.logicalcpu", &num_cpus, &size, NULL, 0) == -1)
        fatal("sysctlbyname()");

    return num_cpus;
}

int get_somaxconn(void)
{
    int somaxconn;
    size_t size = sizeof(somaxconn);
    if (sysctlbyname("kern.ipc.somaxconn", &somaxconn, &size, NULL, 0) == -1)
        fatal("sysctlbyname()");

    return somaxconn;
}
