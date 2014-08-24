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
#include <stdbool.h>

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/event.h>
#include <sys/sysctl.h>

#include <netinet/in.h>

#include <arpa/inet.h>

#define ATOMIC_LOAD(x) __atomic_load_n(&(x), __ATOMIC_SEQ_CST)
#define ATOMIC_STORE(x, y) __atomic_store_n(&(x), (y), __ATOMIC_SEQ_CST)

struct peer {
    struct sockaddr_storage sockaddr;
    union {
        void *raw_addr_ptr;
        struct in_addr *ip4_addr_ptr;
        struct in6_addr *ip6_addr_ptr;
    }; // Points into sockaddr.
    int fd;
    in_port_t port;
    bool is_connected;
    char ascii_addr[INET6_ADDRSTRLEN];
    char __pad[3];
};

struct worker_data {
    int active_conn_kqueue;
    int num_events;
};

static noreturn void fatal(const char *msg);
static noreturn void fatal2(const char *msg);
static noreturn void fatal_strerror(const char *msg, int err);
static noreturn void fatal_gai(const char *msg, int err);

static int str_to_int(const char *str, int base);

static int get_num_cpus(void);
static int get_somaxconn(void);

static void *worker_thread(void *data);

static void accept_conn(int fd, struct peer *peer);

static void add_to_kqueue(int fd, uintptr_t ident, int16_t filter, uint16_t flags,
                          uint32_t fflags, intptr_t data, void *udata);

static int get_free_peer(struct peer *const *peer_ptrs, int max, int hint);

int main(int argc, const char *argv[])
{
    if (argc < 2)
        fatal2("Missing port number as argument");

    int somaxconn = get_somaxconn();
    int backlog = somaxconn;
    if (argc == 3) {
        backlog = str_to_int(argv[2], 0);

        if (backlog == 0)
            fatal2("Backlog has to be bigger than 0.\n");

        if (backlog > somaxconn) {
            fprintf(stderr, "Backlog exceeds kern.ipc.somaxconn, truncating...\n");
            backlog = somaxconn;
        }
    }

    struct addrinfo hints = {
        .ai_flags = AI_ADDRCONFIG | AI_PASSIVE,
        .ai_family = PF_UNSPEC,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP
    };

    struct addrinfo *addrinfo;

    int err = getaddrinfo(NULL, argv[1], &hints, &addrinfo);
    if (err != 0)
        fatal_gai("getaddrinfo()", err);

    int sockfd = socket(addrinfo->ai_family, addrinfo->ai_socktype, addrinfo->ai_protocol);
    if (sockfd == -1)
        fatal("socket()");

    if (bind(sockfd, addrinfo->ai_addr, addrinfo->ai_addrlen) == -1)
        fatal("bind()");

    freeaddrinfo(addrinfo);

    if (listen(sockfd, backlog) == -1)
        fatal("listen()");

    int max_num_active_conn = backlog;

    struct peer **peer_ptrs = calloc((size_t)max_num_active_conn, sizeof(*peer_ptrs));
    if (peer_ptrs == NULL)
        fatal("calloc()");

    for (int i = 0; i < max_num_active_conn; ++i) {
        peer_ptrs[i] = malloc(sizeof(*peer_ptrs[i]));
        if (peer_ptrs[i] == NULL)
            fatal("malloc()");

        peer_ptrs[i]->is_connected = false;
    }

    int worker_queue_fd = kqueue();
    if (worker_queue_fd == -1)
        fatal("kqueue()");

    struct worker_data worker_data = {
        .active_conn_kqueue = worker_queue_fd,
        .num_events = max_num_active_conn
    };

    int num_threads = get_num_cpus();

    pthread_t *threads = calloc((size_t)num_threads, sizeof(*threads));
    if (threads == NULL)
        fatal("calloc()");

    for (int i = 0; i < num_threads; ++i) {
        err = pthread_create(&threads[i], NULL, worker_thread, &worker_data);
        if (err != 0)
            fatal_strerror("pthread_create()", err);

        err = pthread_detach(threads[i]);
        if (err != 0)
            fatal_strerror("pthread_detach()", err);
    }

    if (signal(SIGINT, SIG_IGN) == SIG_ERR)
        fatal("signal()");
    if (signal(SIGTERM, SIG_IGN) == SIG_ERR)
        fatal("signal()");

    int accept_queue_fd = kqueue();
    if (accept_queue_fd == -1)
        fatal("kqueue()");

    add_to_kqueue(accept_queue_fd, SIGINT, EVFILT_SIGNAL, EV_ADD, 0, 0, NULL);
    add_to_kqueue(accept_queue_fd, SIGTERM, EVFILT_SIGNAL, EV_ADD, 0, 0, NULL);
    add_to_kqueue(accept_queue_fd, (uintptr_t)sockfd, EVFILT_READ, EV_ADD, 0, 0, NULL);

    fprintf(stderr, "[INFO] Waiting for incoming connections...\n");

    for (int idx = 0; ;) {
        struct kevent revents[3];
        int events_triggered = kevent(accept_queue_fd, NULL, 0, revents, 3, NULL);
        if (events_triggered == -1)
            fatal("kevent()");

        for (int i = 0; i < events_triggered; ++i) {
            if (revents[i].flags & EV_ERROR)
                fatal_strerror("Error processing events", (int)revents[i].data);

            if (revents[i].filter == EVFILT_SIGNAL) {
                fprintf(stderr, "[INFO] Got signal #%d.\n", (int)revents[i].ident);

                for (int j = 0; j < num_threads; ++j)
                    pthread_cancel(threads[j]);

                fprintf(stderr, "[INFO] Shutting down...\n");

                for (int j = 0; j < max_num_active_conn; ++j)
                    if (peer_ptrs[j]->is_connected)
                        shutdown(peer_ptrs[j]->fd, SHUT_RDWR);

                return 0;
            }

            // Got new connection

            assert(revents[i].filter == EVFILT_READ);
            assert((int)revents[i].ident == sockfd);

            struct peer *peer = peer_ptrs[idx];

            assert(!ATOMIC_LOAD(peer->is_connected));

            accept_conn(sockfd, peer);

            add_to_kqueue(worker_queue_fd, (uintptr_t)peer->fd, EVFILT_READ,
                          EV_ADD | EV_DISPATCH, 0, 0, peer);

            fprintf(stderr, "[INFO] New connection from %s:%u\n",
                    peer->ascii_addr, peer->port);

            idx = get_free_peer(peer_ptrs, max_num_active_conn, idx);
            if (idx != -1)
                continue;

            // No unconnected peers, reallocate...
            int old_size = max_num_active_conn;
            max_num_active_conn *= 2;

            peer_ptrs = reallocf(peer_ptrs, (size_t)max_num_active_conn);
            if (peer_ptrs == NULL)
                fatal("reallocf()");

            for (int j = old_size; j < max_num_active_conn; ++j) {
                peer_ptrs[j] = malloc(sizeof(*peer_ptrs[j]));
                if (peer_ptrs[j] == NULL)
                    fatal("malloc()");

                peer_ptrs[j]->is_connected = false;
            }

            idx = old_size;

            ATOMIC_STORE(worker_data.num_events, max_num_active_conn);
        }
    }
}

void *worker_thread(void *data)
{
    struct worker_data *info = data;
    int kqueue = info->active_conn_kqueue;
    int num_revents = info->num_events;

    struct kevent *revents = calloc((size_t)num_revents, sizeof(*revents));
    if (revents == NULL)
        fatal("calloc()");

    struct kevent *events = calloc((size_t)num_revents, sizeof(*events));
    if (events == NULL)
        fatal("calloc()");

    size_t bufsiz = 8192; // Should be big enough for most things
    unsigned char *buffer = malloc(bufsiz);
    if (buffer == NULL)
        fatal("malloc()");

    int re_add_event = 0;

    for (;;) {
        int events_recvd = kevent(kqueue, events, re_add_event,
                                  revents, num_revents, NULL);
        if (events_recvd == -1)
            fatal("kevent()");

        re_add_event = 0;

        for (int i = 0; i < events_recvd; ++i) {
            if (revents[i].flags & EV_ERROR)
                fatal_strerror("Error processing events", (int)revents[i].data);

            assert(revents[i].filter = EVFILT_READ);

            struct peer *peer = revents[i].udata;
            assert((int)revents[i].ident == peer->fd);

            size_t bytes_to_read = (size_t)revents[i].data;

            if (revents[i].data == 0 && revents[i].flags & EV_EOF) {
                fprintf(stderr, "[INFO] Lost connection with %s:%u\n",
                        peer->ascii_addr, peer->port);

                if (close(peer->fd) == -1)
                    fatal("close()");

                ATOMIC_STORE(peer->is_connected, false);

                continue;
            }

            fprintf(stderr, "[INFO] %s:%u sent %zu bytes\n", peer->ascii_addr,
                    peer->port, bytes_to_read);

            if (bytes_to_read > bufsiz) {
                bufsiz = bytes_to_read;
                buffer = reallocf(buffer, bufsiz);
                if (buffer == NULL)
                    fatal("reallocf()");
            }

            ssize_t bytes_read = read(peer->fd, buffer, bytes_to_read);
            if (bytes_read == -1)
                fatal("read()");

            fprintf(stderr, "[BEGIN DATA]\n");

            if (write(STDOUT_FILENO, buffer, (size_t)bytes_read) == -1)
                fatal("write()");

            fprintf(stderr, "\n[END DATA]\n");

            EV_SET(&events[re_add_event++], revents[i].ident, revents[i].filter,
                   EV_ENABLE, revents[i].fflags, revents[i].data, revents[i].udata);
        }

        int new_num_events = ATOMIC_LOAD(info->num_events);

        if (new_num_events != num_revents) {
            num_revents = new_num_events;

            revents = reallocf(revents, (size_t)num_revents);
            if (revents == NULL)
                fatal("reallocf()");

            events = reallocf(events, (size_t)num_revents);
            if (events == NULL)
                fatal("reallocf()");
        }
    }
}

void accept_conn(int fd, struct peer *peer)
{
    socklen_t size = sizeof(peer->sockaddr);
    int new_fd = accept(fd, (struct sockaddr *)&peer->sockaddr, &size);
    if (new_fd == -1)
        fatal("accept()");

    int af = peer->sockaddr.ss_family;
    switch (af) {
    case AF_INET: {
        struct sockaddr_in *addr = (void *)&peer->sockaddr;
        peer->ip4_addr_ptr = &addr->sin_addr;
        peer->port = addr->sin_port;
        break; }
    case AF_INET6: {
        struct sockaddr_in6 *addr = (void *)&peer->sockaddr;
        peer->ip6_addr_ptr = &addr->sin6_addr;
        peer->port = addr->sin6_port;
        break; }
    }

    inet_ntop(af, peer->raw_addr_ptr, peer->ascii_addr, sizeof(peer->ascii_addr));

    peer->is_connected = true;
    peer->fd = new_fd;
}

void add_to_kqueue(int fd, uintptr_t ident, int16_t filter, uint16_t flags,
                   uint32_t fflags, intptr_t data, void *udata)
{
    struct kevent event;
    EV_SET(&event, ident, filter, flags, fflags, data, udata);
    if (kevent(fd, &event, 1, NULL, 0, NULL) == -1)
        fatal("kevent()");
}

int get_free_peer(struct peer *const *peer_ptrs, int max, int hint)
{
    for (int i = hint; i < max; ++i)
        if (!ATOMIC_LOAD(peer_ptrs[i]->is_connected))
            return i;

    for (int i = 0; i < hint; ++i)
        if (!ATOMIC_LOAD(peer_ptrs[i]->is_connected))
            return i;

    return -1;
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

void fatal_strerror(const char *msg, int err)
{
    fprintf(stderr, "%s: %s\n", msg, strerror(err));
    exit(EXIT_FAILURE);
}

void fatal_gai(const char *msg, int err)
{
    fprintf(stderr, "%s: %s\n", msg, gai_strerror(err));
    exit(EXIT_FAILURE);
}

int str_to_int(const char *str, int base)
{
    char *endptr;
    long ret = strtol(str, &endptr, base);
    if (errno != 0)
        fatal("strtol()");
    if (ret > INT_MAX || ret < INT_MIN)
        fatal_strerror("strtol()", ERANGE);
    if (*endptr != '\0')
        fatal2("String to int conversion: encountered garbage");
    return (int)ret;
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
