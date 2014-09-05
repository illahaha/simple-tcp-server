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

#define KQUEUE_REALLOC 1

struct peer {
    union {
        struct in_addr in;
        struct in6_addr in6;
    } binary_addr;

    sa_family_t af;
    int fd;
    in_port_t port;
    bool is_connected;
    char ascii_addr[INET6_ADDRSTRLEN];
};

struct worker_data {
    int kqueue;
    int num_events;
};

static noreturn void fatal(const char *msg);
static noreturn void fatal2(const char *msg);
static noreturn void fatal_strerror(const char *msg, int err);
static noreturn void fatal_gai(const char *msg, int err);

static int str_to_int(const char *str);

static int get_num_logical_cpus(void);
static int get_somaxconn(void);

static void *worker_thread(void *data);

static void accept_conn(int fd, struct peer *peer);

static void add_to_kqueue(int fd, uintptr_t ident, int16_t filter, uint16_t flags,
                          uint32_t fflags, intptr_t data, void *udata);

static int get_free_peer(struct peer *const *peer_ptrs, int max, int hint);

int main(int argc, const char *argv[])
{
    if (argc < 2 || strcmp(argv[1], "help") == 0)
        fatal2("Usage: <port> (optional) <# threads>");

    int num_threads = get_num_logical_cpus();

    if (argv[2] != NULL) {
        num_threads = str_to_int(argv[2]);

        if (num_threads == 0)
            fatal2("Invalid number of threads, has to be 1 or more");
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

    int somaxconn = get_somaxconn();

    if (listen(sockfd, somaxconn) == -1)
        fatal("listen()");

    struct peer **peer_ptrs = calloc((size_t)somaxconn, sizeof(*peer_ptrs));
    if (peer_ptrs == NULL)
        fatal("calloc()");

    for (int i = 0; i < somaxconn; ++i) {
        peer_ptrs[i] = malloc(sizeof(*peer_ptrs[i]));
        if (peer_ptrs[i] == NULL)
            fatal("malloc()");

        peer_ptrs[i]->is_connected = false;
    }

    pthread_t *threads = calloc((size_t)num_threads, sizeof(*threads));
    if (threads == NULL)
        fatal("calloc()");

    struct worker_data *worker_data = calloc((size_t)num_threads, sizeof(*worker_data));
    if (worker_data == NULL)
        fatal("calloc()");

    int *kqueues = calloc((size_t)num_threads, sizeof(*kqueues));
    if (kqueues == NULL)
        fatal("calloc()");

    for (int i = 0; i < num_threads; ++i) {
        kqueues[i] = kqueue();
        if (kqueues[i] == -1)
            fatal("kqueue()");

        add_to_kqueue(kqueues[i], KQUEUE_REALLOC, EVFILT_USER, EV_ADD | EV_DISABLE, 0, 0, NULL);

        worker_data[i] = (struct worker_data){
            .kqueue = kqueues[i],
            .num_events = somaxconn / num_threads
        };

        err = pthread_create(&threads[i], NULL, worker_thread, &worker_data[i]);
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

    for (int idx = 0, curr_thread_idx = 0; ;) {
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

                for (int j = 0; j < somaxconn; ++j)
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

            add_to_kqueue(kqueues[curr_thread_idx++], (uintptr_t)peer->fd,
                          EVFILT_READ, EV_ADD, 0, 0, peer);

            if (curr_thread_idx == num_threads)
                curr_thread_idx = 0;

            fprintf(stderr, "[INFO] New connection from %s:%u\n",
                    peer->ascii_addr, peer->port);

            idx = get_free_peer(peer_ptrs, somaxconn, idx);
            if (idx != -1)
                continue;

            // No unconnected peers, reallocate...
            int old_size = somaxconn;
            somaxconn *= 2;

            peer_ptrs = reallocf(peer_ptrs, (size_t)somaxconn);
            if (peer_ptrs == NULL)
                fatal("reallocf()");

            for (int j = old_size; j < somaxconn; ++j) {
                peer_ptrs[j] = malloc(sizeof(*peer_ptrs[j]));
                if (peer_ptrs[j] == NULL)
                    fatal("malloc()");

                peer_ptrs[j]->is_connected = false;
            }

            idx = old_size;

            for (int j = 0; j < num_threads; ++j)
                add_to_kqueue(kqueues[j], KQUEUE_REALLOC, EVFILT_USER, EV_ENABLE, NOTE_TRIGGER, 0, NULL);
        }
    }
}

void *worker_thread(void *data)
{
    struct worker_data *info = data;
    int kqueue = info->kqueue;
    int num_revents = info->num_events;

    size_t pagesize = (size_t)getpagesize();

    struct kevent *revents = calloc((size_t)num_revents, sizeof(*revents));
    if (revents == NULL)
        fatal("calloc()");

    size_t bufsiz = pagesize;
    unsigned char *buffer = malloc(bufsiz);
    if (buffer == NULL)
        fatal("malloc()");

    for (;;) {
        int events_recvd = kevent(kqueue, NULL, 0, revents, num_revents, NULL);
        if (events_recvd == -1)
            fatal("kevent()");

        for (int i = 0; i < events_recvd; ++i) {
            if (revents[i].flags & EV_ERROR)
                fatal_strerror("Error processing events", (int)revents[i].data);

            if (revents[i].filter == EVFILT_USER && revents[i].ident == KQUEUE_REALLOC) {
                num_revents *= 2;

                revents = reallocf(revents, (size_t)num_revents);
                if (revents == NULL)
                    fatal("reallocf()");

                add_to_kqueue(kqueue, KQUEUE_REALLOC, EVFILT_USER, EV_DISABLE, 0, 0, NULL);

                continue;
            }

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
                bufsiz = bytes_to_read + (pagesize - bytes_to_read % pagesize);
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
        }
    }
}

void accept_conn(int fd, struct peer *peer)
{
    struct sockaddr_storage sockaddr;
    socklen_t size = sizeof(sockaddr);
    int new_fd = accept(fd, (struct sockaddr *)&sockaddr, &size);
    if (new_fd == -1)
        fatal("accept()");

    int af = sockaddr.ss_family;
    switch (af) {
    case AF_INET: {
        struct sockaddr_in *x = (struct sockaddr_in *)&sockaddr;
        peer->binary_addr.in = x->sin_addr;
        peer->port = x->sin_port;
        break; }
    case AF_INET6: {
        struct sockaddr_in6 *x = (struct sockaddr_in6 *)&sockaddr;
        peer->binary_addr.in6 = x->sin6_addr;
        peer->port = x->sin6_port;
        break; }
    }

    if (inet_ntop(af, &peer->binary_addr, peer->ascii_addr, sizeof(peer->ascii_addr)) == NULL)
        fatal("inet_ntop()");

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
    fprintf(stderr, "Error: ");
    perror(msg);
    exit(EXIT_FAILURE);
}

void fatal2(const char *msg)
{
    fprintf(stderr, "Error: %s\n", msg);
    exit(EXIT_FAILURE);
}

void fatal_strerror(const char *msg, int err)
{
    fprintf(stderr, "Error: %s: %s\n", msg, strerror(err));
    exit(EXIT_FAILURE);
}

void fatal_gai(const char *msg, int err)
{
    fprintf(stderr, "Error: %s: %s\n", msg, gai_strerror(err));
    exit(EXIT_FAILURE);
}

int str_to_int(const char *str)
{
    char *endptr;
    long ret = strtol(str, &endptr, 0);
    if (errno != 0)
        fatal("strtol()");
    if (ret > INT_MAX || ret < INT_MIN)
        fatal_strerror("strtol()", ERANGE);
    if (*endptr != '\0')
        fatal2("String to int conversion: encountered garbage");
    return (int)ret;
}

int get_num_logical_cpus(void)
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
