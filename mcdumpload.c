/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  mcdumpload - dump cache data and send it somewhere else
 *
 *       https://github.com/dormando/mcdumpload
 *
 *  Copyright 2023 Cache Forge LLC.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      dormando <dormando@rydia.net>
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <sys/time.h>
#include <fcntl.h>

#include <poll.h>

static int dump_connect(char *host, char *port) {
    int s;
    int sock = -1;
    struct addrinfo hints;
    struct addrinfo *ai;
    struct addrinfo *next;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    s = getaddrinfo(host, port, &hints, &ai);

    if (s != 0) {
        hints.ai_family = AF_INET6;
        s = getaddrinfo(host, port, &hints, &ai);
        if (s != 0) {
            goto end;
        }
    }

    for (next = ai; next != NULL; next = next->ai_next) {
        sock = socket(next->ai_family, next->ai_socktype,
                next->ai_protocol);
        if (sock == -1)
            continue;

        if (connect(sock, next->ai_addr, next->ai_addrlen) == -1) {
            perror("Failed to connect");
            close(sock);
            goto end;
        }

        int flags = fcntl(sock, F_GETFL);
        if (flags < 0) {
            close(sock);
            goto end;
        }
        // set nonblock after we connect to simplify the connection process.
        if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
            perror("Failed to set nonblock");
            close(sock);
            goto end;
        }

        break;
    }

    if (next == NULL) {
        goto end;
    }

end:
    if (ai) {
        freeaddrinfo(ai);
    }
    // FIXME: return -1 if failed to connect.
    return sock;
}

// TODO: check for POLLHUP
static void poll_for(struct pollfd *to_poll, int idx, int flag) {
    to_poll[idx].events = flag;
    while (1) {
        int ret = poll(to_poll, 3, 1000);
        if (ret == 0)
            continue; // timeout
        if (to_poll[idx].revents & flag) {
            return;
        }
    }
}

struct mcdump_stats {
    uint64_t not_stored;
    uint64_t server_error;
    uint64_t requests;
    struct timeval next_check;
};

struct mcdump_conf {
    char source_host[NI_MAXHOST];
    char source_port[NI_MAXSERV];
    char dest_host[NI_MAXHOST];
    char dest_port[NI_MAXSERV];
    char key_host[NI_MAXHOST];
    char key_port[NI_MAXSERV];
    uint32_t dest_bwlimit; // kilobytes per timeslice
    uint32_t src_reqlimit; // requests per timeslice
    uint32_t read_bufsize; // megabytes
    int use_add; // use add instead of set
    int show_stats; // print periodic stats
    int dest_conns; // how many destination sockets to make
    struct mcdump_filter *filter;
};

struct mcdump_limiter {
    uint32_t limit; // kilobytes per timeslice
    int32_t remain; // bytes remaining for this next window
    uint32_t reqlimit; // requests per timeslice
    int32_t reqremain; // requests remaining for this next window
    struct timeval window_end; // the end of the next window
};

struct mcdump_buf {
    char *buf;
    int size;
    int filled;
    int consumed;
    int parsed; // used just for the data transformer
    int pending; // for pipeline rate limiting of requests out/in
    int complete; // 0 or 1 to indicate if this stream has completed.
    int want_readpoll;
    int want_writepoll;
    struct timeval last_check; // for time based polling.
};

#define FILTER_MAX_LEN 500
#define FILTER_MODE_INCLUDE 1
#define FILTER_MODE_EXCLUDE 0
struct mcdump_filter {
    char filter[FILTER_MAX_LEN];
    int len;
    int mode; // 0 exclude, 1 include
    struct mcdump_filter *next; // next key filter to test.
};

// if window is zero and remain is zero, just sleep for window length
// if window and remain is <= 0, sleep until window and reset window
// if window and remain is <= 0 and window is in the past, just set window
#define WINDOW_LENGTH 100000 // 1/10th of a sec
static void run_bwlimit(struct mcdump_limiter *l, int sent) {
    if (l->limit == 0)
        return;

    l->remain -= sent;
}

static void run_reqlimit(struct mcdump_limiter *l, int requests) {
    if (l->reqlimit == 0)
        return;

    l->reqremain -= requests;
}

static void run_limiter(struct mcdump_limiter *l) {
    struct timeval now;
    struct timeval toadd = {.tv_sec = 0, .tv_usec = WINDOW_LENGTH};
    int wait = 0;

    if (l->limit != 0 && l->remain <= 0) {
        wait = 1;
    }
    if (l->reqlimit != 0 && l->reqremain <= 0) {
        wait = 1;
    }

    if (wait == 0)
        return;

    gettimeofday(&now, NULL);

    if (l->remain <= 0 || l->reqlimit <= 0) {
        l->remain = l->limit;
        l->reqremain = l->reqlimit;
        if (l->window_end.tv_sec == 0 && l->window_end.tv_usec == 0) {
            usleep(WINDOW_LENGTH);
            gettimeofday(&now, NULL);
            timeradd(&now, &toadd, &l->window_end);
        } else {
            if (timercmp(&now, &l->window_end, >)) {
                // current time is past the end of the window, set a new
                // window and return without sleeping.
                timeradd(&now, &toadd, &l->window_end);
            } else {
                struct timeval tosleep = {0};
                timersub(&l->window_end, &now, &tosleep);
                // should never be higher than a second...
                usleep(tosleep.tv_usec);
                // reset window to future
                gettimeofday(&now, NULL);
                timeradd(&now, &toadd, &l->window_end);
            }
        }
    }
}

#define MGDUMP_START "lru_crawler mgdump hash\r\n"
// return the key, client flags, TTL remaining, value
// quiet flag to skip EN\r\n's so we don't have to handle them.
// u flag to not bump the LRU for all the items we're fetching.
// Adds the O flag to be replaced by new flags so we can have the
// string line up.
#define MGDUMP_FLAGS " k f t v q u Ooooo\r\n"
#define KEYSBUF_SIZE 65536
#define DESTINBUF_SIZE 65536
#define DATA_WRITEBUF_SIZE KEYSBUF_SIZE * 2
#define DATA_READBUF_DEFAULT (1024 * 1024 * 2)
#define MIN_KEYOUT_SPACE 1024
#define PIPELINES_DEFAULT 32

// apply rate limiter here?
// would allow avoiding memove of data_cmds buf since we just wait for it to
// drain
static void keylist_to_requests(struct mcdump_buf *keys, struct mcdump_buf *requests,
        struct mcdump_limiter *l, struct mcdump_filter *f) {
    // loop:
    // memchr to find \n in keys.buf + consumed
    // if found, and enough space in data_cmds.buf + filled, copy + add flags
    // if not found or space full, advance dump consumed
    int limit = PIPELINES_DEFAULT;
    int count = 0;
    if (l->reqlimit != 0 && l->reqremain < limit) {
        limit = l->reqremain;
    }
    for (int x = 0; x < PIPELINES_DEFAULT; x++) {
        char *start = keys->buf + keys->consumed;
        char *end = memchr(keys->buf + keys->consumed, '\n',
                        keys->filled - keys->consumed);
        if (end == NULL) {
            if (keys->consumed != 0 && keys->filled != 0) {
                memmove(keys->buf, keys->buf+keys->consumed,
                        keys->filled - keys->consumed);
                keys->filled -= keys->consumed;
                keys->consumed = 0;
            }
            break;
        }

        size_t len = end - start;
        // ensure space in data_cmds.
        if (requests->size - requests->filled < MIN_KEYOUT_SPACE) {
            break;
        }

        if (len > 2 && strncmp(start, "EN\r\n", 4) == 0) {
            memmove(keys->buf, start, 4);
            keys->filled = 4;
            keys->consumed = 0;
            // Toss a nop to cap off the key write
            if (keys->complete == 0) {
                char *data = requests->buf + requests->filled;
                memcpy(data, "mn\r\n", 4);
                requests->filled += 4;
                keys->complete = 1;
            }
            break;
        }

        char *sdata;
        char *data = sdata = requests->buf + requests->filled;

        struct mcdump_filter *cf = f;
        // the first filter given sets the default include or exclude.
        int use_key = 1;
        if (cf) {
            if (cf->mode == FILTER_MODE_INCLUDE) {
                use_key = 0; // key must be included.
            } else {
                use_key = 1;
            }
        }
        // TODO: Need temporary buffer and base64 decoder to filter binary.
        while (cf) {
            if (strncmp(start + 3, cf->filter, cf->len) == 0) {
                if (cf->mode == FILTER_MODE_INCLUDE) {
                    use_key = 1;
                } else if (cf->mode == FILTER_MODE_EXCLUDE) {
                    use_key = 0;
                }
            }

            cf = cf->next;
        }

        if (use_key) {
            // copy base command but don't copy the \r\n
            memcpy(data, start, len-1);
            keys->consumed += len+1; // get the \n
            data += len-1;

            memcpy(data, MGDUMP_FLAGS, sizeof(MGDUMP_FLAGS)-1);
            data += sizeof(MGDUMP_FLAGS)-1;

            requests->filled += data - sdata;
            requests->pending++;
            count++;
            //fprintf(stderr, "Copied command: %.*s\n", (int)(data - sdata)-1, sdata);
        } else {
            // ignore this key and move on.
            keys->consumed += len+1;
            // give another chance to pipeline something else.
            x--;
        }

        // ran out of keys to copy.
        if (keys->consumed == keys->filled) {
            keys->consumed = 0;
            keys->filled = 0;
            break;
        }
    }
    run_reqlimit(l, count);
}

#define TEMP_SIZE 512

static int convert_data_in(struct mcdump_buf *data_in, int use_add) {
    char temp[TEMP_SIZE];
    int temp_len = 0;
    int parsed = data_in->parsed;
    int converted = 0;
    if (data_in->complete) {
        return 0;
    }
    if (data_in->filled < data_in->parsed) {
        abort();
    }

    while (1) {
        char *np;
        char *p;
        // sorry :)
        char *start = p = np = data_in->buf + parsed;
        char *end = memchr(start, '\n', data_in->filled - parsed);
        if (end - data_in->buf > data_in->filled) {
            abort();
        }
        if (end == NULL) {
            break;
        }

        size_t len = end - start;
        if (len > 2 && strncmp(start, "VA ", 3) != 0) {
            // This nop is the finalizer from the key dumper, now we've seen
            // every possible response.
            if (strncmp(start, "MN\r\n", 4) == 0) {
                data_in->complete = 1;
                data_in->filled -= 4; // chop off the MN\r\n
                break;
            }
            fprintf(stderr, "ERROR: bad response from data dump: %s\n", start);
            abort();
        }

        // VA 1 kfoo18 f0 t-1\r\ndata\r\n
        // ms foo18 1 F0 T0  \r\ndata\r\n
        char *n = NULL;
        p += 3;
        uint32_t vsize = strtoul(p, &n, 10);
        if ((errno == ERANGE) || (p == n)) {
            fprintf(stderr, "ERROR: bad response line from data dump: %s\n", start);
            abort();
        }
        vsize += 2; // add \r\n
        // ensure we have the whole value read before modifying the command.
        if (len + vsize + 1 > data_in->filled - parsed) {
            break;
        }

        // hold onto the unparsed number
        temp_len = n - p;
        if (temp_len > TEMP_SIZE) {
            fprintf(stderr, "ERROR: wildly long value length: %s\n", start);
            abort();
        }
        memcpy(temp, p, n - p);

        p = n;

        // first part of conversion, convert "VA " to "ms "
        memcpy(np, "ms ", 3);
        np += 3;

        // find the start of the key
        while (*p != 'k') {
            p++;
        }
        p++; // skip the 'k' part

        // copy the key over (slowish)
        // FIXME: also check for \n for safety?
        while (*p != ' ') {
            *np = *p;
            np++;
            p++;
        }
        *np = ' ';
        np++;

        // copy back the length number
        memcpy(np, temp, temp_len);
        np += temp_len;

        *np = ' ';
        np++;
        p++; // skip the space after key

        if (*p != 'f') {
            fprintf(stderr, "ERROR: missing client flag: %s\n", start);
            abort();
        }
        *np = 'F';
        np++;
        p++;

        while (*p != ' ') {
            *np = *p;
            np++;
            p++;
        }

        *np = *p;
        np++;
        p++;

        if (*p != 't') {
            fprintf(stderr, "ERROR: missing TTL flag: %s\n", start);
            abort();
        }

        if (strncmp(p, "t-1", 3) == 0) {
            memcpy(np, "T0 ", 3);
            np += 3;
            p += 3;
        } else {
            *np = 'T';
            np++;
            p++;
            while (*p != ' ') {
                *np = *p;
                np++;
                p++;
            }

            *np = *p;
            np++;
            p++;
        }

        // we should have enough space left for "q " then "\r\n"
        if (end - np < 4) {
            fprintf(stderr, "ERROR: not enough space left to append q flag: %s\n", start);
            abort();
        }

        *np = 'q';
        np++;
        *np = ' ';
        np++;

        if (end - np < 4) {
            fprintf(stderr, "ERROR: not enough space left to append M flag: %s\n", start);
            abort();
        }

        *np = 'M';
        np++;
        if (use_add) {
            *np = 'E';
        } else {
            *np = 'S';
        }
        np++;
        while (*np != '\r') {
            *np = ' '; // blank out any remaining characters
            np++;
        }

        // done...
        parsed += end - start + 1; // include the \n
        parsed += vsize;
        converted++;

        if (parsed >= data_in->filled)
            break;
    }

    data_in->parsed = parsed;
    if (data_in->filled < data_in->parsed) {
        abort();
    }
    return converted;
}

static void run_keys(int keys_fd, struct mcdump_buf *keys) {
    // No space to continue reading.
    keys->want_readpoll = 0;
    if (keys->filled == keys->size) {
        return;
    }

    // Stream completed? Though we shouldn't be continuing to get read events
    if (keys->filled > 3) {
        if (strncmp(keys->buf + keys->filled - 4, "EN\r\n", 4) == 0) {
            return;
        }
    }

    int readb;
    if (keys_fd == 0) {
        readb = read(keys_fd, keys->buf + keys->filled, keys->size - keys->filled);
    } else {
        readb = recv(keys_fd, keys->buf + keys->filled, keys->size - keys->filled, 0);
    }
    if (readb == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            keys->want_readpoll = 1;
        } else {
            fprintf(stderr, "ERROR: key list connection closed\n");
            exit(1);
        }
    } else {
        keys->filled += readb;
    }
}

static void run_requests_out(int reqs_fd, struct mcdump_buf *keys, struct mcdump_buf *requests,
        struct mcdump_limiter *l, struct mcdump_filter *f) {
    // Flush previous buffer and read responses before requesting more
    // TODO: when enforced, waits for all responses before sending more
    // requests, but seems to 8x the CPU usage of the dumploader.
    //if (requests->filled <= requests->consumed && requests->pending == 0) {
    if (requests->filled <= requests->consumed) {
        // No key data to work with right now.
        if (keys->filled <= keys->consumed) {
            return;
        }

        // Fill new request data.
        keylist_to_requests(keys, requests, l, f);
    }

    if (requests->filled > requests->consumed) {
        requests->want_writepoll = 0;
        requests->want_readpoll = 0;
        int sent = send(reqs_fd, requests->buf + requests->consumed, requests->filled - requests->consumed, 0);
        if (sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                requests->want_writepoll = 1;
                requests->want_readpoll = 1; // read queue may be full.
            } else {
                fprintf(stderr, "ERROR: source connection closed\n");
                exit(1);
            }
        } else {
            requests->consumed += sent;
            // We've sent the full buffer.
            if (requests->consumed == requests->filled) {
                requests->consumed = 0;
                requests->filled = 0;
            }
        }
    }
}

static void run_responses_in(int src_fd, struct mcdump_buf *responses,
        struct mcdump_buf *requests, struct mcdump_stats *stats, int use_add) {
    // if space, read as much as we can
    if (responses->filled < responses->size) {
        responses->want_readpoll = 0;
        int read = recv(src_fd, responses->buf + responses->filled, responses->size - responses->filled, 0);
        if (read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                responses->want_readpoll = 1;
            } else {
                fprintf(stderr, "ERROR: data source connection closed\n");
                exit(1);
            }
        } else {
            responses->filled += read;
            int converted = convert_data_in(responses, use_add);
            requests->pending -= converted;
            stats->requests += converted;
        }
    }
}

// move converted responses from the resp buffer to destination buffer
static void move_responses(struct mcdump_buf *responses, struct mcdump_buf *dest_outs, int dest_conns) {
    for (int x = 0; x < dest_conns; x++) {
        struct mcdump_buf *dest_out = &dest_outs[x];
        if (responses->parsed <= responses->consumed) {
            break;
        }
        if (dest_out->filled >= dest_out->size) {
            continue;
        }

        int tocopy = responses->parsed - responses->consumed;
        int remain = dest_out->size - dest_out->filled;
        if (tocopy > remain) {
            // can only safely pull in responses if there's enough room to fit
            // all of them. Or else we have to scan to ensure we don't end in
            // the middle of a response.
            continue;
        }

        memcpy(dest_out->buf + dest_out->filled,
               responses->buf + responses->consumed,
               tocopy);

        responses->consumed += tocopy;
        dest_out->filled += tocopy;
    }

    if (responses->consumed == responses->parsed) {
        // shift a partial buffer.
        if (responses->parsed < responses->filled) {
            memmove(responses->buf, responses->buf+responses->parsed,
                    responses->filled - responses->consumed);
            responses->filled = responses->filled - responses->consumed;
        } else {
            responses->filled = 0;
        }
        responses->consumed = 0;
        responses->parsed = 0;
    }
}

static void run_dest_out(int *dest_fds, struct mcdump_buf *dest_outs, int dest_conns,
        struct mcdump_limiter *bwlimit) {
    for (int x = 0; x < dest_conns; x++) {
        int dest_fd = dest_fds[x];
        struct mcdump_buf *dest_out = &dest_outs[x];
        // anything to write out?
        if (dest_out->filled <= dest_out->consumed) {
            continue;
        }

        int tosend = dest_out->filled - dest_out->consumed;
        if (bwlimit->limit != 0 && bwlimit->remain < tosend) {
            tosend = bwlimit->remain;
        }

        dest_out->want_writepoll = 0;
        int sent = send(dest_fd, dest_out->buf + dest_out->consumed, tosend, 0);
        if (sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                dest_out->want_writepoll = 1;
            } else {
                fprintf(stderr, "ERROR: destination connection closed\n");
                exit(1);
            }
        } else {
            dest_out->consumed += sent;
            run_bwlimit(bwlimit, sent);
        }

        if (dest_out->consumed == dest_out->filled) {
            dest_out->consumed = 0;
            dest_out->filled = 0;
        }
    }
}

static int run_dest_in(int dest_fd, struct mcdump_buf *dest_in, struct mcdump_stats *stats) {
    while (1) {
        int read = recv(dest_fd, dest_in->buf + dest_in->filled, dest_in->size - dest_in->filled, 0);
        if (read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // nothing to read right now.
                return -1;
            } else {
                fprintf(stderr, "ERROR: destination connection closed\n");
                exit(1);
            }
        }

        dest_in->filled += read;
        // TODO: how to "expect" the actual end of the stream?
        // else we silently bail here if there's a missing newline but garbage on
        // the line.
        // add "expect_complete" flag argument?
        // if read == 0 and something in the buffer and it's not MN print and die.
        while (1) {
            char *start = dest_in->buf;
            char *end = memchr(dest_in->buf, '\n', dest_in->filled);
            if (end == NULL) {
                break;
            }

            int len = end - start + 1;

            // We don't need to pre-check the length since we only look at the
            // start of the buffer and it will always be longer than the test
            // string.
            if (strncmp(start, "NS\r\n", len) == 0) {
                stats->not_stored++;
            } else if (strncmp(start, "SERVER_ERROR", len) == 0) {
                fprintf(stderr, "%.*s", len, start);
                stats->server_error++;
            } else if (strncmp(start, "CLIENT_ERROR", len) == 0) {
                fprintf(stderr, "%.*s", len, start);
                fprintf(stderr, "ERROR: received CLIENT_ERROR from destination, protocol is out of sync and must stop\n");
                exit(1);
            } else if (strncmp(start, "ERROR", len) == 0) {
                fprintf(stderr, "%.*s", len, start);
                fprintf(stderr, "ERROR: received ERROR from destination, protocol is out of sync and must stop\n");
                exit(1);
            } else if (strncmp(start, "MN\r\n", len) == 0) {
                dest_in->complete = 1;
            } else {
                fprintf(stderr, "ERROR: Got unexpected response from destination: %.*s", len, start);
                exit(1);
            }
            memmove(dest_in->buf, end+1, dest_in->filled - len);
            dest_in->filled -= len;
        }
    }

    return 0;
}

static void check_dest_in(int dest_fd, struct mcdump_buf *dest_in, struct mcdump_stats *stats) {
    struct timeval now;

    gettimeofday(&now, NULL);
    if (dest_in->last_check.tv_sec != now.tv_sec) {
        dest_in->last_check = now;
        run_dest_in(dest_fd, dest_in, stats);
    }
}

static void check_stats(struct mcdump_stats *stats, struct mcdump_stats *stats2) {
    struct timeval now;
    struct mcdump_stats s = {0};
    gettimeofday(&now, NULL);
    if (now.tv_sec <= stats->next_check.tv_sec) {
        return;
    }

    s.not_stored = stats->not_stored - stats2->not_stored;
    s.server_error = stats->server_error - stats2->server_error;
    s.requests = stats->requests - stats2->requests;

    fprintf(stderr, "===STATS=== NS: [%llu] SERVER_ERROR: [%llu] REQUESTS: [%llu]\n",
            (unsigned long long)s.not_stored,
            (unsigned long long)s.server_error,
            (unsigned long long)s.requests);

    stats->next_check = now;
    *stats2 = *stats;
}

static int is_work_complete(struct mcdump_buf *keys,
                            struct mcdump_buf *responses,
                            struct mcdump_buf *keys_out,
                            struct mcdump_buf *dest_outs,
                            int dest_conns) {
    if (keys->complete == 1) {
        if (responses->complete == 1 && responses->filled == 0) {
            for (int x = 0; x < dest_conns; x++) {
                if (dest_outs[x].filled != 0) {
                    return 0;
                }
            }
            return 1;
        }
    }

    return 0;
}

#define POLL_KEYS 0
#define POLL_SOURCE 1
#define POLL_DEST 2

#define MAXDESTS 8
#define PCNT MAXDESTS + 3

static int run_dump(struct mcdump_conf *conf) {
    int dest_conns = conf->dest_conns;
    struct pollfd to_poll[PCNT] = {0};
    // for reading the list of keys
    struct mcdump_buf keys_buf = {0};
    // for holding requests converted from the key list
    struct mcdump_buf requests_buf = {0};
    // for reading responses back from the source connection
    struct mcdump_buf responses_buf = {0};
    // for reading responses from the destination connection.
    struct mcdump_buf dest_in_bufs[MAXDESTS] = {0};
    // for holding converted responses for destination conn.
    struct mcdump_buf dest_out_bufs[MAXDESTS] = {0};
    struct mcdump_limiter bwlimit = {0};
    struct mcdump_stats stats = {0};
    struct mcdump_stats stats2 = {0};

    gettimeofday(&stats.next_check, NULL);
    bwlimit.limit = conf->dest_bwlimit;
    bwlimit.reqlimit = conf->src_reqlimit;

    bwlimit.remain = bwlimit.limit; // seed the limiter.
    bwlimit.reqremain = bwlimit.reqlimit;

    int keys_fd;
    if (conf->key_host[0] == '-') {
        keys_fd = 0;
    } else {
        keys_fd = dump_connect(conf->key_host, conf->key_port);
    }
    int src_fd = dump_connect(conf->source_host, conf->source_port);
    int dest_fds[MAXDESTS];
    for (int x = 0; x < dest_conns; x++) {
        dest_fds[x] = dump_connect(conf->dest_host, conf->dest_port);
        to_poll[POLL_DEST + x].fd = dest_fds[x];
        to_poll[POLL_DEST + x].events = 0;
    }

    to_poll[POLL_KEYS].fd = keys_fd;
    to_poll[POLL_KEYS].events = 0;
    to_poll[POLL_SOURCE].fd = src_fd;
    to_poll[POLL_SOURCE].events = 0;

    keys_buf.buf = malloc(KEYSBUF_SIZE);
    keys_buf.size = KEYSBUF_SIZE;
    requests_buf.buf = malloc(DATA_WRITEBUF_SIZE);
    requests_buf.size = DATA_WRITEBUF_SIZE;
    responses_buf.buf = malloc(conf->read_bufsize);
    responses_buf.size = conf->read_bufsize;
    for (int x = 0; x < dest_conns; x++) {
        dest_in_bufs[x].buf = malloc(DESTINBUF_SIZE);
        dest_in_bufs[x].size = DESTINBUF_SIZE;
        dest_out_bufs[x].buf = malloc(conf->read_bufsize);
        dest_out_bufs[x].size = conf->read_bufsize;
    }

    while (1) {
        int sent = send(keys_fd, MGDUMP_START, sizeof(MGDUMP_START)-1, 0);
        if (sent < sizeof(MGDUMP_START)-1) {
            fprintf(stderr, "ERROR: Failed write for fetching key list\n");
            exit(EXIT_FAILURE);
        }
        poll_for(to_poll, POLL_KEYS, POLLIN);
        // Do a trial read.
        run_keys(keys_fd, &keys_buf);
        if (keys_buf.filled >= 4) {
            if (strncmp(keys_buf.buf, "BUSY", 4) == 0) {
                // reset the keys buffer and loop again.
                keys_buf.filled = 0;
                sleep(1);
                continue;
            } else if (strncmp(keys_buf.buf, "mg", 2) != 0) {
                fprintf(stderr, "ERROR: Unexpected response from key dump: %.*s\n",
                        keys_buf.filled, keys_buf.buf);
                exit(EXIT_FAILURE);
            }
            break;
        }
    }

    // core loop
    while (1) {
        int do_poll = 0;
        // fetch more keys to convert if we have room for it.
        if (requests_buf.size - requests_buf.filled > MIN_KEYOUT_SPACE) {
            run_keys(keys_fd, &keys_buf);
        }

        run_requests_out(src_fd, &keys_buf, &requests_buf, &bwlimit, conf->filter);
        run_responses_in(src_fd, &responses_buf, &requests_buf, &stats, conf->use_add);
        move_responses(&responses_buf, dest_out_bufs, dest_conns);
        run_dest_out(dest_fds, dest_out_bufs, dest_conns, &bwlimit);
        // periodically check in on the destination so we can give interactive
        // statistics/errors.
        for (int x = 0; x < dest_conns; x++) {
            check_dest_in(dest_fds[x], &dest_in_bufs[x], &stats);
        }
        if (conf->show_stats) {
            check_stats(&stats, &stats2);
        }

        // Set polling for next round.
        if (keys_buf.want_readpoll) {
            to_poll[POLL_KEYS].events = POLLIN;
            do_poll = 1;
        }
        if (requests_buf.want_writepoll) {
            to_poll[POLL_SOURCE].events |= POLLOUT;
            do_poll = 1;
        }
        if (requests_buf.want_readpoll) {
            to_poll[POLL_SOURCE].events |= POLLIN;
            do_poll = 1;
        }
        if (responses_buf.want_readpoll) {
            to_poll[POLL_SOURCE].events |= POLLIN;
            do_poll = 1;
        }
        int dests_full = 0;
        for (int x = 0; x < dest_conns; x++) {
            if (dest_out_bufs[x].want_writepoll) {
                to_poll[POLL_DEST+x].events = POLLIN|POLLOUT;
                dests_full++;
            }
        }
        if (dests_full == dest_conns) {
            do_poll = 1;
        }

        if (is_work_complete(&keys_buf, &responses_buf, &requests_buf, dest_out_bufs, dest_conns)) {
            if (conf->show_stats) {
                fprintf(stderr, "=== Source read completed\n");
            }
            break;
        }

        run_limiter(&bwlimit); // pause if necessary
        if (do_poll) {
            // TODO: check POLLHUP/etc
            int ret = poll(to_poll, 3+dest_conns, 1000);
            if (ret < 0) {
                abort();
            }
            // destination socket can block writes if we need to read.
            for (int x = 0; x < dest_conns; x++) {
                if (to_poll[POLL_DEST+x].revents & POLLIN) {
                    run_dest_in(dest_fds[x], &dest_in_bufs[x], &stats);
                }
            }
            for (int x = 0; x < PCNT; x++) {
                to_poll[x].events = 0;
            }
            if (ret == 0) // FIXME: just loop poll?
                continue; // timeout
        }
    }

    // Drop an endcap down each destination so we can know they've consumed
    // all of the converted requests.
    for (int x = 0; x < PCNT; x++) {
        to_poll[x].events = 0;
    }
    for (int x = 0; x < dest_conns; x++) {
        int sent = send(dest_fds[x], "mn\r\n", 4, 0);
        to_poll[POLL_DEST+x].events |= POLLIN;
        if (sent < 4) {
            fprintf(stderr, "Fuck\n");
            abort();
        }
    }

    while (1) {
        int complete = 0;
        for (int x = 0; x < dest_conns; x++) {
            run_dest_in(dest_fds[x], &dest_in_bufs[x], &stats);
            if (dest_in_bufs[x].complete) {
                complete++;
            }
        }
        if (complete == dest_conns) {
            fprintf(stderr, "Dumpload complete\n");
            break;
        } else {
            int ret = poll(to_poll, 3+dest_conns, 1000);
            if (ret < 0) {
                abort();
            }
        }
    }

    close(keys_fd);
    close(src_fd);

    return EXIT_SUCCESS;
}

static void usage(void) {
    printf("usage:\n"
           "--srcip=<addr> (127.0.0.1)\n"
           "--srcport=<port> (11211)\n"
           "--dstip=<addr> (no default)\n"
           "--dstport=<port> (no default)\n"
           "--keylistip=<addr> (srcip) ['-' for STDIN]\n"
           "--keylistport=<port> (srcport)\n"
           "--srcratelimit=<requests/sec> (0)\n"
           "--dstbwlimit=<kilobits/sec> (0)\n"
           "--readbufsize=<megabytes> (2)\n"
           "--keyinclude=<prefix> ("")\n"
           "--keyexclude=<prefix> ("")\n"
           "--add (use meta adds instead of sets)\n"
           "--stats (print some stats once per second to STDERR)\n"
          );
}

// TODO:
// - add a "stdout" mode
int main(int argc, char **argv) {
    struct mcdump_conf conf = {.source_host = "127.0.0.1", .source_port = "11211",
                               .key_host = "127.0.0.1", .key_port = "11211",
                               .dest_bwlimit = 0, .src_reqlimit = 0, .use_add = 0,
                               .read_bufsize = DATA_READBUF_DEFAULT,
                               .dest_conns = 1};
    int dest_host = 0;
    int dest_port = 0;
    int key_host = 0;
    int key_port = 0;
    struct mcdump_filter *filter = NULL;
    struct mcdump_filter *curfilter = NULL;
    const struct option longopts[] = {
        {"srcip", required_argument, 0, 'i'},
        {"srcport", required_argument, 0, 'p'},
        {"dstip", required_argument, 0, 'd'},
        {"dstport", required_argument, 0, 's'},
        {"dstconns", required_argument, 0, 'c'},
        {"keylistip", required_argument, 0, 'w'},
        {"keylistport", required_argument, 0, 'e'},
        {"dstbwlimit", required_argument, 0, 'l'},
        {"srcratelimit", required_argument, 0, 'r'},
        {"readbufsize", required_argument, 0, 'b'},
        {"keyinclude", required_argument, 0, 'k'},
        {"keyexclude", required_argument, 0, 'x'},
        {"add", no_argument, 0, 'a'},
        {"stats", no_argument, 0, 't'},
        {"help", no_argument, 0, 'h'},
        // end
        {0, 0, 0, 0}
    };
    int optindex;
    int c;
    while (-1 != (c = getopt_long(argc, argv, "", longopts, &optindex))) {
        switch (c) {
        case 'a':
            conf.use_add = 1;
            break;
        case 'i':
            strncpy(conf.source_host, optarg, NI_MAXHOST-1);
            if (key_host == 0) {
                strncpy(conf.key_host, optarg, NI_MAXHOST-1);
            }
            break;
        case 'p':
            strncpy(conf.source_port, optarg, NI_MAXSERV-1);
            if (key_port == 0) {
                strncpy(conf.key_port, optarg, NI_MAXSERV-1);
            }
            break;
        case 'd':
            strncpy(conf.dest_host, optarg, NI_MAXHOST-1);
            dest_host = 1;
            break;
        case 's':
            strncpy(conf.dest_port, optarg, NI_MAXSERV-1);
            dest_port = 1;
            break;
        case 'c':
            conf.dest_conns = atoi(optarg);
            if (conf.dest_conns > MAXDESTS) {
                fprintf(stderr, "ERROR: Limit of 8 destination connections\n");
                return EXIT_FAILURE;
            }
            break;
        case 'w':
            strncpy(conf.key_host, optarg, NI_MAXHOST-1);
            key_host = 1;
            break;
        case 'e':
            strncpy(conf.key_port, optarg, NI_MAXSERV-1);
            key_port = 1;
            break;
        case 'l':
            conf.dest_bwlimit = atoi(optarg);
            break;
        case 'r':
            conf.src_reqlimit = atoi(optarg);
            break;
        case 'b':
            conf.read_bufsize = atoi(optarg) * (1024 * 1024);
            break;
        case 'k':
            if (curfilter) {
                curfilter->next = calloc(1, sizeof(struct mcdump_filter));
                curfilter = curfilter->next;
            } else {
                filter = calloc(1, sizeof(struct mcdump_filter));
                curfilter = filter;
            }
            curfilter->len = strlen(optarg);
            curfilter->mode = FILTER_MODE_INCLUDE;
            strncpy(curfilter->filter, optarg, FILTER_MAX_LEN-1);
            break;
        case 'x':
            if (curfilter) {
                curfilter->next = calloc(1, sizeof(struct mcdump_filter));
                curfilter = curfilter->next;
            } else {
                filter = calloc(1, sizeof(struct mcdump_filter));
                curfilter = filter;
            }
            curfilter->len = strlen(optarg);
            curfilter->mode = FILTER_MODE_EXCLUDE;
            strncpy(curfilter->filter, optarg, FILTER_MAX_LEN-1);
            break;
        case 't':
            conf.show_stats = 1;
            break;
        case 'h':
            usage();
            return EXIT_SUCCESS;
            break;
        default:
            fprintf(stderr, "Unknown option\n");
            return EXIT_FAILURE;
        }
    }

    conf.filter = filter;

    if (dest_host == 0 || dest_port == 0) {
        fprintf(stderr, "Arguments require at least '--dstip and --dstport'\n");
        return EXIT_FAILURE;
    }

    if (conf.dest_bwlimit) {
        int64_t bits_sec = conf.dest_bwlimit * 1024;
        int64_t bytes_sec = bits_sec / 8;
        // ultimately bytes per time slice.
        conf.dest_bwlimit = bytes_sec / 10;
    }

    if (conf.src_reqlimit) {
        if (conf.src_reqlimit > 10) {
            conf.src_reqlimit /= 10;
        } else {
            conf.src_reqlimit = 0;
        }
    }

    return run_dump(&conf);
}
