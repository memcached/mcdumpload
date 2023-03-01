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

static void poll_for(struct pollfd *to_poll, int idx, int flag) {
    while (1) {
        int ret = poll(to_poll, 3, 1000);
        if (ret == 0)
            continue; // timeout
        if (to_poll[idx].revents & flag) {
            return;
        }
    }
}

struct mcdump_conf {
    char source_host[NI_MAXHOST];
    char source_port[NI_MAXSERV];
    char dest_host[NI_MAXHOST];
    char dest_port[NI_MAXSERV];
    uint32_t dest_bwlimit; // kilobytes per timeslice
};

struct mcdump_bwlim {
    uint32_t limit; // kilobytes per timeslice
    int32_t remain; // bytes remaining for this next window
    struct timeval window_end; // the enxt of the next window
};

struct mcdump_buf {
    char *buf;
    int size;
    int filled;
    int consumed;
    int parsed; // used just for the data transformer
    int complete; // 0 or 1 to indicate if this stream has completed.
};

// if window is zero and remain is zero, just sleep for window length
// if window and remain is <= 0, sleep until window and reset window
// if window and remain is <= 0 and window is in the past, just set window
#define WINDOW_LENGTH 100000 // 1/10th of a sec
static void run_bwlimit(struct mcdump_bwlim *l, int sent) {
    struct timeval now;
    struct timeval toadd = {.tv_sec = 0, .tv_usec = WINDOW_LENGTH};

    if (l->limit == 0)
        return;

    l->remain -= sent;
    gettimeofday(&now, NULL);

    if (l->remain <= 0) {
        l->remain = l->limit;
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
// Adds the Oo flag to be replaced by "q" so we can quiet set and have the
// string line up.
#define MGDUMP_FLAGS " k f t v q u Oo\r\n"
#define KEYSBUF_SIZE 65536
#define DATA_WRITEBUF_SIZE KEYSBUF_SIZE * 2
#define DATA_READBUF_SIZE (1024 * 1024 * 16)
#define MIN_KEYOUT_SPACE 1024

// apply rate limiter here?
// would allow avoiding memove of data_cmds buf since we just wait for it to
// drain
static void dumplist_to_data_write(struct mcdump_buf *keys, struct mcdump_buf *data_cmds) {
    // loop:
    // memchr to find \n in keys.buf + consumed
    // if found, and enough space in data_cmds.buf + filled, copy + add flags
    // if not found or space full, advance dump consumed
    while (1) {
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
        if (data_cmds->size - data_cmds->filled < MIN_KEYOUT_SPACE) {
            break;
        }

        if (len > 2 && strncmp(start, "EN\r\n", 4) == 0) {
            memmove(keys->buf, start, 4);
            keys->filled = 4;
            keys->consumed = 0;
            // Toss a nop to cap off the key write
            if (keys->complete == 0) {
                char *data = data_cmds->buf + data_cmds->filled;
                memcpy(data, "mn\r\n", 4);
                data_cmds->filled += 4;
                keys->complete = 1;
            }
            break;
        }

        char *sdata;
        char *data = sdata = data_cmds->buf + data_cmds->filled;
        // copy base command but don't copy the \r\n
        memcpy(data, start, len-1);
        keys->consumed += len+1; // get the \n
        data += len-1;

        memcpy(data, MGDUMP_FLAGS, sizeof(MGDUMP_FLAGS)-1);
        data += sizeof(MGDUMP_FLAGS)-1;

        data_cmds->filled += data - sdata;
        //fprintf(stderr, "Copied command: %.*s\n", (int)(data - sdata)-1, sdata);

        // ran out of keys to copy.
        if (keys->consumed == keys->filled) {
            keys->consumed = 0;
            keys->filled = 0;
            break;
        }
    }
}

#define TEMP_SIZE 512

static void convert_data_in(struct mcdump_buf *data_in) {
    char temp[TEMP_SIZE];
    int temp_len = 0;
    int parsed = data_in->parsed;
    if (data_in->complete) {
        return;
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
                // forward the mn along to the destination.
                memcpy(start, "mn", 2);
                parsed += 4;
                data_in->complete = 1;
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

        // we should have enough space left for "q" then "\r\n"
        if (end - np < 3) {
            fprintf(stderr, "ERROR: not enough space left to append q flag: %s\n", start);
            abort();
        }

        *np = 'q';
        np++;
        while (*np != '\r') {
            *np = ' '; // blank out any remaining characters
            np++;
        }

        // done...
        parsed += end - start + 1; // include the \n
        parsed += vsize;

        if (parsed >= data_in->filled)
            break;
    }

    data_in->parsed = parsed;
    if (data_in->filled < data_in->parsed) {
        abort();
    }


}

static void run_keys(int keys_fd, struct mcdump_buf *keys) {
    // No space to continue reading.
    if (keys->filled == keys->size) {
        return;
    }

    // Stream completed? Though we shouldn't be continuing to get read events
    if (keys->filled > 3) {
        if (strncmp(keys->buf, "EN\r\n", 4) == 0) {
            return;
        }
    }

    int read = recv(keys_fd, keys->buf + keys->filled, keys->size - keys->filled, 0);
    // TODO: check read result for error.
    keys->filled += read;
}

// TODO: check fetch rate limiter and wait?
// or is that simply a sleep elsewhere?
// TODO: max pipelines per fetch?
static void run_data_out(int data_fd, struct mcdump_buf *keys, struct mcdump_buf *data) {
    // Do we have anything to write?
    if (data->filled <= data->consumed) {
        // No key data to work with right now.
        if (keys->filled <= keys->consumed) {
            return;
        }

        // Fill new key data.
        dumplist_to_data_write(keys, data);
    }

    // TODO: Detect disconnect
    if (data->filled > data->consumed) {
        int sent = send(data_fd, data->buf + data->consumed, data->filled - data->consumed, 0);
        if (sent < 0) {
            abort();
        }
        data->consumed += sent;
    }

    // We've sent the full buffer.
    if (data->consumed == data->filled) {
        data->consumed = 0;
        data->filled = 0;
    }
}

static void run_data_in(int data_fd, struct mcdump_buf *data) {
    // if space, read as much as we can
    if (data->filled < data->size) {
        int read = recv(data_fd, data->buf + data->filled, data->size - data->filled, 0);
        // TODO: check read result for error.
        data->filled += read;

        convert_data_in(data);
    }
}

static void run_dest_out(int dest_fd, struct mcdump_buf *data_in, struct mcdump_bwlim *bwlimit) {
    // anything to write out?
    if (data_in->parsed <= data_in->consumed) {
        return;
    }

    int tosend = data_in->parsed - data_in->consumed;
    if (bwlimit->limit != 0 && bwlimit->remain < tosend) {
        tosend = bwlimit->remain;
    }

    int sent = send(dest_fd, data_in->buf + data_in->consumed, tosend, 0);
    // TODO: check result for error.
    data_in->consumed += sent;

    run_bwlimit(bwlimit, sent);

    if (data_in->consumed == data_in->parsed) {
        // shift a partial buffer.
        if (data_in->parsed < data_in->filled) {
            memmove(data_in->buf, data_in->buf+data_in->parsed,
                    data_in->filled - data_in->consumed);
            data_in->filled = data_in->filled - data_in->consumed;
        } else {
            data_in->filled = 0;
        }
        data_in->consumed = 0;
        data_in->parsed = 0;
    }
}

static int is_work_complete(struct mcdump_buf *keys,
                            struct mcdump_buf *data_read,
                            struct mcdump_buf *keys_out) {
    if (keys->complete == 1) {
        if (data_read->complete == 1 && data_read->filled == 0) {
            return 1;
        }
    }

    return 0;
}

#define POLL_KEYS 0
#define POLL_KEYOUT 1
#define POLL_DEST 2

static int run_dump(struct mcdump_conf *conf) {
    struct pollfd to_poll[3];
    struct mcdump_buf keys_buf = {0};
    struct mcdump_buf keys_out_buf = {0};
    struct mcdump_buf data_read_buf = {0};
    struct mcdump_bwlim bwlimit = {0};

    bwlimit.limit = conf->dest_bwlimit;
    bwlimit.remain = bwlimit.limit; // seed the limiter.

    // TODO: commandline arguments
    int keys_fd = dump_connect(conf->source_host, conf->source_port);
    int kout_fd = dump_connect(conf->source_host, conf->source_port);
    int dest_fd = dump_connect(conf->dest_host, conf->dest_port);

    to_poll[POLL_KEYS].fd = keys_fd;
    to_poll[POLL_KEYS].events = POLLIN;
    to_poll[POLL_KEYOUT].fd = kout_fd;
    to_poll[POLL_KEYOUT].events = POLLIN|POLLOUT;
    to_poll[POLL_DEST].fd = dest_fd;
    to_poll[POLL_DEST].events = POLLIN|POLLOUT;

    keys_buf.buf = malloc(KEYSBUF_SIZE);
    keys_buf.size = KEYSBUF_SIZE;
    keys_out_buf.buf = malloc(DATA_WRITEBUF_SIZE);
    keys_out_buf.size = DATA_WRITEBUF_SIZE;
    data_read_buf.buf = malloc(DATA_READBUF_SIZE);
    data_read_buf.size = DATA_READBUF_SIZE;

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
        // TODO: check POLLHUP/etc
        int ret = poll(to_poll, 3, 1000);
        if (ret < 0) {
            abort();
        }
        if (ret == 0)
            continue; // timeout

        if (to_poll[POLL_KEYS].revents & POLLIN) {
            run_keys(keys_fd, &keys_buf);
        }

        if (to_poll[POLL_KEYOUT].revents & POLLOUT) {
            run_data_out(kout_fd, &keys_buf, &keys_out_buf);
        }

        if (to_poll[POLL_KEYOUT].revents & POLLIN) {
            run_data_in(kout_fd, &data_read_buf);
        }

        if (to_poll[POLL_DEST].revents & POLLOUT) {
            run_dest_out(dest_fd, &data_read_buf, &bwlimit);
        }

        if (is_work_complete(&keys_buf, &data_read_buf, &keys_out_buf)) {
            fprintf(stderr, "Dumpload complete\n");
            break;
        }

        // Set polling for next round.
        // If there's no space in the keys buffer, don't keep checking read.
        if (keys_buf.filled < keys_buf.size
                && keys_out_buf.size - keys_out_buf.filled > MIN_KEYOUT_SPACE) {
            to_poll[POLL_KEYS].events = POLLIN;
        } else {
            to_poll[POLL_KEYS].events = 0;
        }

        // If we have no keys to move to the key out buffer, and no data
        // currently being flushed to the data in fd, don't poll.
        if (keys_buf.filled <= keys_buf.consumed
                && keys_out_buf.size - keys_out_buf.filled < MIN_KEYOUT_SPACE) {
            to_poll[POLL_KEYOUT].events = 0;
        } else if (data_read_buf.size == data_read_buf.filled) {
            // destination buffer is full, wait before fetching more keys.
            to_poll[POLL_KEYOUT].events = 0;
        } else {
            to_poll[POLL_KEYOUT].events = POLLIN|POLLOUT;
        }

        // If there's no converted data to for the destination, don't poll.
        if (data_read_buf.parsed <= data_read_buf.consumed) {
            to_poll[POLL_DEST].events = 0;
        } else {
            to_poll[POLL_DEST].events = POLLIN|POLLOUT;
        }
    }

    while (1) {
        poll_for(to_poll, POLL_DEST, POLLIN);
        int read = recv(dest_fd, data_read_buf.buf + data_read_buf.filled,
                data_read_buf.size - data_read_buf.filled, 0);
        data_read_buf.filled += read;
        if (data_read_buf.filled < 4)
            continue;
        if (strncmp(data_read_buf.buf, "MN\r\n", 4) == 0) {
            break;
        } else {
            fprintf(stderr, "ERROR: Got unexpected result from destination socket: %.*s\n",
                    data_read_buf.filled, data_read_buf.buf);
        }
    }

    close(keys_fd);
    close(kout_fd);

    return EXIT_SUCCESS;
}

static void usage(void) {
    printf("usage:\n"
           "--srcip=<addr>\n"
           "--srcport=<port>\n"
           "--dstip=<addr>\n"
           "--dstport=<port>\n"
           "--dstbwlimit=<kilobits/sec>\n"
          );
}

// TODO:
// - add a "stdout" mode
int main(int argc, char **argv) {
    struct mcdump_conf conf = {.source_host = "127.0.0.1", .source_port = "11211",
                               .dest_bwlimit = 0};
    int dest_host = 0;
    int dest_port = 0;
    const struct option longopts[] = {
        {"srcip", required_argument, 0, 'i'},
        {"srcport", required_argument, 0, 'p'},
        {"dstip", required_argument, 0, 'd'},
        {"dstport", required_argument, 0, 's'},
        {"dstbwlimit", required_argument, 0, 'b'},
        {"help", no_argument, 0, 'h'},
        // end
        {0, 0, 0, 0}
    };
    int optindex;
    int c;
    while (-1 != (c = getopt_long(argc, argv, "", longopts, &optindex))) {
        switch (c) {
        case 'i':
            strncpy(conf.source_host, optarg, NI_MAXHOST-1);
            break;
        case 'p':
            strncpy(conf.source_port, optarg, NI_MAXSERV-1);
            break;
        case 'd':
            strncpy(conf.dest_host, optarg, NI_MAXHOST-1);
            dest_host = 1;
            break;
        case 's':
            strncpy(conf.dest_port, optarg, NI_MAXSERV-1);
            dest_port = 1;
            break;
        case 'b':
            conf.dest_bwlimit = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
            break;
        default:
            fprintf(stderr, "Unknown option\n");
            return EXIT_FAILURE;
        }
    }

    if (dest_host == 0 || dest_port == 0) {
        fprintf(stderr, "Arguments require at least '--dstip and --dstport'");
        return EXIT_FAILURE;
    }

    if (conf.dest_bwlimit) {
        int64_t bits_sec = conf.dest_bwlimit * 1024;
        int64_t bytes_sec = bits_sec / 8;
        // ultimately bytes per time slice.
        conf.dest_bwlimit = bytes_sec / 10;
    }

    return run_dump(&conf);
}
