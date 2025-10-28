#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_SERVER "127.0.0.1"
#define DEFAULT_PORT 9000

static void send_all(int sock, const void *buf, size_t len) {
    size_t sent = 0;
    const char *p = buf;
    while (sent < len) {
        ssize_t s = send(sock, p+sent, len-sent, 0);
        if (s <= 0) { perror("send"); exit(1); }
        sent += s;
    }
}

static ssize_t recv_line(int sock, char *buf, size_t maxlen) {
    size_t n = 0; char c;
    while (n+1 < maxlen) {
        ssize_t r = recv(sock, &c, 1, 0);
        if (r <= 0) return -1;
        buf[n++] = c;
        if (c == '\n') break;
    }
    buf[n] = '\0';
    return n;
}

typedef struct {
    int id;
    const char *user;
} ThreadArg;

void *worker(void *arg) {
    ThreadArg *ta = arg;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa; sa.sin_family = AF_INET; sa.sin_port = htons(DEFAULT_PORT); inet_pton(AF_INET, DEFAULT_SERVER, &sa.sin_addr);
    if (connect(sock, (struct sockaddr*)&sa, sizeof(sa)) < 0) { perror("connect"); return NULL; }
    char buf[1024];

    char signup[128]; snprintf(signup, sizeof(signup), "SIGNUP %s\n", ta->user);
    send_all(sock, signup, strlen(signup));
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { close(sock); return NULL; }

    char fname[64];
    snprintf(fname, sizeof(fname), "file_%d.txt", ta->id);
    char cmd[256];
    const char *payload = "hello_multitest";
    snprintf(cmd, sizeof(cmd), "UPLOAD %s %s %zu\n", ta->user, fname, strlen(payload));
    send_all(sock, cmd, strlen(cmd));
    send_all(sock, payload, strlen(payload));
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { close(sock); return NULL; }

    snprintf(cmd, sizeof(cmd), "LIST %s\n", ta->user);
    send_all(sock, cmd, strlen(cmd));
    while (1) {
        if (recv_line(sock, buf, sizeof(buf)) <= 0) break;
        if (strcmp(buf, "END\n") == 0) break;
    }

    snprintf(cmd, sizeof(cmd), "DOWNLOAD %s %s\n", ta->user, fname);
    send_all(sock, cmd, strlen(cmd));
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { close(sock); return NULL; }
    if (strncmp(buf, "OK ", 3) == 0) {
        size_t sz = 0;
        sscanf(buf+3, "%zu", &sz);
        char *d = malloc(sz+1);
        size_t total = 0;
        while (total < sz) {
            ssize_t r = recv(sock, d + total, sz - total, 0);
            if (r <= 0) break;
            total += r;
        }
        d[total] = '\0';
        free(d);
    }
    snprintf(cmd, sizeof(cmd), "DELETE %s %s\n", ta->user, fname);
    send_all(sock, cmd, strlen(cmd));
    if (recv_line(sock, buf, sizeof(buf)) <= 0) {}
    close(sock);
    return NULL;
}

int main(int argc, char **argv) {
    int threads = 50;
    if (argc >= 2) threads = atoi(argv[1]);
    pthread_t *t = malloc(sizeof(pthread_t)*threads);
    ThreadArg *args = malloc(sizeof(ThreadArg)*threads);
    for (int i = 0; i < threads; ++i) {
        args[i].id = i;
        char *uname = malloc(32);
        snprintf(uname, 32, "user%d", i%10);
        args[i].user = uname;
        pthread_create(&t[i], NULL, worker, &args[i]);
    }
    for (int i = 0; i < threads; ++i) pthread_join(t[i], NULL);
    printf("All threads done\n");
    return 0;
}
