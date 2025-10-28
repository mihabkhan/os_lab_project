#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static ssize_t recv_line(int sock, char *buf, size_t maxlen) {
    size_t n = 0;
    char c;
    while (n + 1 < maxlen) {
        ssize_t r = recv(sock, &c, 1, 0);
        if (r <= 0) return -1;
        buf[n++] = c;
        if (c == '\n') break;
    }
    buf[n] = '\0';
    return (ssize_t)n;
}
static ssize_t recv_all(int sock, void *buf, size_t len) {
    size_t total = 0;
    char *p = buf;
    while (total < len) {
        ssize_t r = recv(sock, p + total, len - total, 0);
        if (r <= 0) return -1;
        total += r;
    }
    return (ssize_t)total;
}
static ssize_t send_all(int sock, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = buf;
    while (total < len) {
        ssize_t s = send(sock, p + total, len - total, 0);
        if (s <= 0) return -1;
        total += s;
    }
    return (ssize_t)total;
}

int main(void) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }
    struct sockaddr_in sa;
    sa.sin_family = AF_INET;
    sa.sin_port = htons(9000);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(sock, (struct sockaddr*)&sa, sizeof(sa)) < 0) { perror("connect"); return 1; }

    char buf[1024];

    send_all(sock, "SIGNUP alice\n", 13);
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { perror("recv"); close(sock); return 1; }
    printf("SIGNUP -> %s", buf);

    send_all(sock, "LOGIN alice\n", 12);
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { perror("recv login"); close(sock); return 1; }
    printf("LOGIN -> %s", buf);

    send_all(sock, "UPLOAD alice hello.txt 5\n", 24);
    send_all(sock, "hello", 5);
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { perror("recv upload"); close(sock); return 1; }
    printf("UPLOAD -> %s", buf);

    send_all(sock, "LIST alice\n", 11);
    printf("LIST ->\n");
    while (1) {
        if (recv_line(sock, buf, sizeof(buf)) <= 0) { perror("recv list"); close(sock); return 1; }
        if (strcmp(buf, "END\n") == 0) break;
        printf("%s", buf);
    }

    send_all(sock, "DOWNLOAD alice hello.txt\n", 24);
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { perror("recv download header"); close(sock); return 1; }
    if (strncmp(buf, "OK ", 3) == 0) {
        size_t sz = 0;
        sscanf(buf+3, "%zu", &sz);
        char *data = malloc(sz+1);
        if (recv_all(sock, data, sz) <= 0) { perror("recv file"); free(data); close(sock); return 1; }
        data[sz] = '\0';
        printf("DOWNLOAD content (%zu bytes): %s\n", sz, data);
        free(data);
    } else {
        printf("DOWNLOAD -> %s", buf);
    }

    send_all(sock, "DELETE alice hello.txt\n", 22);
    if (recv_line(sock, buf, sizeof(buf)) <= 0) { perror("recv delete"); close(sock); return 1; }
    printf("DELETE -> %s", buf);

    close(sock);
    return 0;
}
