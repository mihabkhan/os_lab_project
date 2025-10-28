#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <time.h>
#include <limits.h>

#define PORT 9000
#define BACKLOG 128
#define CLIENT_POOL_SIZE 8
#define WORKER_POOL_SIZE 6
#define SENDER_POOL_SIZE 4
#define MAX_USERNAME 64
#define MAX_FILENAME 256
#define DEFAULT_QUOTA_BYTES (100*1024*1024)
#define LINEBUF 1024

static ssize_t recv_line(int sock, char *buf, size_t maxlen) {
    size_t n = 0;
    char c;
    while (n + 1 < maxlen) {
        ssize_t r = recv(sock, &c, 1, 0);
        if (r == 0 && n == 0) return 0;
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

typedef struct Node {
    void *val;
    struct Node *next;
} Node;

typedef struct {
    Node *head, *tail;
    pthread_mutex_t m;
    pthread_cond_t nonempty;
    int shutting_down;
} GenQueue;

static void queue_init(GenQueue *q) {
    q->head = q->tail = NULL;
    pthread_mutex_init(&q->m, NULL);
    pthread_cond_init(&q->nonempty, NULL);
    q->shutting_down = 0;
}
static void queue_push(GenQueue *q, void *v) {
    Node *n = malloc(sizeof(Node));
    if (!n) return;
    n->val = v; n->next = NULL;
    pthread_mutex_lock(&q->m);
    if (q->tail) q->tail->next = n; else q->head = n;
    q->tail = n;
    pthread_cond_signal(&q->nonempty);
    pthread_mutex_unlock(&q->m);
}
static void *queue_pop(GenQueue *q) {
    pthread_mutex_lock(&q->m);
    while (!q->head && !q->shutting_down) pthread_cond_wait(&q->nonempty, &q->m);
    if (!q->head) {
        pthread_mutex_unlock(&q->m);
        return NULL;
    }
    Node *n = q->head;
    q->head = n->next;
    if (!q->head) q->tail = NULL;
    pthread_mutex_unlock(&q->m);
    void *v = n->val;
    free(n);
    return v;
}
static void queue_wakeup_all(GenQueue *q) {
    pthread_mutex_lock(&q->m);
    q->shutting_down = 1;
    pthread_cond_broadcast(&q->nonempty);
    pthread_mutex_unlock(&q->m);
}

typedef struct FileEntry {
    char name[MAX_FILENAME];
    size_t size;
    struct FileEntry *next;
} FileEntry;

typedef struct User {
    char username[MAX_USERNAME];
    size_t quota_bytes;
    size_t used_bytes;
    FileEntry *files;
    pthread_mutex_t lock;
    struct User *next;
} User;

static User *users_head = NULL;
static pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct FileLockEntry {
    char key[512];
    pthread_rwlock_t rw;
    struct FileLockEntry *next;
    int refcount;
} FileLockEntry;

static FileLockEntry *filelocks_head = NULL;
static pthread_mutex_t filelocks_mutex = PTHREAD_MUTEX_INITIALIZER;

static pthread_rwlock_t *get_file_lock(const char *user, const char *fname) {
    char key[512];
    snprintf(key, sizeof(key), "%s/%s", user, fname);
    pthread_mutex_lock(&filelocks_mutex);
    FileLockEntry *e = filelocks_head;
    while (e) {
        if (strcmp(e->key, key) == 0) { e->refcount++; pthread_mutex_unlock(&filelocks_mutex); return &e->rw; }
        e = e->next;
    }
    e = calloc(1, sizeof(FileLockEntry));
    if (!e) { pthread_mutex_unlock(&filelocks_mutex); return NULL; }
    strncpy(e->key, key, sizeof(e->key)-1);
    pthread_rwlock_init(&e->rw, NULL);
    e->refcount = 1;
    e->next = filelocks_head;
    filelocks_head = e;
    pthread_mutex_unlock(&filelocks_mutex);
    return &e->rw;
}
static void release_file_lock(const char *user, const char *fname) {
    char key[512];
    snprintf(key, sizeof(key), "%s/%s", user, fname);
    pthread_mutex_lock(&filelocks_mutex);
    FileLockEntry *prev = NULL, *cur = filelocks_head;
    while (cur) {
        if (strcmp(cur->key, key) == 0) {
            cur->refcount--;
            if (cur->refcount == 0) {
                if (prev) prev->next = cur->next; else filelocks_head = cur->next;
                pthread_rwlock_destroy(&cur->rw);
                free(cur);
            }
            break;
        }
        prev = cur; cur = cur->next;
    }
    pthread_mutex_unlock(&filelocks_mutex);
}

static User *find_user(const char *username) {
    pthread_mutex_lock(&users_mutex);
    User *u = users_head;
    while (u) {
        if (strcmp(u->username, username) == 0) break;
        u = u->next;
    }
    pthread_mutex_unlock(&users_mutex);
    return u;
}

static int create_user(const char *username) {
    pthread_mutex_lock(&users_mutex);
    User *u = users_head;
    while (u) { if (strcmp(u->username, username)==0) { pthread_mutex_unlock(&users_mutex); return -1; } u=u->next; }
    User *nu = calloc(1, sizeof(User));
    if (!nu) { pthread_mutex_unlock(&users_mutex); return -1; }
    strncpy(nu->username, username, sizeof(nu->username)-1);
    nu->quota_bytes = DEFAULT_QUOTA_BYTES;
    nu->used_bytes = 0;
    nu->files = NULL;
    pthread_mutex_init(&nu->lock, NULL);
    nu->next = users_head;
    users_head = nu;
    pthread_mutex_unlock(&users_mutex);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "storage/%s", username);
    mkdir("storage", 0755);
    mkdir(path, 0755);
    return 0;
}

static void add_file_to_user(User *u, const char *fname, size_t fsize) {
    FileEntry *fe = malloc(sizeof(FileEntry));
    if (!fe) return;
    strncpy(fe->name, fname, sizeof(fe->name)-1);
    fe->size = fsize;
    fe->next = u->files;
    u->files = fe;
    u->used_bytes += fsize;
}
static int remove_file_from_user(User *u, const char *fname) {
    FileEntry *prev = NULL, *cur = u->files;
    while (cur) {
        if (strcmp(cur->name, fname) == 0) {
            if (prev) prev->next = cur->next;
            else u->files = cur->next;
            u->used_bytes -= cur->size;
            free(cur);
            return 0;
        }
        prev = cur; cur = cur->next;
    }
    return -1;
}

typedef enum { TASK_UPLOAD=1, TASK_DOWNLOAD=2, TASK_DELETE=3, TASK_LIST=4 } task_type_t;

typedef struct Task {
    int client_sock;
    int session_id;
    char username[MAX_USERNAME];
    task_type_t type;
    char filename[MAX_FILENAME];
    size_t filesize;
    char *outbuf; size_t outlen;
    int result_code;
    char errmsg[256];
} Task;

typedef struct Result {
    int client_sock;
    int session_id;
    char *data; size_t len;
} Result;

static GenQueue client_q;
static GenQueue task_q;
static GenQueue result_q;

static volatile int running = 1;
static int listenfd = -1;

static void send_error_task(Task *t, const char *err) {
    t->result_code = -1;
    snprintf(t->errmsg, sizeof(t->errmsg), "%s", err);
}

static void make_paths(const char *user, const char *fname, char *outpath, size_t outlen) {
    snprintf(outpath, outlen, "storage/%s/%s", user, fname);
}

static void handle_upload(Task *t) {
    char userdir[PATH_MAX], tmp_template[PATH_MAX], final[PATH_MAX];
    snprintf(userdir, sizeof(userdir), "storage/%s", t->username);
    int n = snprintf(tmp_template, sizeof(tmp_template), "%s/.tmp_%lu_XXXXXX", userdir, (unsigned long)pthread_self());
    if (n < 0 || (size_t)n >= sizeof(tmp_template)) {
        send_error_task(t, "ERR path_overflow\n");
        return;
    }

    pthread_rwlock_t *fl = get_file_lock(t->username, t->filename);
    if (!fl) { send_error_task(t, "ERR lock_fail\n"); return; }
    pthread_rwlock_wrlock(fl);

    int tmpfd = mkstemp(tmp_template);
    if (tmpfd < 0) {
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        send_error_task(t, "ERR cannot_create_tmp\n");
        return;
    }
    FILE *f = fdopen(tmpfd, "wb");
    if (!f) {
        close(tmpfd);
        unlink(tmp_template);
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        send_error_task(t, "ERR cannot_create_tmp\n");
        return;
    }

    size_t left = t->filesize;
    char buf[4096];
    int read_ok = 1;
    while (left) {
        size_t toread = (left > sizeof(buf) ? sizeof(buf) : left);
        ssize_t r = recv_all(t->client_sock, buf, toread);
        if (r <= 0) { read_ok = 0; break; }
        size_t w = fwrite(buf, 1, (size_t)r, f);
        if (w != (size_t)r) { read_ok = 0; break; }
        left -= (size_t)r;
    }
    fclose(f);
    if (!read_ok) {
        unlink(tmp_template);
        send_error_task(t, "ERR upload_recv_failed\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }

    User *u = find_user(t->username);
    if (!u) {
        unlink(tmp_template);
        send_error_task(t, "ERR user_not_found\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }

    pthread_mutex_lock(&u->lock);
    FileEntry *cur = u->files;
    size_t prev_size = 0;
    while (cur) { if (strcmp(cur->name, t->filename) == 0) { prev_size = cur->size; break; } cur = cur->next; }
    if (u->used_bytes - prev_size + t->filesize > u->quota_bytes) {
        pthread_mutex_unlock(&u->lock);
        unlink(tmp_template);
        send_error_task(t, "ERR quota_exceeded\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }

    int rn = snprintf(final, sizeof(final), "%s/%s", userdir, t->filename);
    if (rn < 0 || (size_t)rn >= sizeof(final)) {
        pthread_mutex_unlock(&u->lock);
        unlink(tmp_template);
        send_error_task(t, "ERR path_overflow\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }
    if (rename(tmp_template, final) != 0) {
        pthread_mutex_unlock(&u->lock);
        unlink(tmp_template);
        send_error_task(t, "ERR rename_failed\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }

    cur = u->files;
    while (cur) {
        if (strcmp(cur->name, t->filename) == 0) { u->used_bytes = u->used_bytes - cur->size + t->filesize; cur->size = t->filesize; break; }
        cur = cur->next;
    }
    if (!cur) add_file_to_user(u, t->filename, t->filesize);
    pthread_mutex_unlock(&u->lock);

    t->result_code = 1;
    snprintf(t->errmsg, sizeof(t->errmsg), "OK\n");
    pthread_rwlock_unlock(fl);
    release_file_lock(t->username, t->filename);
}

static void handle_download(Task *t) {
    pthread_rwlock_t *fl = get_file_lock(t->username, t->filename);
    if (!fl) { send_error_task(t, "ERR lock_fail\n"); return; }
    pthread_rwlock_rdlock(fl);

    char path[PATH_MAX];
    make_paths(t->username, t->filename, path, sizeof(path));
    FILE *f = fopen(path, "rb");
    if (!f) {
        send_error_task(t, "ERR not_found\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); send_error_task(t, "ERR io\n"); pthread_rwlock_unlock(fl); release_file_lock(t->username, t->filename); return; }
    long ft = ftell(f);
    if (ft < 0) { fclose(f); send_error_task(t, "ERR io\n"); pthread_rwlock_unlock(fl); release_file_lock(t->username, t->filename); return; }
    size_t sz = (size_t)ft;
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); send_error_task(t, "ERR io\n"); pthread_rwlock_unlock(fl); release_file_lock(t->username, t->filename); return; }

    char header[128];
    int hn = snprintf(header, sizeof(header), "OK %zu\n", sz);
    if (hn < 0) { fclose(f); send_error_task(t, "ERR mem\n"); pthread_rwlock_unlock(fl); release_file_lock(t->username, t->filename); return; }
    size_t tot = (size_t)hn + sz;
    char *buf = malloc(tot);
    if (!buf) { fclose(f); send_error_task(t, "ERR mem\n"); pthread_rwlock_unlock(fl); release_file_lock(t->username, t->filename); return; }
    memcpy(buf, header, (size_t)hn);
    size_t off = (size_t)hn;
    size_t left = sz;
    char tmpbuf[4096];
    while (left) {
        size_t r = fread(tmpbuf, 1, (left > sizeof(tmpbuf) ? sizeof(tmpbuf) : left), f);
        if (r == 0) break;
        memcpy(buf + off, tmpbuf, r);
        off += r;
        left -= r;
    }
    fclose(f);
    t->outbuf = buf; t->outlen = tot;
    t->result_code = 1;
    pthread_rwlock_unlock(fl);
    release_file_lock(t->username, t->filename);
}

static void handle_delete(Task *t) {
    pthread_rwlock_t *fl = get_file_lock(t->username, t->filename);
    if (!fl) { send_error_task(t, "ERR lock_fail\n"); return; }
    pthread_rwlock_wrlock(fl);

    User *u = find_user(t->username);
    if (!u) {
        send_error_task(t, "ERR user_not_found\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }
    pthread_mutex_lock(&u->lock);
    FileEntry *cur = u->files;
    size_t fsize = 0;
    while (cur) { if (strcmp(cur->name, t->filename) == 0) { fsize = cur->size; break; } cur = cur->next; }
    if (fsize == 0) {
        pthread_mutex_unlock(&u->lock);
        send_error_task(t, "ERR not_found\n");
        pthread_rwlock_unlock(fl);
        release_file_lock(t->username, t->filename);
        return;
    }
    remove_file_from_user(u, t->filename);
    pthread_mutex_unlock(&u->lock);
    char path[PATH_MAX];
    make_paths(t->username, t->filename, path, sizeof(path));
    unlink(path);
    t->result_code = 1;
    snprintf(t->errmsg, sizeof(t->errmsg), "OK\n");
    pthread_rwlock_unlock(fl);
    release_file_lock(t->username, t->filename);
}

static void handle_list(Task *t) {
    User *u = find_user(t->username);
    if (!u) {
        send_error_task(t, "ERR user_not_found\n");
        return;
    }
    pthread_mutex_lock(&u->lock);
    size_t need = 0;
    FileEntry *f = u->files;
    char line[512];
    while (f) {
        int n = snprintf(line, sizeof(line), "%s %zu\n", f->name, f->size);
        need += (n>0?n:0);
        f = f->next;
    }
    need += 5;
    char *buf = malloc(need+1);
    if (!buf) { pthread_mutex_unlock(&u->lock); send_error_task(t, "ERR mem\n"); return; }
    size_t off = 0;
    f = u->files;
    while (f) {
        int n = snprintf(line, sizeof(line), "%s %zu\n", f->name, f->size);
        memcpy(buf + off, line, n); off += n;
        f = f->next;
    }
    memcpy(buf + off, "END\n", 4); off += 4;
    buf[off] = '\0';
    pthread_mutex_unlock(&u->lock);
    t->outbuf = buf; t->outlen = off;
    t->result_code = 1;
}

static void *worker_thread_fn(void *arg) {
    (void)arg;
    while (running) {
        Task *t = (Task *)queue_pop(&task_q);
        if (!t) break;
        if (t->type == TASK_UPLOAD) handle_upload(t);
        else if (t->type == TASK_DOWNLOAD) handle_download(t);
        else if (t->type == TASK_DELETE) handle_delete(t);
        else if (t->type == TASK_LIST) handle_list(t);
        else { send_error_task(t, "ERR unknown_task\n"); }
        queue_push(&result_q, t);
    }
    return NULL;
}

static void *sender_thread_fn(void *arg) {
    (void)arg;
    while (running) {
        Task *t = (Task *)queue_pop(&result_q);
        if (!t) break;
        if (t->result_code == -1) {
            send_all(t->client_sock, t->errmsg, strlen(t->errmsg));
        } else {
            if (t->outbuf) {
                if (send_all(t->client_sock, t->outbuf, t->outlen) < 0) {
                }
                free(t->outbuf);
            } else {
                send_all(t->client_sock, t->errmsg, strlen(t->errmsg));
            }
        }
        free(t);
    }
    return NULL;
}


static int next_session_id() {
    static pthread_mutex_t sid_m = PTHREAD_MUTEX_INITIALIZER;
    static int sid = 1;
    pthread_mutex_lock(&sid_m); int v = sid++; pthread_mutex_unlock(&sid_m); return v;
}

static void *client_thread_fn(void *arg) {
    (void)arg;
    char line[LINEBUF];
    while (running) {
        int sock = (int)(intptr_t)queue_pop(&client_q);
        if (!running || sock == 0) { if (sock) close(sock); break; }
        int session = next_session_id();
        while (1) {
            ssize_t n = recv_line(sock, line, sizeof(line));
            if (n <= 0) { close(sock); break; }
            char cmd[32], username[MAX_USERNAME];
            if (sscanf(line, "%31s %63s", cmd, username) >= 1) {
                if (strcmp(cmd, "SIGNUP") == 0) {
                    if (sscanf(line, "%*s %63s", username) != 1) { send_all(sock, "ERR invalid_signup\n", 19); continue; }
                    if (create_user(username) != 0) {
                        send_all(sock, "ERR user_exists\n", 16);
                    } else {
                        send_all(sock, "OK\n", 3);
                    }
                    continue;
                } else if (strcmp(cmd, "LOGIN") == 0) {
                    if (sscanf(line, "%*s %63s", username) != 1) { send_all(sock, "ERR invalid_login\n", 18); continue; }
                    if (!find_user(username)) send_all(sock, "ERR no_such_user\n", 17);
                    else send_all(sock, "OK\n", 3);
                    continue;
                }
            }

            char first[128];
            if (sscanf(line, "%127s", first) != 1) { send_all(sock, "ERR unknown\n", 12); continue; }
            if (strcmp(first, "UPLOAD") == 0) {
                char uname[MAX_USERNAME], fname[MAX_FILENAME]; size_t fsize;
                if (sscanf(line, "UPLOAD %63s %255s %zu", uname, fname, &fsize) != 3) {
                    send_all(sock, "ERR bad_upload_syntax\n", 22); continue;
                }
                Task *t = calloc(1, sizeof(Task));
                if (!t) { send_all(sock, "ERR mem\n", 8); continue; }
                t->client_sock = sock;
                t->session_id = session;
                strncpy(t->username, uname, sizeof(t->username)-1);
                t->type = TASK_UPLOAD;
                strncpy(t->filename, fname, sizeof(t->filename)-1);
                t->filesize = fsize;
                t->outbuf = NULL; t->outlen = 0;
                t->result_code = 0;
                t->errmsg[0] = '\0';
                queue_push(&task_q, t);
                continue;
            } else if (strcmp(first, "DOWNLOAD") == 0) {
                char uname[MAX_USERNAME], fname[MAX_FILENAME];
                if (sscanf(line, "DOWNLOAD %63s %255s", uname, fname) != 2) {
                    send_all(sock, "ERR bad_download_syntax\n", 24); continue;
                }
                Task *t = calloc(1, sizeof(Task));
                if (!t) { send_all(sock, "ERR mem\n", 8); continue; }
                t->client_sock = sock;
                t->session_id = session;
                strncpy(t->username, uname, sizeof(t->username)-1);
                t->type = TASK_DOWNLOAD;
                strncpy(t->filename, fname, sizeof(t->filename)-1);
                t->outbuf = NULL; t->outlen = 0;
                t->result_code = 0;
                t->errmsg[0] = '\0';
                queue_push(&task_q, t);
                continue;
            } else if (strcmp(first, "DELETE") == 0) {
                char uname[MAX_USERNAME], fname[MAX_FILENAME];
                if (sscanf(line, "DELETE %63s %255s", uname, fname) != 2) {
                    send_all(sock, "ERR bad_delete_syntax\n", 22); continue;
                }
                Task *t = calloc(1, sizeof(Task));
                if (!t) { send_all(sock, "ERR mem\n", 8); continue; }
                t->client_sock = sock;
                t->session_id = session;
                strncpy(t->username, uname, sizeof(t->username)-1);
                t->type = TASK_DELETE;
                strncpy(t->filename, fname, sizeof(t->filename)-1);
                t->outbuf = NULL; t->outlen = 0;
                t->result_code = 0;
                t->errmsg[0] = '\0';
                queue_push(&task_q, t);
                continue;
            } else if (strcmp(first, "LIST") == 0) {
                char uname[MAX_USERNAME];
                if (sscanf(line, "LIST %63s", uname) != 1) { send_all(sock, "ERR bad_list_syntax\n", 20); continue; }
                Task *t = calloc(1, sizeof(Task));
                if (!t) { send_all(sock, "ERR mem\n", 8); continue; }
                t->client_sock = sock;
                t->session_id = session;
                strncpy(t->username, uname, sizeof(t->username)-1);
                t->type = TASK_LIST;
                t->outbuf = NULL; t->outlen = 0;
                t->result_code = 0;
                t->errmsg[0] = '\0';
                queue_push(&task_q, t);
                continue;
            } else {
                send_all(sock, "ERR unknown_command\n", 20);
                continue;
            }
        }
    }
    return NULL;
}

static void do_shutdown(int signo) {
    (void)signo;
    running = 0;
    if (listenfd >= 0) close(listenfd);
    queue_wakeup_all(&client_q);
    queue_wakeup_all(&task_q);
    queue_wakeup_all(&result_q);
}

int main(int argc, char **argv) {
    srand((unsigned int)time(NULL));
    mkdir("storage", 0755);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, do_shutdown);
    signal(SIGTERM, do_shutdown);

    queue_init(&client_q);
    queue_init(&task_q);
    queue_init(&result_q);

    pthread_t client_pool[CLIENT_POOL_SIZE];
    for (int i = 0; i < CLIENT_POOL_SIZE; ++i)
        pthread_create(&client_pool[i], NULL, client_thread_fn, NULL);

    pthread_t worker_pool[WORKER_POOL_SIZE];
    for (int i = 0; i < WORKER_POOL_SIZE; ++i)
        pthread_create(&worker_pool[i], NULL, worker_thread_fn, NULL);

    pthread_t sender_pool[SENDER_POOL_SIZE];
    for (int i = 0; i < SENDER_POOL_SIZE; ++i)
        pthread_create(&sender_pool[i], NULL, sender_thread_fn, NULL);

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) { perror("socket"); exit(1); }
    int opt = 1; setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(PORT);
    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(listenfd, BACKLOG) < 0) { perror("listen"); exit(1); }
    printf("server_phase2 listening on %d\n", PORT);

    while (running) {
        int client = accept(listenfd, NULL, NULL);
        if (client < 0) {
            if (!running) break;
            continue;
        }
        queue_push(&client_q, (void *)(intptr_t)client);
    }

    queue_wakeup_all(&client_q);
    queue_wakeup_all(&task_q);
    queue_wakeup_all(&result_q);

    for (int i = 0; i < CLIENT_POOL_SIZE; ++i) pthread_join(client_pool[i], NULL);
    for (int i = 0; i < WORKER_POOL_SIZE; ++i) pthread_join(worker_pool[i], NULL);
    for (int i = 0; i < SENDER_POOL_SIZE; ++i) pthread_join(sender_pool[i], NULL);

    pthread_mutex_lock(&users_mutex);
    User *u = users_head;
    while (u) {
        FileEntry *f = u->files;
        while (f) { FileEntry *nf = f->next; free(f); f = nf; }
        User *nu = u->next;
        pthread_mutex_destroy(&u->lock);
        free(u);
        u = nu;
    }
    users_head = NULL;
    pthread_mutex_unlock(&users_mutex);

    pthread_mutex_lock(&filelocks_mutex);
    FileLockEntry *fl = filelocks_head;
    while (fl) {
        FileLockEntry *nf = fl->next;
        pthread_rwlock_destroy(&fl->rw);
        free(fl);
        fl = nf;
    }
    filelocks_head = NULL;
    pthread_mutex_unlock(&filelocks_mutex);

    printf("server shutdown complete\n");
    return 0;
}
