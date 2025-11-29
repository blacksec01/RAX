#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <termios.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <curl/curl.h>
#include <ctype.h>
#include <sys/file.h>
#include <inttypes.h>
#include <stdint.h>
#include <sys/ioctl.h>
#include <stdatomic.h>
#include <dirent.h>
#include <sys/mman.h>

/*
 * Spinlock compatibility layer for systems without native spinlock support.
 * On macOS and other systems lacking POSIX spinlocks, we fall back to mutexes.
 */
#if defined(__APPLE__) || !defined(_POSIX_SPIN_LOCKS)
    typedef pthread_mutex_t pthread_spinlock_t;
    
    #define spin_init(lock, pshared) \
        pthread_mutex_init(lock, NULL)
    
    #define spin_destroy(lock) \
        pthread_mutex_destroy(lock)
    
    #define spin_lock(lock) \
        pthread_mutex_lock(lock)
    
    #define spin_unlock(lock) \
        pthread_mutex_unlock(lock)
    
    #define spin_trylock(lock) \
        pthread_mutex_trylock(lock)
#else
    #define spin_lock(lock) pthread_spin_lock(lock)
    #define spin_init(lock, pshared) pthread_spin_init(lock, pshared)
    #define spin_destroy(lock) pthread_spin_destroy(lock)
    #define spin_unlock(lock) pthread_spin_unlock(lock)
    #define spin_trylock(lock) pthread_spin_trylock(lock)
#endif

#if defined(__linux__)
#include <linux/limits.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#endif

/*
 * Compiler hints for branch prediction optimization.
 * These macros help the compiler generate more efficient code
 * by indicating which branch is more likely to be taken.
 */
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define ALIGNED(x)  __attribute__((aligned(x)))
#define CACHE_LINE  64
#define PREFETCH(addr) __builtin_prefetch(addr, 0, 3)
#define FORCE_INLINE __attribute__((always_inline)) inline

/* Network configuration for weak or unstable connections */
#define WEAK_MAX_PARTS           4
#define WEAK_MIN_PARTS           2
#define WEAK_DEF_PARTS           2
#define WEAK_CONN_TIMEOUT        45
#define WEAK_LOW_SPEED_TIME      90
#define WEAK_RETRY_MAX           150
#define WEAK_BACKOFF_MAX         20000

/* Network configuration for strong and stable connections */
#define STRONG_MAX_PARTS         16
#define STRONG_MIN_PARTS         4
#define STRONG_DEF_PARTS         8
#define STRONG_CONN_TIMEOUT      10
#define STRONG_LOW_SPEED_TIME    20
#define STRONG_RETRY_MAX         30
#define STRONG_BACKOFF_MAX       5000

/* Active network configuration variables, initialized to moderate defaults */
static int max_parts = 8;
static int min_parts = 3;
static int def_parts = 4;
static int conn_timeout = 20;
static int low_speed_time = 45;
static int retry_max = 75;
static int backoff_max = 10000;

/*
 * Hard disk drive tuning parameters.
 * HDD benefits from larger buffers and less frequent syncs
 * due to mechanical seek times.
 */
#define HDD_BUF_SIZE             (2*1024*1024)
#define HDD_SYNC_MS              20000
#define HDD_FLUSH_THRESH         (1536*1024)
#define HDD_DIRECT_IO            1

/*
 * Solid state drive tuning parameters.
 * SSD can handle smaller buffers and more frequent operations
 * since there is no mechanical latency.
 */
#define SSD_BUF_SIZE             (1*1024*1024)
#define SSD_SYNC_MS              30000
#define SSD_FLUSH_THRESH         (512*1024)
#define SSD_DIRECT_IO            0

/* Active disk configuration, defaults to HDD settings */
static size_t g_buf_size = HDD_BUF_SIZE;
static int g_sync_ms = HDD_SYNC_MS;
static size_t g_flush_thresh = HDD_FLUSH_THRESH;
static int g_direct_io = HDD_DIRECT_IO;

/*
 * Timing and threshold constants used throughout the downloader.
 * These values control how often we update the display, save progress,
 * and handle various timeout conditions.
 */
#define PRINT_MS              4010.9
#define PROGRESS_FLUSH_MS     15000.0
#define LOW_SPEED_BPS         1024
#define BACKOFF_INIT_MS       1000
#define ADAPT_WINDOW_MS       3000.0
#define BASELINE_KBPS         256.0
#define PROGRESS_JSON_MAX     32768
#define EWMA_ALPHA            0.3
#define ETA_MIN_KBPS          10.0
#define CURL_BUF_SIZE         (512*1024)
#define PROGRESS_SAVE_DELTA   (50*1024*1024LL)
#define MAX_STALL_MS          180000.0
#define WBUF_FLUSH_MS         3000.0
#define ADAPT_COOLDOWN_MS     5000.0
#define HEAD_TIMEOUT          20
#define MAX_PATH              2048

/*
 * Direct IO requires sector-aligned buffers and offsets.
 * Define O_DIRECT as zero if the system does not support it.
 */
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

#define SECTOR_SZ 4096

/* Round up a size value to the nearest multiple of alignment */
static inline size_t round_up(size_t sz, size_t align) {
    return (sz + align - 1) & ~(align - 1);
}

/* Round down a size value to the nearest multiple of alignment */
static inline size_t round_down(size_t sz, size_t align) {
    return sz & ~(align - 1);
}

/* Check if a pointer is aligned to the specified boundary */
static inline int is_aligned(const void *p, size_t align) {
    return ((uintptr_t)p & (align - 1)) == 0;
}

/* Shared curl handle for connection reuse across multiple requests */
static CURLSH *g_share = NULL;

/* Global flags and settings */
_Atomic int g_stop = 0;
static _Atomic int g_term_width = 74;
static char g_dir[MAX_PATH] = "./";

/*
 * Error types used for tracking and recovery.
 * Different error types may have different retry strategies.
 */
typedef enum {
    ERR_NET,
    ERR_DISK,
    ERR_AUTH,
    ERR_RES
} ErrType;

/* Tracks error occurrences and manages exponential backoff */
typedef struct {
    ErrType type;
    int count;
    uint64_t last_ms;
    int backoff;
} ErrTrack;

/*
 * Global statistics structure for monitoring download performance.
 * All fields are atomic to allow safe concurrent updates.
 */
typedef struct ALIGNED(CACHE_LINE) {
    _Atomic uint64_t bytes_written;
    _Atomic uint64_t retries;
    _Atomic uint64_t net_errs;
    _Atomic uint64_t disk_errs;
    _Atomic double peak_kbps;
    _Atomic uint64_t stalls;
} Stats;

static Stats g_stats = {0};

/*
 * Buffer pool for efficient memory management.
 * Pre-allocates buffers to avoid repeated malloc/free calls
 * during high-speed downloads.
 */
typedef struct {
    void *bufs[STRONG_MAX_PARTS];
    _Atomic uint32_t mask;
    pthread_spinlock_t lock;
    int ready;
} BufPool;

static BufPool g_pool = {0};

/* Initialize the buffer pool with properly aligned memory */
static int init_buf_pool(void) {
    if (g_pool.ready) return 0;
    
    g_buf_size = round_up(g_buf_size, SECTOR_SZ);
    
    for (int i = 0; i < max_parts; i++) {
        if (g_direct_io) {
            #if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L
            if (posix_memalign(&g_pool.bufs[i], SECTOR_SZ, g_buf_size) != 0) {
                fprintf(stderr, "Error: posix_memalign failed\n");
                for (int j = 0; j < i; j++) {
                    free(g_pool.bufs[j]);
                }
                return -1;
            }
            #else
            #ifdef __GLIBC__
            g_pool.bufs[i] = memalign(SECTOR_SZ, g_buf_size);
            #else
            g_pool.bufs[i] = malloc(g_buf_size);
            if (i == 0) {
                fprintf(stderr, "Warning: Aligned alloc not supported, Direct IO disabled\n");
                g_direct_io = 0;
            }
            #endif
            if (!g_pool.bufs[i]) {
                for (int j = 0; j < i; j++) {
                    free(g_pool.bufs[j]);
                }
                return -1;
            }
            #endif
        } else {
            g_pool.bufs[i] = malloc(g_buf_size);
            if (!g_pool.bufs[i]) {
                for (int j = 0; j < i; j++) {
                    free(g_pool.bufs[j]);
                }
                return -1;
            }
        }
        memset(g_pool.bufs[i], 0, g_buf_size);
    }
    
    if (spin_init(&g_pool.lock, PTHREAD_PROCESS_PRIVATE) != 0) {
        for (int i = 0; i < max_parts; i++) {
            free(g_pool.bufs[i]);
        }
        return -1;
    }
    
    atomic_store(&g_pool.mask, 0);
    g_pool.ready = 1;
    
    if (g_direct_io)
        fprintf(stderr, "[OK] Buffers aligned to %d bytes\n", SECTOR_SZ);
    
    return 0;
}

/* Release all buffers and destroy the pool */
static void cleanup_buf_pool(void) {
    if (!g_pool.ready) return;
    
    for (int i = 0; i < max_parts; i++) {
        if (g_pool.bufs[i]) {
            free(g_pool.bufs[i]);
            g_pool.bufs[i] = NULL;
        }
    }
    
    spin_destroy(&g_pool.lock);
    g_pool.ready = 0;
}

/* Acquire a buffer from the pool, returns NULL if none available */
static void* get_buf(int *idx) {
    spin_lock(&g_pool.lock);
    
    uint32_t m = atomic_load_explicit(&g_pool.mask, memory_order_acquire);
    
    for (int i = 0; i < max_parts; i++) {
        if (!(m & (1U << i))) {
            uint32_t nm = m | (1U << i);
            atomic_store_explicit(&g_pool.mask, nm, memory_order_release);
            *idx = i;
            spin_unlock(&g_pool.lock);
            memset(g_pool.bufs[i], 0, g_buf_size);
            return g_pool.bufs[i];
        }
    }
    
    spin_unlock(&g_pool.lock);
    *idx = -1;
    return NULL;
}

/* Return a buffer to the pool for reuse */
static void put_buf(int idx) {
    if (idx < 0 || idx >= max_parts) return;
    
    spin_lock(&g_pool.lock);
    uint32_t m = atomic_load_explicit(&g_pool.mask, memory_order_acquire);
    atomic_store_explicit(&g_pool.mask, m & ~(1U << idx), memory_order_release);
    spin_unlock(&g_pool.lock);
}

/*
 * Write buffer structure for batching writes.
 * Accumulates data before flushing to disk to reduce
 * the number of system calls.
 */
typedef struct {
    void *data;
    _Atomic size_t used;
    curl_off_t off;
    pthread_spinlock_t lock;
    int ready;
    int buf_idx;
    _Atomic uint64_t last_flush;
} WBuf;

/* Forward declaration for session reference in connection info */
typedef struct Sess Sess;

/*
 * Per-connection download state.
 * Each part of a multi-part download has its own connection info
 * to track progress and handle errors independently.
 */
typedef struct ALIGNED(CACHE_LINE) {
    _Atomic curl_off_t next;
    curl_off_t start;
    curl_off_t end;
    _Atomic int active;
    _Atomic int retry_cnt;
    _Atomic uint64_t last_act;
} PartProg;

/*
 * Connection information structure.
 * Contains all state needed to manage a single download connection.
 */
typedef struct {
    int fd;
    _Atomic curl_off_t off;
    int part_idx;
    _Atomic uint64_t last_time;
    _Atomic curl_off_t last_dl;
    _Atomic int time_init;
    curl_off_t start;
    curl_off_t end;
    _Atomic double last_kbps;
    WBuf wbuf;
    Sess *sess;
    ErrTrack err;
} ConnInfo;

/*
 * Download session structure.
 * Manages the overall state of a file download including
 * all parts and progress tracking.
 */
struct Sess {
    char fname[MAX_PATH];
    char prog_path[MAX_PATH];
    _Atomic curl_off_t fsize;
    _Atomic int total_parts;
    PartProg parts[STRONG_MAX_PARTS];
    _Atomic int running;
    pthread_spinlock_t save_lock;
    char etag[256];
    char lastmod[256];
    _Atomic double ewma;
    _Atomic int single_mode;
    _Atomic int net_errs;
    _Atomic double last_kbps;
    _Atomic double last_eta;
    _Atomic uint64_t start_time;
    _Atomic int complete;
    pthread_spinlock_t etag_lock;
    _Atomic curl_off_t saved_bytes;
    _Atomic uint64_t last_adapt;
    _Atomic uint64_t last_sync;
};

/* Simple structure to pass download parameters */
typedef struct {
    const char *url;
    double size;
} DlItem;

/* Information gathered from HTTP HEAD request */
typedef struct {
    curl_off_t len;
    int ranges;
    char etag[256];
    char lastmod[256];
} HeadInfo;

/*
 * CURL handle pool for reusing connections.
 * Avoids the overhead of creating new handles for each request.
 */
typedef struct {
    CURL *h[STRONG_MAX_PARTS];
    _Atomic uint32_t avail;
    pthread_spinlock_t lock;
    int ready;
    int size;
} CurlPool;

static CurlPool g_curl = {0};

/* Clean up all CURL handles in the pool */
static void cleanup_curl_pool(void) {
    if (g_share) {
        curl_share_cleanup(g_share);
        g_share = NULL;
    }
    if (!g_curl.ready) return;
    
    spin_lock(&g_curl.lock);
    for (int i = 0; i < g_curl.size; i++) {
        if (g_curl.h[i]) {
            curl_easy_cleanup(g_curl.h[i]);
            g_curl.h[i] = NULL;
        }
    }
    spin_unlock(&g_curl.lock);
    spin_destroy(&g_curl.lock);
    g_curl.ready = 0;
}

/* Initialize the CURL handle pool with shared settings */
static int init_curl_pool(void) {
    g_share = curl_share_init();
    if (!g_share) return -1;
    
    curl_share_setopt(g_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(g_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
    curl_share_setopt(g_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);

    if (spin_init(&g_curl.lock, PTHREAD_PROCESS_PRIVATE) != 0) {
        return -1;
    }
    
    g_curl.size = max_parts;
    
    for (int i = 0; i < g_curl.size; i++) {
        g_curl.h[i] = curl_easy_init();
        if (!g_curl.h[i]) {
            for (int j = 0; j < i; j++) {
                curl_easy_cleanup(g_curl.h[j]);
            }
            spin_destroy(&g_curl.lock);
            return -1;
        }
    }
    
    atomic_store(&g_curl.avail, (1U << g_curl.size) - 1);
    g_curl.ready = 1;
    
    return init_buf_pool();
}

/* Get a CURL handle from the pool or create a new one */
static CURL* get_curl(int *idx) {
    spin_lock(&g_curl.lock);
    
    uint32_t m = atomic_load(&g_curl.avail);
    for (int i = 0; i < g_curl.size; i++) {
        if (m & (1U << i)) {
            atomic_store(&g_curl.avail, m & ~(1U << i));
            *idx = i;
            spin_unlock(&g_curl.lock);
            curl_easy_reset(g_curl.h[i]);
            return g_curl.h[i];
        }
    }
    
    spin_unlock(&g_curl.lock);
    *idx = -1;
    return curl_easy_init();
}

/* Return a CURL handle to the pool */
static void put_curl(int idx, CURL *h) {
    if (idx >= 0 && idx < g_curl.size) {
        spin_lock(&g_curl.lock);
        uint32_t m = atomic_load(&g_curl.avail);
        atomic_store(&g_curl.avail, m | (1U << idx));
        spin_unlock(&g_curl.lock);
    } else if (h) {
        curl_easy_cleanup(h);
    }
}

/* User agent strings to rotate between requests */
static const char *ua_list[] = {
    "rax/2.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "curl/8.2.0"
};

/* Select a random user agent string */
static FORCE_INLINE const char* pick_ua(void) {
    return ua_list[rand() % 3];
}

/* Get current time in milliseconds using monotonic clock */
static FORCE_INLINE uint64_t get_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* Calculate time difference in milliseconds */
static FORCE_INLINE double ms_diff(uint64_t now, uint64_t before) {
    return (double)(now - before);
}

/* byte formatting */
static const char *units[] = {"B", "KB", "MB", "GB", "TB"};

/* format with appropriate unit */
static void hSize(double bytes, char *out, size_t n) {
    int i = 0;
    double v = bytes < 0 ? 0 : bytes;
    
    while (v >= 1024.0 && i < 4) {
        v /= 1024.0;
        i++;
    }
    
    snprintf(out, n, "%.2f %s", v, units[i]);
}

/* Query terminal width for progress bar formatting */
static void get_term_size(void) {
    struct winsize w;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
        atomic_store(&g_term_width, (int)w.ws_col);
    }
}

/* Create download directory if it does not exist */
static int ensure_dir(void) {
    struct stat st;
    
    if (stat(g_dir, &st) == -1) {
        if (mkdir(g_dir, 0755) == -1) {
            fprintf(stderr, "Error creating directory: %s\n", strerror(errno));
            return -1;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Path is not a directory: %s\n", g_dir);
        return -1;
    }
    
    return 0;
}

/* Check Linux network tuning parameters and print recommendations */
static void check_sys_tuning(void) {
    #ifdef __linux__
    
    fprintf(stderr, "\n[INFO] Checking system settings:\n");
    
    FILE *f = fopen("/proc/sys/net/ipv4/tcp_rmem", "r");
    if (f) {
        int min, def, max;
        if (fscanf(f, "%d %d %d", &min, &def, &max) == 3) {
            fprintf(stderr, "   TCP rmem: min=%d default=%d max=%d\n", min, def, max);
            
            if (max < 16*1024*1024) {
                fprintf(stderr, "   [WARN] Consider increasing tcp_rmem max:\n");
                fprintf(stderr, "      sudo sysctl -w net.ipv4.tcp_rmem='4096 131072 16777216'\n");
            }
        }
        fclose(f);
    }
    
    f = fopen("/proc/sys/net/ipv4/tcp_window_scaling", "r");
    if (f) {
        int en;
        if (fscanf(f, "%d", &en) == 1 && !en) {
            fprintf(stderr, "   [WARN] TCP window scaling is disabled\n");
            fprintf(stderr, "      sudo sysctl -w net.ipv4.tcp_window_scaling=1\n");
        }
        fclose(f);
    }
    
    f = fopen("/proc/sys/net/ipv4/tcp_congestion_control", "r");
    if (f) {
        char cc[32];
        if (fscanf(f, "%31s", cc) == 1) {
            fprintf(stderr, "   TCP Congestion Control: %s\n", cc);
            
            if (strcmp(cc, "bbr") != 0) {
                fprintf(stderr, "   [TIP] Enable BBR for better speed:\n");
                fprintf(stderr, "      sudo modprobe tcp_bbr\n");
                fprintf(stderr, "      sudo sysctl -w net.ipv4.tcp_congestion_control=bbr\n");
            }
        }
        fclose(f);
    }
    
    fprintf(stderr, "\n");
    
    #endif
}

/* Determine if an error is retryable and calculate backoff delay */
static int should_retry(ErrTrack *t, ErrType type) {
    if (!t) return 0;
    
    if (t->type != type) {
        t->type = type;
        t->count = 0;
        t->backoff = BACKOFF_INIT_MS;
    }
    
    t->count++;
    
    if (t->count > retry_max) return 0;
    if (type == ERR_AUTH && t->count > 3) return 0;
    
    if (t->backoff > backoff_max) {
        t->backoff = backoff_max;
    }
    
    t->last_ms = get_ms();

    int base = t->backoff;
    int jitter = (rand() % (base / 2)) - (base / 4);
    int sleep_ms = (rand() % base) + jitter;
    
    if (sleep_ms < 100) sleep_ms = 100;
    if (sleep_ms > backoff_max) sleep_ms = backoff_max;
    
    fprintf(stderr, "Waiting %d ms before retry...\n", sleep_ms);
    
    t->backoff = base * 2;
    t->last_ms = get_ms();

    return 1;
}

/* Initialize a write buffer structure */
static int init_wbuf(WBuf *wb) {
    if (!wb) return -1;
    
    int idx = -1;
    void *buf = get_buf(&idx);
    
    if (!buf) {
        if (g_direct_io) {
            #if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L
            if (posix_memalign(&buf, SECTOR_SZ, g_buf_size) != 0) {
                return -1;
            }
            #else
            #ifdef __GLIBC__
            buf = memalign(SECTOR_SZ, g_buf_size);
            #else
            buf = malloc(g_buf_size);
            #endif
            if (!buf) return -1;
            #endif
        } else {
            buf = malloc(g_buf_size);
            if (!buf) return -1;
        }
        idx = -1;
    }
    
    wb->data = buf;
    wb->buf_idx = idx;
    atomic_store(&wb->used, 0);
    wb->off = 0;
    atomic_store(&wb->last_flush, get_ms());
    
    if (spin_init(&wb->lock, PTHREAD_PROCESS_PRIVATE) != 0) {
        if (idx >= 0) {
            put_buf(idx);
        } else {
            free(buf);
        }
        return -1;
    }
    
    wb->ready = 1;
    return 0;
}

/* Destroy a write buffer and release resources */
static void destroy_wbuf(WBuf *wb) {
    if (!wb || !wb->ready) return;
    
    spin_destroy(&wb->lock);
    
    if (wb->buf_idx >= 0) {
        put_buf(wb->buf_idx);
    } else if (wb->data) {
        free(wb->data);
    }
    
    wb->data = NULL;
    wb->ready = 0;
}

/*
 * Robust write function with retry logic.
 * Handles partial writes and temporary errors gracefully.
 */
static ssize_t robust_pwrite(int fd, const void *buf, size_t cnt, off_t off) {
    if (!buf || cnt == 0) return 0;
    
    const char *p = (const char*)buf;
    size_t left = cnt;
    int retry = 0;
    
    if (g_direct_io) {
        if (!is_aligned(buf, SECTOR_SZ)) {
            fprintf(stderr, "Warning: Buffer not aligned: %p\n", buf);
        }
        
        if (off % SECTOR_SZ != 0) {
            fprintf(stderr, "Warning: Offset not aligned: %ld\n", (long)off);
        }
        
        left = round_down(cnt, SECTOR_SZ);
        
        if (left == 0 && cnt > 0) {
            static char abuf[SECTOR_SZ] __attribute__((aligned(SECTOR_SZ)));
            size_t to_write = (cnt < SECTOR_SZ) ? cnt : SECTOR_SZ;
            memcpy(abuf, buf, to_write);
            
            if (to_write < SECTOR_SZ) {
                memset(abuf + to_write, 0, SECTOR_SZ - to_write);
            }
            
            ssize_t w = pwrite(fd, abuf, SECTOR_SZ, off);
            if (w < 0) {
                atomic_fetch_add(&g_stats.disk_errs, 1);
                return -1;
            }
            
            atomic_fetch_add(&g_stats.bytes_written, (uint64_t)to_write);
            return (ssize_t)to_write;
        }
    }
    
    while (left > 0 && retry < 5) {
        ssize_t w = pwrite(fd, p, left, off);
        
        if (likely(w > 0)) {
            left -= w;
            p += w;
            off += w;
            retry = 0;
        } else if (w == 0) {
            if (++retry >= 5) break;
            usleep(1000);
        } else {
            if (errno == EINTR) {
                continue;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (++retry >= 5) break;
                usleep(2000);
            } else if (errno == EINVAL && g_direct_io) {
                fprintf(stderr, "EINVAL error - likely alignment issue\n");
                atomic_fetch_add(&g_stats.disk_errs, 1);
                return -1;
            } else {
                atomic_fetch_add(&g_stats.disk_errs, 1);
                return -1;
            }
        }
    }
    
    ssize_t written = (ssize_t)(cnt - left);
    atomic_fetch_add(&g_stats.bytes_written, (uint64_t)written);
    
    return written;
}

/* Flush accumulated data from write buffer to disk */
static int flush_wbuf(ConnInfo *ci) {
    if (!ci || !ci->wbuf.ready) return -1;
    
    spin_lock(&ci->wbuf.lock);
    
    size_t used = atomic_load(&ci->wbuf.used);
    if (used == 0 || !ci->wbuf.data) {
        spin_unlock(&ci->wbuf.lock);
        return 0;
    }
    
    if (used > g_buf_size) used = g_buf_size;
    
    curl_off_t off = ci->wbuf.off;
    void *data = ci->wbuf.data;
    size_t write_sz = used;
    int need_pad = 0;
    
    if (g_direct_io) {
        if (off % SECTOR_SZ != 0) {
            spin_unlock(&ci->wbuf.lock);
            return -1;
        }
        
        curl_off_t end_pos = off + used - 1;
        int is_last = (end_pos >= ci->end - SECTOR_SZ);
        
        if (!is_last) {
            write_sz = (used / SECTOR_SZ) * SECTOR_SZ;
            
            if (write_sz == 0) {
                spin_unlock(&ci->wbuf.lock);
                return 0;
            }
            
            size_t rem = used - write_sz;
            if (rem > 0) {
                atomic_store(&ci->wbuf.used, rem);
                ci->wbuf.off = off + write_sz;
                memmove(ci->wbuf.data, (char*)ci->wbuf.data + write_sz, rem);
            } else {
                atomic_store(&ci->wbuf.used, 0);
            }
        } else {
            write_sz = ((used + SECTOR_SZ - 1) / SECTOR_SZ) * SECTOR_SZ;
            if (write_sz > used) {
                memset((char*)data + used, 0, write_sz - used);
                need_pad = 1;
            }
            atomic_store(&ci->wbuf.used, 0);
        }
    } else {
        atomic_store(&ci->wbuf.used, 0);
    }
    
    spin_unlock(&ci->wbuf.lock);
    
    ssize_t w = robust_pwrite(ci->fd, data, write_sz, (off_t)off);
    if (unlikely(w < 0)) return -1;
    
    if (need_pad && w > (ssize_t)used) {
        atomic_fetch_add(&g_stats.bytes_written, (uint64_t)used);
    } else {
        atomic_fetch_add(&g_stats.bytes_written, (uint64_t)w);
    }
    
    atomic_store(&ci->wbuf.last_flush, get_ms());
    return 0;
}

/*
 * Input thread for handling user keyboard commands.
 * Allows pausing or stopping the download with p or q keys.
 */
static void* input_thread(void* arg) {
    (void)arg;
    
    #if defined(__linux__)
    prctl(PR_SET_NAME, "input", 0, 0, 0);
    #endif
    
    struct termios oldt, newt;
    if (tcgetattr(STDIN_FILENO, &oldt) != 0) return NULL;
    
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    
    int flags = fcntl(STDIN_FILENO, F_GETFL);
    fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK);
    
    while (atomic_load(&g_stop) == 0) {
        fd_set fds;
        struct timeval tv = {1, 0};
        
        FD_ZERO(&fds);
        FD_SET(STDIN_FILENO, &fds);
        
        if (select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv) > 0) {
            int c = getchar();
            if (c == 'p' || c == 'P' || c == 'q' || c == 'Q') {
                atomic_store(&g_stop, 1);
                fprintf(stderr, "\n\n[PAUSED]\n");
                break;
            }
        }
    }
    
    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    fcntl(STDIN_FILENO, F_SETFL, flags);
    return NULL;
}

/* Extract filename from URL path */
static void get_fname_from_url(const char *url, char *out, size_t sz) {
    if (!url || !out || sz < 2) return;
    
    const char *slash = strrchr(url, '/');
    const char *name = (slash && slash[1]) ? slash + 1 : "download.bin";
    
    const char *q = strchr(name, '?');
    size_t len = q ? (size_t)(q - name) : strlen(name);
    
    strncpy(out, name, len);
    out[len] = '\0';
}

/* Callback to discard response body data */
static size_t discard_cb(void *p, size_t sz, size_t n, void *ud) {
    (void)p; (void)ud;
    return sz * n;
}

/* Callback to parse HTTP headers for file information */
static size_t header_cb(char *buf, size_t sz, size_t n, void *ud) {
    HeadInfo *hi = (HeadInfo*)ud;
    size_t len = sz * n;
    
    if (len >= 15 && strncasecmp(buf, "Content-Length:", 15) == 0) {
        hi->len = strtoll(buf + 15, NULL, 10);
    } else if (len >= 14 && strncasecmp(buf, "Accept-Ranges:", 14) == 0) {
        if (strstr(buf + 14, "bytes")) hi->ranges = 1;
    } else if (len >= 5 && strncasecmp(buf, "ETag:", 5) == 0) {
        char *v = buf + 5;
        while (*v == ' ') v++;
        snprintf(hi->etag, sizeof(hi->etag), "%.*s", (int)(len - (v - buf)), v);
        hi->etag[strcspn(hi->etag, "\r\n")] = 0;
    } else if (len >= 14 && strncasecmp(buf, "Last-Modified:", 14) == 0) {
        char *v = buf + 14;
        while (*v == ' ') v++;
        snprintf(hi->lastmod, sizeof(hi->lastmod), "%.*s", (int)(len - (v - buf)), v);
        hi->lastmod[strcspn(hi->lastmod, "\r\n")] = 0;
    }
    
    return len;
}

/* Perform HTTP HEAD request to get file metadata */
static int head_probe(const char *url, HeadInfo *out) {
    memset(out, 0, sizeof(*out));
    
    CURL *c = curl_easy_init();
    if (!c) return -1;
    
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, header_cb);
    curl_easy_setopt(c, CURLOPT_HEADERDATA, out);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, discard_cb);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_MAXREDIRS, 5L);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, (long)conn_timeout);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, (long)HEAD_TIMEOUT);
    curl_easy_setopt(c, CURLOPT_USERAGENT, ua_list[0]);
    curl_easy_setopt(c, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    
    CURLcode rc = curl_easy_perform(c);
    curl_easy_cleanup(c);
    
    return (rc == CURLE_OK) ? 0 : -1;
}

/*
 * CURL write callback to receive downloaded data.
 * Buffers data before writing to disk for better performance.
 */
static size_t write_cb(void *ptr, size_t sz, size_t nmemb, void *ud) {
    ConnInfo *ci = (ConnInfo*)ud;
    size_t bytes = sz * nmemb;
    
    if (!ci->wbuf.ready || bytes == 0) return 0;
    
    curl_off_t cur = atomic_load(&ci->off);
    
    if (ci->end >= ci->start) {
        if (cur + (curl_off_t)bytes - 1 > ci->end) {
            if (cur > ci->end) return 0;
            bytes = (ci->end - cur) + 1;
        }
    }
    
    spin_lock(&ci->wbuf.lock);
    
    size_t used = atomic_load(&ci->wbuf.used);
    
    if (used > 0 && (used + bytes > g_buf_size ||
        cur != ci->wbuf.off + (curl_off_t)used)) {
        spin_unlock(&ci->wbuf.lock);
        flush_wbuf(ci);
        spin_lock(&ci->wbuf.lock);
        used = 0;
    }
    
    if (used == 0) {
        ci->wbuf.off = cur;
    }
    
    memcpy((char*)ci->wbuf.data + used, ptr, bytes);
    atomic_store(&ci->wbuf.used, used + bytes);
    atomic_store(&ci->off, cur + bytes);
    
    used = atomic_load(&ci->wbuf.used);
    spin_unlock(&ci->wbuf.lock);
    
    if (used > g_flush_thresh) {
        flush_wbuf(ci);
    }
    
    return bytes;
}

/*
 * CURL progress callback to track download speed.
 * Also checks for user stop requests.
 */
static int prog_cb(void *p, curl_off_t dltot, curl_off_t dlnow, 
                   curl_off_t ultot, curl_off_t ulnow) {
    (void)ultot; (void)ulnow; (void)dltot;
    
    ConnInfo *ci = (ConnInfo*)p;
    uint64_t now = get_ms();
    
    if (!atomic_load(&ci->time_init)) {
        atomic_store(&ci->last_time, now);
        atomic_store(&ci->last_dl, dlnow);
        atomic_store(&ci->time_init, 1);
        return atomic_load(&g_stop);
    }
    
    uint64_t last = atomic_load(&ci->last_time);
    double elapsed = ms_diff(now, last);
    
    if (elapsed >= 1000.0) {
        curl_off_t last_dl = atomic_load(&ci->last_dl);
        double kbps = 0.0;
        
        if (elapsed > 0 && dlnow > last_dl) {
            kbps = ((double)(dlnow - last_dl) / elapsed) * (1000.0 / 1024.0);
        }
        
        atomic_store(&ci->last_kbps, kbps);
        
        double peak = atomic_load(&g_stats.peak_kbps);
        if (kbps > peak) {
            atomic_store(&g_stats.peak_kbps, kbps);
        }
        
        atomic_store(&ci->last_time, now);
        atomic_store(&ci->last_dl, dlnow);
    }
    
    return atomic_load(&g_stop);
}

/* Generate progress file path from filename */
static void make_prog_path(const char *fn, char *out, size_t sz) {
    snprintf(out, sz, "%s.RAX", fn);
}

/* Save download progress to JSON file for resume capability */
static int save_progress(Sess *S) {
    char tmp[MAX_PATH];
    snprintf(tmp, sizeof(tmp), "%s.tmp", S->prog_path);
    
    FILE *f = fopen(tmp, "w");
    if (!f) return -1;
    
    fprintf(f, "{\n\"file_size\":%" PRId64 ",\n", 
           (int64_t)atomic_load(&S->fsize));
    fprintf(f, "\"total_parts\":%d,\n", atomic_load(&S->total_parts));
    
    spin_lock(&S->etag_lock);
    fprintf(f, "\"etag\":\"%s\",\n", S->etag);
    fprintf(f, "\"last_modified\":\"%s\",\n", S->lastmod);
    spin_unlock(&S->etag_lock);
    
    fprintf(f, "\"parts\":[\n");
    
    int tot = atomic_load(&S->total_parts);
    for (int i = 0; i < tot; i++) {
        fprintf(f, "{\"idx\":%d,\"start\":%" PRId64 ",\"end\":%" PRId64 
               ",\"next\":%" PRId64 "}%s\n",
               i, (int64_t)S->parts[i].start, (int64_t)S->parts[i].end,
               (int64_t)atomic_load(&S->parts[i].next),
               i == tot-1 ? "" : ",");
    }
    
    fprintf(f, "]\n}\n");
    
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    
    return rename(tmp, S->prog_path) == 0 ? 0 : -1;
}

/* Load previous download progress from file */
static int load_progress(Sess *S) {
    FILE *f = fopen(S->prog_path, "r");
    if (!f) return -1;
    
    char *buf = malloc(PROGRESS_JSON_MAX);
    if (!buf) {
        fclose(f);
        return -1;
    }
    
    size_t r = fread(buf, 1, PROGRESS_JSON_MAX - 1, f);
    fclose(f);
    
    if (r == 0) {
        free(buf);
        return -1;
    }
    buf[r] = 0;
    
    int64_t fsz;
    int parts;
    
    char *ptr = strstr(buf, "file_size");
    if (!ptr || sscanf(ptr, "file_size\":%" SCNd64, &fsz) != 1) {
        free(buf);
        return -1;
    }
    
    ptr = strstr(buf, "total_parts");
    if (!ptr || sscanf(ptr, "total_parts\":%d", &parts) != 1 ||
        parts < 1 || parts > max_parts) {
        free(buf);
        return -1;
    }
    
    atomic_store(&S->fsize, fsz);
    atomic_store(&S->total_parts, parts);
    
    char *p = strstr(buf, "\"parts\"");
    for (int i = 0; i < parts && p; i++) {
        p = strstr(p, "{\"idx\"");
        if (!p) break;
        
        int idx;
        int64_t st, en, nx;
        
        if (sscanf(p, "{\"idx\":%d,\"start\":%" SCNd64 ",\"end\":%" SCNd64 
                  ",\"next\":%" SCNd64, &idx, &st, &en, &nx) == 4) {
            if (idx >= 0 && idx < parts) {
                S->parts[idx].start = st;
                S->parts[idx].end = en;
                atomic_store(&S->parts[idx].next, nx);
                atomic_store(&S->parts[idx].active, 1);
            }
        }
        p++;
    }
    
    free(buf);
    return 0;
}

/* Check if all parts have been downloaded */
static int is_complete(const Sess *S) {
    int tot = atomic_load(&S->total_parts);
    for (int i = 0; i < tot; i++) {
        if (S->parts[i].end < S->parts[i].start) return 0;
        if (atomic_load(&S->parts[i].next) <= S->parts[i].end) return 0;
    }
    return 1;
}

/* Apply filesystem hints for sequential read optimization */
static void opt_disk(int fd, curl_off_t sz) {
    if (fd < 0) return;
    
    #ifdef POSIX_FADV_SEQUENTIAL
    if (!g_direct_io) {
        posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    }
    #endif
    
    #ifdef POSIX_FADV_WILLNEED
    if (!g_direct_io && sz < 100*1024*1024) {
        posix_fadvise(fd, 0, sz, POSIX_FADV_WILLNEED);
    }
    #endif
    
    if (g_direct_io) {
        fprintf(stderr, "[OK] Direct IO enabled - Page Cache bypassed\n");
    }
}

/* Divide file into parts for parallel download */
static void plan_parts(Sess *S) {
    curl_off_t fsz = atomic_load(&S->fsize);
    
    int parts;
    if (fsz <= 0) {
        parts = 1;
    } else if (fsz < 10*1024*1024) {
        parts = min_parts;
    } else if (fsz < 100*1024*1024) {
        parts = def_parts;
    } else {
        parts = max_parts;
    }
    
    atomic_store(&S->total_parts, parts);
    
    curl_off_t psz;
    
    if (g_direct_io && fsz > 0) {
        curl_off_t raw = fsz / parts;
        psz = (raw / SECTOR_SZ) * SECTOR_SZ;
        
        if (psz < SECTOR_SZ) {
            psz = SECTOR_SZ;
        }
        
        fprintf(stderr, "[OK] Part boundaries aligned to %d bytes\n", SECTOR_SZ);
    } else {
        psz = (fsz > 0) ? (fsz / parts) : 0;
    }
    
    curl_off_t start = 0;
    
    for (int i = 0; i < parts; i++) {
        S->parts[i].start = start;
        
        if (i == parts - 1) {
            S->parts[i].end = fsz - 1;
        } else {
            curl_off_t nxt = start + psz;
            
            if (g_direct_io) {
                nxt = (nxt / SECTOR_SZ) * SECTOR_SZ;
            }
            
            S->parts[i].end = nxt - 1;
        }
        
        if (fsz <= 0) S->parts[i].end = -1;
        
        atomic_store(&S->parts[i].next, start);
        atomic_store(&S->parts[i].active, 1);
        atomic_store(&S->parts[i].retry_cnt, 0);
        
        start = S->parts[i].end + 1;
    }
}

/* Update part progress from connection info */
static void update_part_prog(Sess *S, const ConnInfo* ci) {
    curl_off_t new_off = atomic_load(&ci->off);
    curl_off_t cur = atomic_load(&S->parts[ci->part_idx].next);
    
    while (new_off > cur) {
        if (atomic_compare_exchange_weak(&S->parts[ci->part_idx].next,
                                         &cur, new_off)) {
            atomic_store(&S->parts[ci->part_idx].last_act, get_ms());
            break;
        }
    }
}

/* Calculate total bytes downloaded across all parts */
static curl_off_t total_done(Sess *S) {
    curl_off_t sum = 0;
    int tot = atomic_load(&S->total_parts);
    
    for (int i = 0; i < tot; i++) {
        curl_off_t next = atomic_load(&S->parts[i].next);
        curl_off_t done = next - S->parts[i].start;
        
        if (done < 0) done = 0;
        
        if (S->parts[i].end >= S->parts[i].start) {
            curl_off_t len = S->parts[i].end - S->parts[i].start + 1;
            if (done > len) done = len;
        }
        
        sum += done;
    }
    
    return sum;
}

/* Update exponentially weighted moving average of speed */
static void update_ewma(Sess *S, double kbps) {
    double cur = atomic_load(&S->ewma);
    double nv = (cur <= 0) ? kbps : 
                (EWMA_ALPHA * kbps + (1.0 - EWMA_ALPHA) * cur);
    atomic_store(&S->ewma, nv);
}

/* Adjust number of concurrent connections based on speed */
static void maybe_adapt(Sess *S, double kbps_tot) {
    if (atomic_load(&S->single_mode)) return;
    
    int run = atomic_load(&S->running);
    double per = (run > 0) ? (kbps_tot / run) : kbps_tot;
    
    update_ewma(S, per);
    
    uint64_t now = get_ms();
    uint64_t last = atomic_load(&S->last_adapt);
    
    if (last && ms_diff(now, last) < ADAPT_COOLDOWN_MS) return;
    
    double smooth = atomic_load(&S->ewma);
    int tot = atomic_load(&S->total_parts);
    
    if (smooth > BASELINE_KBPS * 2.0 && run < tot && run < max_parts) {
        atomic_fetch_add(&S->running, 1);
        atomic_store(&S->last_adapt, now);
    } else if (smooth < BASELINE_KBPS * 0.5 && run > min_parts) {
        atomic_fetch_sub(&S->running, 1);
        atomic_store(&S->last_adapt, now);
    }
}

/* Check if CURL error is a network-related issue */
static int is_net_err(CURLcode rc, long http) {
    switch (rc) {
        case CURLE_COULDNT_CONNECT:
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_OPERATION_TIMEDOUT:
        case CURLE_RECV_ERROR:
        case CURLE_SEND_ERROR:
            atomic_fetch_add(&g_stats.net_errs, 1);
            return 1;
        default:
            break;
    }
    
    return (http >= 500 || http == 408 || http == 429);
}

/* Socket options callback for TCP tuning */
static int sockopt_cb(void *p, curl_socket_t fd, curlsocktype purpose) {
    (void)purpose;
    
    #ifdef TCP_CONGESTION
    const char *cc = "bbr";
    if (setsockopt(fd, IPPROTO_TCP, TCP_CONGESTION, cc, strlen(cc)) < 0) {
        cc = "htcp";
        setsockopt(fd, IPPROTO_TCP, TCP_CONGESTION, cc, strlen(cc));
    }
    #endif
    
    if (!p) return 0;
    
    long bufsz = *(long*)p;
    
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof(bufsz));
    
    #ifdef TCP_WINDOW_CLAMP
    int clamp = 0;
    setsockopt(fd, IPPROTO_TCP, TCP_WINDOW_CLAMP, &clamp, sizeof(clamp));
    #endif
    
    #ifdef TCP_QUICKACK
    int qa = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &qa, sizeof(qa));
    #endif
    
    return CURL_SOCKOPT_OK;
}

/* Create and configure a CURL handle for downloading a part */
static int create_handle(CURLM *multi, CURL **out, ConnInfo *ci,
                         const char *url, int idx, Sess *S,
                         int fd, int *cidx) {
    curl_off_t next = atomic_load(&S->parts[idx].next);
    
    if (S->parts[idx].end >= S->parts[idx].start && 
        next > S->parts[idx].end) {
        return -2;
    }
    
    CURL *c = get_curl(cidx);
    if (!c) return -1;
    
    curl_easy_setopt(c, CURLOPT_SHARE, g_share);
    curl_easy_setopt(c, CURLOPT_MAXCONNECTS, 10L);
    curl_easy_setopt(c, CURLOPT_DNS_CACHE_TIMEOUT, 300L);
    
    #ifdef CURLOPT_DOH_URL
    if (getenv("USE_DOH")) {
        curl_easy_setopt(c, CURLOPT_DOH_URL, "https://1.1.1.1/dns-query");
    }
    #endif

    curl_easy_setopt(c, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_WHATEVER);

    char range[64];
    if (atomic_load(&S->single_mode)) {
        snprintf(range, sizeof(range), "%" PRId64 "-", (int64_t)next);
    } else {
        snprintf(range, sizeof(range), "%" PRId64 "-%" PRId64,
                (int64_t)next, (int64_t)S->parts[idx].end);
    }
    
    if (init_wbuf(&ci->wbuf) < 0) {
        put_curl(*cidx, c);
        return -1;
    }
    
    ci->fd = fd;
    atomic_store(&ci->off, next);
    ci->part_idx = idx;
    atomic_store(&ci->time_init, 0);
    ci->start = S->parts[idx].start;
    ci->end = S->parts[idx].end;
    ci->sess = S;
    memset(&ci->err, 0, sizeof(ci->err));
    
    curl_easy_setopt(c, CURLOPT_URL, url);
    if (strlen(range) > 0) curl_easy_setopt(c, CURLOPT_RANGE, range);

    long tcp_buf = 2 * 1024 * 1024;
    curl_easy_setopt(c, CURLOPT_SOCKOPTFUNCTION, sockopt_cb);
    curl_easy_setopt(c, CURLOPT_SOCKOPTDATA, &tcp_buf);

    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, ci);
    curl_easy_setopt(c, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(c, CURLOPT_XFERINFOFUNCTION, prog_cb);
    curl_easy_setopt(c, CURLOPT_XFERINFODATA, ci);
    
    curl_easy_setopt(c, CURLOPT_BUFFERSIZE, (long)CURL_BUF_SIZE);
    curl_easy_setopt(c, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_MAXREDIRS, 5L);
    
    curl_easy_setopt(c, CURLOPT_USERAGENT, pick_ua());
    curl_easy_setopt(c, CURLOPT_LOW_SPEED_LIMIT, (long)LOW_SPEED_BPS);
    curl_easy_setopt(c, CURLOPT_LOW_SPEED_TIME, (long)low_speed_time);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, (long)conn_timeout);
    
    curl_easy_setopt(c, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(c, CURLOPT_TCP_KEEPALIVE, 1L);
    
    if (curl_multi_add_handle(multi, c) != CURLM_OK) {
        destroy_wbuf(&ci->wbuf);
        put_curl(*cidx, c);
        return -1;
    }
    
    *out = c;
    return 0;
}

/* Print download progress to terminal */
static void print_prog(Sess *S, ConnInfo *infos, const char *url) {
    static uint64_t last_print = 0;
    
    uint64_t now = get_ms();
    if (last_print && ms_diff(now, last_print) < PRINT_MS) return;
    
    curl_off_t fsz = atomic_load(&S->fsize);
    curl_off_t done = total_done(S);
    double pct = (fsz > 0) ? ((double)done / fsz * 100.0) : 0.0;
    
    double tot_kbps = 0.0;
    int active = 0;
    int tot = atomic_load(&S->total_parts);
    
    for (int i = 0; i < tot; i++) {
        if (atomic_load(&infos[i].time_init)) {
            tot_kbps += atomic_load(&infos[i].last_kbps);
            active++;
        }
    }
    
    double eta = 0.0;
    if (fsz > 0 && tot_kbps > ETA_MIN_KBPS) {
        eta = ((double)(fsz - done) / 1024.0) / tot_kbps;
    }
    
    char done_h[32], tot_h[32];
    hSize((double)done, done_h, sizeof(done_h));
    hSize((double)fsz, tot_h, sizeof(tot_h));
    
    const char *fn = strrchr(url, '/');
    fn = fn ? fn + 1 : url;
    
    fprintf(stderr, "\r %-25.25s %5.1f%% | %8s/%8s | %7.1f KB/s | %d/%d | ETA:%4.0fs",
           fn, pct, done_h, fsz > 0 ? tot_h : "?", 
           tot_kbps, active, tot, eta);
    
    fflush(stderr);
    last_print = now;
}

/* Save progress periodically if enough data has been downloaded */
static void persist_check(Sess *S) {
    static uint64_t last_save = 0;
    uint64_t now = get_ms();
    
    if (last_save == 0) {
        last_save = now;
        return;
    }
    
    curl_off_t cur = total_done(S);
    curl_off_t last = atomic_load(&S->saved_bytes);
    
    if (ms_diff(now, last_save) >= PROGRESS_FLUSH_MS ||
        cur - last >= PROGRESS_SAVE_DELTA) {
        
        if (spin_trylock(&S->save_lock) == 0) {
            save_progress(S);
            spin_unlock(&S->save_lock);
            last_save = now;
            atomic_store(&S->saved_bytes, cur);
        }
    }
}

/* Sync file to disk periodically for large files */
static void sync_check(Sess *S, int fd) {
    uint64_t now = get_ms();
    uint64_t last = atomic_load(&S->last_sync);
    
    if (last == 0) {
        atomic_store(&S->last_sync, now);
        return;
    }
    
    if (ms_diff(now, last) >= g_sync_ms) {
        curl_off_t fsz = atomic_load(&S->fsize);
        if (fsz > 50*1024*1024) {
            fdatasync(fd);
        }
        atomic_store(&S->last_sync, now);
    }
}

/* Print final download statistics */
static void print_stats(void) {
    fprintf(stderr, "\n\n[STATS]\n");
    
    char h[32];
    hSize((double)atomic_load(&g_stats.bytes_written), h, sizeof(h));
    fprintf(stderr, "   Downloaded: %s\n", (h ));
    fprintf(stderr, "   Retries: %" PRIu64 "\n", 
           atomic_load(&g_stats.retries));
    fprintf(stderr, "   Peak speed: %.1f KB/s\n", 
           atomic_load(&g_stats.peak_kbps));
}

#ifdef __linux__
/* Set I/O scheduling priority on Linux */
static void set_io_prio(int prio) {
    int ioprio = (2 << 13) | prio;
    if (syscall(SYS_ioprio_set, 1, 0, ioprio) == 0) {
        fprintf(stderr, "[OK] IO priority set: %d\n", prio);
    }
}
#endif

/* Detect disk type and configure settings accordingly */
static int detect_disk(const char *path) {
    #ifdef __linux__
    
    struct stat st;
    if (stat(path, &st) == 0) {
        unsigned int major = (st.st_dev >> 8) & 0xff;
        
        if (major == 259) {
            fprintf(stderr, "[OK] NVMe SSD detected\n");
            g_buf_size = SSD_BUF_SIZE;
            g_sync_ms = SSD_SYNC_MS;
            g_flush_thresh = SSD_FLUSH_THRESH;
            g_direct_io = SSD_DIRECT_IO;
            return 0;
        }
        
        char cmd[256];
        snprintf(cmd, sizeof(cmd), 
                "cat /sys/dev/block/%u:%u/../queue/rotational 2>/dev/null || "
                "cat /sys/dev/block/%u:%u/queue/rotational 2>/dev/null",
                major, (unsigned int)(st.st_dev & 0xff),
                major, (unsigned int)(st.st_dev & 0xff));
        
        FILE *fp = popen(cmd, "r");
        if (fp) {
            int rot = -1;
            if (fscanf(fp, "%d", &rot) == 1) {
                pclose(fp);
                
                if (rot == 0) {
                    fprintf(stderr, "[OK] SATA SSD detected\n");
                    g_buf_size = SSD_BUF_SIZE;
                    g_sync_ms = SSD_SYNC_MS;
                    g_flush_thresh = SSD_FLUSH_THRESH;
                    g_direct_io = SSD_DIRECT_IO;
                    return 0;
                } else if (rot == 1) {
                    fprintf(stderr, "[OK] HDD detected\n");
                    g_buf_size = HDD_BUF_SIZE;
                    g_sync_ms = HDD_SYNC_MS;
                    g_flush_thresh = HDD_FLUSH_THRESH;
                    g_direct_io = HDD_DIRECT_IO;
                    return 1;
                }
            }
            pclose(fp);
        }
    }
    
    #endif
    
    fprintf(stderr, "[WARN] Unknown disk type - using HDD settings\n");
    g_buf_size = HDD_BUF_SIZE;
    g_sync_ms = HDD_SYNC_MS;
    g_flush_thresh = HDD_FLUSH_THRESH;
    g_direct_io = HDD_DIRECT_IO;
    
    return -1;
}

/*
 * Main download function.
 * Manages the entire download process including multi-part
 * concurrent downloads, progress tracking, and error recovery.
 */
static void *downloader(void *arg) {
    DlItem *item = (DlItem*)arg;
    const char *url = item->url;
    
    Sess S;
    memset(&S, 0, sizeof(S));
    
    if (spin_init(&S.save_lock, PTHREAD_PROCESS_PRIVATE) != 0 ||
        spin_init(&S.etag_lock, PTHREAD_PROCESS_PRIVATE) != 0) {
        return NULL;
    }
    
    atomic_store(&S.fsize, (curl_off_t)item->size);
    atomic_store(&S.start_time, get_ms());
    
    /* Build full path with download directory */
    char basename[MAX_PATH];
    get_fname_from_url(url, basename, sizeof(basename));
    snprintf(S.fname, sizeof(S.fname), "%s%s", g_dir, basename);
    make_prog_path(S.fname, S.prog_path, sizeof(S.prog_path));
    
    fprintf(stderr, "\n[START] %s\n", S.fname);

    detect_disk(g_dir);

    FILE *fp = NULL;
    int fd = -1;
    int new_file = 0;

    if (g_direct_io && O_DIRECT != 0) {
        int flags = O_RDWR;
        
        fd = open(S.fname, flags | O_DIRECT);
        
        if (fd < 0) {
            fd = open(S.fname, flags | O_CREAT | O_DIRECT, 0644);
            new_file = 1;
        }
        
        if (fd < 0) {
            fprintf(stderr, "[ERR] Cannot open file with O_DIRECT: %s\n", strerror(errno));
            fprintf(stderr, "[INFO] Retrying without O_DIRECT...\n");
            g_direct_io = 0;
            
            fd = open(S.fname, flags | O_CREAT, 0644);
            if (fd < 0) {
                fprintf(stderr, "[ERR] %s\n", strerror(errno));
                goto cleanup;
            }
        } else {
            fprintf(stderr, "[OK] File opened with O_DIRECT\n");
        }
        
        fp = fdopen(fd, "r+b");
        if (!fp) {
            fprintf(stderr, "[ERR] fdopen failed: %s\n", strerror(errno));
            close(fd);
            goto cleanup;
        }
        
    } else {
        fp = fopen(S.fname, "r+b");
        
        if (!fp) {
            fp = fopen(S.fname, "w+b");
            new_file = 1;
            if (!fp) {
                fprintf(stderr, "[ERR] %s\n", strerror(errno));
                goto cleanup;
            }
        }
        
        fd = fileno(fp);
    }

    if (fd < 0 || flock(fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr, "[ERR] File lock failed\n");
        fclose(fp);
        goto cleanup;
    }
    
    #ifdef __linux__
    set_io_prio(4);
    #endif
    
    curl_off_t fsz = atomic_load(&S.fsize);
    if (fsz > 0) {
        #ifdef __linux__
        if (fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, fsz) != 0) {
            if (ftruncate(fd, fsz) != 0) {
                fprintf(stderr, "[WARN] Cannot set file size\n");
            } else {
                fprintf(stderr, "[OK] Sparse file created\n");
            }
        } else {
            fprintf(stderr, "[OK] Space reserved with fallocate\n");
        }
        #else
        ftruncate(fd, fsz);
        #endif
    }
    
    opt_disk(fd, fsz);
    plan_parts(&S);
    
    if (!new_file && load_progress(&S) == 0) {
        fprintf(stderr, "[INFO] Resuming download\n");
    }
    
    if (fsz <= 0) {
        HeadInfo hi;
        if (head_probe(url, &hi) == 0 && hi.len > 0) {
            atomic_store(&S.fsize, hi.len);
            plan_parts(&S);
        } else {
            atomic_store(&S.single_mode, 1);
        }
    }
    
    int incomplete = 0;
    int tot = atomic_load(&S.total_parts);
    
    for (int i = 0; i < tot; i++) {
        if (atomic_load(&S.parts[i].next) <= S.parts[i].end ||
            S.parts[i].end < S.parts[i].start) {
            incomplete++;
        }
    }
    
    if (incomplete == 0) {
        fprintf(stderr, "\n[OK] Already complete\n");
        flock(fd, LOCK_UN);
        fclose(fp);
        unlink(S.prog_path);
        goto cleanup;
    }
    
    atomic_store(&S.running, def_parts);
    if (atomic_load(&S.running) > incomplete) {
        atomic_store(&S.running, incomplete);
    }
    
    CURLM *multi = curl_multi_init();
    if (!multi) {
        flock(fd, LOCK_UN);
        fclose(fp);
        goto cleanup;
    }
    
    curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS, (long)max_parts);
    curl_multi_setopt(multi, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
    
    CURL *handles[STRONG_MAX_PARTS] = {NULL};
    int cidx[STRONG_MAX_PARTS];
    ConnInfo infos[STRONG_MAX_PARTS];
    
    memset(infos, 0, sizeof(infos));
    memset(cidx, -1, sizeof(cidx));
    
    int sched = 0;
    int target = atomic_load(&S.running);
    
    for (int i = 0; i < tot && sched < target; i++) {
        curl_off_t next = atomic_load(&S.parts[i].next);
        if (next <= S.parts[i].end || S.parts[i].end < S.parts[i].start) {
            if (create_handle(multi, &handles[i], &infos[i],
                              url, i, &S, fd, &cidx[i]) == 0) {
                sched++;
            }
        }
    }
    
    int run = 0;
    curl_multi_perform(multi, &run);
    
    uint64_t speed_win = get_ms();
    curl_off_t speed_bytes = 0;
    curl_off_t last_done = total_done(&S);
    
    /* Main download loop */
    while (atomic_load(&g_stop) == 0) {
        curl_multi_wait(multi, NULL, 0, 500, NULL);
        curl_multi_perform(multi, &run);
        
        int msgs;
        CURLMsg *msg;
        
        while ((msg = curl_multi_info_read(multi, &msgs))) {
            if (msg->msg == CURLMSG_DONE) {
                CURL *c = msg->easy_handle;
                CURLcode res = msg->data.result;
                long http = 0;
                curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http);
                
                int pidx = -1;
                for (int i = 0; i < tot; i++) {
                    if (handles[i] == c) {
                        pidx = i;
                        break;
                    }
                }
                
                if (pidx >= 0) {
                    flush_wbuf(&infos[pidx]);
                    update_part_prog(&S, &infos[pidx]);
                    destroy_wbuf(&infos[pidx].wbuf);
                }
                
                curl_multi_remove_handle(multi, c);
                
                if (pidx >= 0) {
                    put_curl(cidx[pidx], c);
                    handles[pidx] = NULL;
                }
                
                if (res == CURLE_OK) {
                    for (int j = 0; j < tot; j++) {
                        curl_off_t next = atomic_load(&S.parts[j].next);
                        if (handles[j] == NULL && 
                            (next <= S.parts[j].end || S.parts[j].end < S.parts[j].start)) {
                            create_handle(multi, &handles[j], &infos[j],
                                          url, j, &S, fd, &cidx[j]);
                            break;
                        }
                    }
                } else if (is_net_err(res, http) && pidx >= 0) {
                    if (should_retry(&infos[pidx].err, ERR_NET)) {
                        atomic_fetch_add(&g_stats.retries, 1);
                        usleep(infos[pidx].err.backoff * 1000);
                        
                        curl_off_t next = atomic_load(&S.parts[pidx].next);
                        if (next <= S.parts[pidx].end || S.parts[pidx].end < S.parts[pidx].start) {
                            create_handle(multi, &handles[pidx], &infos[pidx],
                                          url, pidx, &S, fd, &cidx[pidx]);
                        }
                    }
                }
            }
        }
        
        curl_off_t done_now = total_done(&S);
        speed_bytes += (done_now - last_done);
        last_done = done_now;
        
        print_prog(&S, infos, url);
        persist_check(&S);
        sync_check(&S, fd);
        
        uint64_t now = get_ms();
        if (ms_diff(now, speed_win) >= ADAPT_WINDOW_MS) {
            double kbps = (speed_bytes / ADAPT_WINDOW_MS) * (1000.0 / 1024.0);
            maybe_adapt(&S, kbps);
            speed_win = now;
            speed_bytes = 0;
        }
        
        if (run == 0) {
            int need = 0;
            for (int j = 0; j < tot; j++) {
                curl_off_t next = atomic_load(&S.parts[j].next);
                if (next <= S.parts[j].end || S.parts[j].end < S.parts[j].start) {
                    need = 1;
                    break;
                }
            }
            
            if (!need) break;
        }
    }
    
    /* Cleanup all handles and buffers */
    for (int i = 0; i < tot; i++) {
        if (infos[i].wbuf.ready) {
            flush_wbuf(&infos[i]);
            destroy_wbuf(&infos[i].wbuf);
        }
        if (handles[i]) {
            curl_multi_remove_handle(multi, handles[i]);
            put_curl(cidx[i], handles[i]);
        }
    }
    
    if (atomic_load(&g_stop) == 0 && is_complete(&S)) {
        fprintf(stderr, "\n\n[DONE] %s\n", S.fname);
        if (g_direct_io) {
            curl_off_t actual = atomic_load(&S.fsize);
            if (actual > 0) {
                ftruncate(fd, actual);
                fprintf(stderr, "[OK] File truncated to actual size: %ld bytes\n", 
                       (long)actual);
            }
        }
        unlink(S.prog_path);
    } else {
        fprintf(stderr, "\n\n[SAVED] Progress saved\n");
        spin_lock(&S.save_lock);
        save_progress(&S);
        spin_unlock(&S.save_lock);
    }
    
    fdatasync(fd);
    flock(fd, LOCK_UN);
    fclose(fp);
    curl_multi_cleanup(multi);

cleanup:
    spin_destroy(&S.save_lock);
    spin_destroy(&S.etag_lock);
    return NULL;
}

/* Handle interrupt signal for graceful shutdown */
static void handle_sig(int sig) {
    (void)sig;
    atomic_store(&g_stop, 1);
}

/* Download a single URL */
static void run_url(const char *url) {
    HeadInfo hi;
    double size = -1.0;
    
    if (head_probe(url, &hi) == 0 && hi.len > 0) {
        size = (double)hi.len;
    }
    
    pthread_t th;
    pthread_create(&th, NULL, input_thread, NULL);
    
    DlItem item = {url, size};
    downloader(&item);
    
    pthread_cancel(th);
    pthread_join(th, NULL);
}

/* Print usage information */
static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s <URL> [OPTIONS]\n", prog);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -d <path>  : Download directory\n");
    fprintf(stderr, "  -w         : Weak network mode\n");
    fprintf(stderr, "  -s         : Strong network mode\n");
    fprintf(stderr, "  --hdd      : HDD optimization\n");
    fprintf(stderr, "  --ssd      : SSD optimization\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }
    
    check_sys_tuning();
    int url_start = 1;
    
    /* Parse command line arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            strncpy(g_dir, argv[i + 1], sizeof(g_dir) - 2);
            size_t len = strlen(g_dir);
            if (len && g_dir[len-1] != '/') {
                strncat(g_dir, "/", 1);
            }
            url_start = i + 2;
            i++;
        } else if (strcmp(argv[i], "-w") == 0) {
            max_parts = WEAK_MAX_PARTS;
            min_parts = WEAK_MIN_PARTS;
            def_parts = WEAK_DEF_PARTS;
            conn_timeout = WEAK_CONN_TIMEOUT;
            low_speed_time = WEAK_LOW_SPEED_TIME;
            retry_max = WEAK_RETRY_MAX;
            backoff_max = WEAK_BACKOFF_MAX;
            fprintf(stderr, "[MODE] Weak network\n");
            url_start = i + 1;
        } else if (strcmp(argv[i], "-s") == 0) {
            max_parts = STRONG_MAX_PARTS;
            min_parts = STRONG_MIN_PARTS;
            def_parts = STRONG_DEF_PARTS;
            conn_timeout = STRONG_CONN_TIMEOUT;
            low_speed_time = STRONG_LOW_SPEED_TIME;
            retry_max = STRONG_RETRY_MAX;
            backoff_max = STRONG_BACKOFF_MAX;
            fprintf(stderr, "[MODE] Strong network\n");
            url_start = i + 1;
        } else if (strcmp(argv[i], "--hdd") == 0) {
            g_buf_size = HDD_BUF_SIZE;
            g_sync_ms = HDD_SYNC_MS;
            g_flush_thresh = HDD_FLUSH_THRESH;
            g_direct_io = HDD_DIRECT_IO;
            fprintf(stderr, "[MODE] HDD optimization\n");
            url_start = i + 1;
        } else if (strcmp(argv[i], "--ssd") == 0) {
            g_buf_size = SSD_BUF_SIZE;
            g_sync_ms = SSD_SYNC_MS;
            g_flush_thresh = SSD_FLUSH_THRESH;
            g_direct_io = SSD_DIRECT_IO;
            fprintf(stderr, "[MODE] SSD optimization\n");
            url_start = i + 1;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }
    
    srand((unsigned)time(NULL));
    get_term_size();
    
    if (ensure_dir() != 0) return 1;
    
    fprintf(stderr, "======================================\n");
    fprintf(stderr, "  * RAX Download Manager *\n");
    fprintf(stderr, "======================================\n\n");
    
    curl_global_init(CURL_GLOBAL_DEFAULT);
    
    if (init_curl_pool() != 0) {
        fprintf(stderr, "[ERR] CURL init failed\n");
        curl_global_cleanup();
        return 1;
    }
    
    struct sigaction sa = {0};
    sa.sa_handler = handle_sig;
    sigaction(SIGINT, &sa, NULL);
    
    /* Process each URL argument */
    for (int i = url_start; i < argc; i++) {
        if (argv[i][0] == '-') continue;
        
        atomic_store(&g_stop, 0);
        run_url(argv[i]);
        
        if (atomic_load(&g_stop)) break;
    }
    
    cleanup_curl_pool();
    cleanup_buf_pool();
    curl_global_cleanup();
    print_stats();
    
    fprintf(stderr, "\n[EXIT]\n");
    return 0;
}