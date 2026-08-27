#ifndef POOL_H
#define POOL_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

/* ── Constants ─────────────────────────────────────────────── */

#define POOL_MAGIC          0x50484F54  /* "PHOT" */
#define POOL_VERSION        2
#define DEFAULT_SLOT_SIZE   (32UL * 1024 * 1024)   /* StratoVirt shrink block */
#define DEFAULT_TOTAL_SLOTS 32
#define STRATOVIRT_CXL_SLOT_COUNT 16U
#define MIN_POOL_SLOTS (STRATOVIRT_CXL_SLOT_COUNT + 1)
#define MAX_SLOTS           1024
#define DEFAULT_LEASE_TTL_NS (30ULL * 1000 * 1000 * 1000)

/* Slot states */
#define SLOT_FREE_COLD      0
#define SLOT_FREE_HOT       1
#define SLOT_INFLIGHT       2
#define SLOT_ATTACHED       3
#define SLOT_COOLDOWN       4
#define SLOT_QUARANTINE     5
#define SLOT_REHEATING      6

/* Response status */
#define STATUS_OK                   0
#define STATUS_NO_FREE_SLOT         1
#define STATUS_GENERATION_MISMATCH  2
#define STATUS_ERROR                3
#define STATUS_QUOTA_EXCEEDED       4
#define STATUS_LEASE_STALE          5
#define STATUS_POOL_BUSY            6

#define BACKEND_FLAG_FD_PASSABLE   (1ULL << 0)
#define BACKEND_FLAG_PERSISTENT     (1ULL << 1)
#define BACKEND_FLAG_BATCH_OPS      (1ULL << 2)

/* Backing store types for pool slot data. */
enum pool_store_type {
    POOL_STORE_MEMFD = 0,
    POOL_STORE_FILE  = 1,
    POOL_STORE_DEVDAX = 2, /* reserved for future CXL/devdax integration */
};

struct pool_init_opts {
    enum pool_store_type store_type;
    const char          *store_path; /* required for POOL_STORE_FILE/POOL_STORE_DEVDAX */
    uint32_t             max_leases_per_vm;
    uint32_t             max_inflight;
};

struct backend_info {
    uint32_t kind;
    uint32_t simulated;
    uint64_t flags;
    uint64_t latency_ns;
};

struct slot_meta {
    uint32_t block_id;
    uint32_t state;
    uint32_t owner_vm_id;
    uint32_t flags;

    uint64_t lease_id;
    uint64_t epoch;
    uint64_t ttl_ns;

    uint64_t cxl_offset;
    uint64_t last_transition_ns;

    uint32_t occupancy_est;
    uint32_t dirty_ratio_est;
    uint32_t zero_ratio_est;
    uint32_t heat_score;
};

/* ── In-daemon pool management ─────────────────────────── */

struct pool {
    int          memfd; /* historical name kept for IPC compatibility */
    enum pool_store_type store_type;
    struct backend_info backend;
    void        *map_base;       /* mmap base of whole region */
    size_t       map_size;
    size_t       header_size;    /* page-aligned, >= slot_size for metadata */
    size_t       backing_alignment;

    uint32_t     total_slots;
    size_t       slot_size;

    /* Per-slot metadata (daemon-internal, NOT in shared memfd) */
    struct slot_meta slots[MAX_SLOTS];

    /* Bitmap for free HOT slots (daemon-internal) */
    _Atomic uint64_t bitmap_free[(MAX_SLOTS + 63) / 64];

    /* Stats */
    _Atomic uint32_t free_hot_count;
    _Atomic uint32_t inflight_count;
    _Atomic uint32_t attached_count;
    _Atomic uint32_t cooldown_count;
    _Atomic uint64_t alloc_count;
    _Atomic uint64_t reheat_count;
    _Atomic uint64_t next_lease_id;

    /* Reheat queue: simple circular buffer (protected by mutex) */
    uint32_t     reheat_queue[MAX_SLOTS];
    int          rq_head;
    int          rq_tail;
    int          rq_count;
    pthread_mutex_t rq_mutex;
    pthread_cond_t  rq_cond;

    /* Watermarks */
    uint32_t     hot_target;
    uint32_t     high_watermark;
    uint32_t     low_watermark;
    uint32_t     critical_watermark;
    uint32_t     max_leases_per_vm;
    uint32_t     max_inflight;
    uint64_t     lease_ttl_ns;

    /* Reheat thread */
    pthread_t    reheat_tid;
    volatile int running;
};

/* Pool lifecycle */
int  pool_init(struct pool *p, uint32_t total_slots, size_t slot_size);
int  pool_init_with_opts(struct pool *p, uint32_t total_slots, size_t slot_size,
                         const struct pool_init_opts *opts);
void pool_destroy(struct pool *p);

/* Slot management */
int  pool_acquire(struct pool *p, uint32_t vm_id,
                  uint32_t *out_slot_id, uint32_t *out_generation,
                  uint64_t *out_offset, uint64_t *out_lease_id,
                  uint64_t *out_ttl_ns);
int  pool_release(struct pool *p, uint32_t vm_id,
                  uint32_t slot_id, uint32_t generation, uint64_t lease_id);
int  pool_attach(struct pool *p, uint32_t vm_id,
                 uint32_t slot_id, uint32_t generation, uint64_t lease_id);
int  pool_force_reclaim(struct pool *p, uint32_t vm_id,
                        uint32_t slot_id, uint32_t generation,
                        uint64_t lease_id);

/* Reheat queue */
void pool_enqueue_reheat(struct pool *p, uint32_t slot_id);

/* Reheat thread entry */
void *reheat_thread_func(void *arg);

/* Helpers */
static inline void *slot_ptr(struct pool *p, uint32_t slot_id)
{
    return (char *)p->map_base + p->header_size + (size_t)slot_id * p->slot_size;
}

static inline uint64_t slot_offset(struct pool *p, uint32_t slot_id)
{
    return p->header_size + (uint64_t)slot_id * p->slot_size;
}

static inline struct slot_meta *slot_meta(struct pool *p, uint32_t slot_id)
{
    return &p->slots[slot_id];
}

#endif /* POOL_H */
