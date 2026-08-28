#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <time.h>
#include <errno.h>
#include <stdint.h>

#include "pool.h"
#include "reheat.h"
#include "backing_store.h"

/* ── Bitmap helpers ──────────────────────────────────── */

static void bitmap_set(struct pool *p, uint32_t slot_id)
{
    int idx = slot_id / 64;
    uint64_t mask = 1ULL << (slot_id % 64);
    atomic_fetch_or(&p->bitmap_free[idx], mask);
}

static void bitmap_clear(struct pool *p, uint32_t slot_id)
{
    int idx = slot_id / 64;
    uint64_t mask = ~(1ULL << (slot_id % 64));
    atomic_fetch_and(&p->bitmap_free[idx], mask);
}

static uint64_t monotonic_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int add_overflow_size(size_t a, size_t b, size_t *out)
{
    if (a > SIZE_MAX - b)
        return -1;
    *out = a + b;
    return 0;
}

static int mul_overflow_size(size_t a, size_t b, size_t *out)
{
    if (a != 0 && b > SIZE_MAX / a)
        return -1;
    *out = a * b;
    return 0;
}

static size_t align_up_size(size_t value, size_t align)
{
    size_t rem;

    if (align == 0)
        return 0;
    rem = value % align;
    if (rem == 0)
        return value;
    if (value > SIZE_MAX - (align - rem))
        return 0;
    return value + (align - rem);
}

static void pool_zero_slot(struct pool *p, uint32_t slot_id)
{
    memset(slot_ptr(p, slot_id), 0, p->slot_size);
}

static void pool_set_state(struct pool *p, uint32_t slot_id, uint32_t state)
{
    struct slot_meta *meta = slot_meta(p, slot_id);

    meta->state = state;
    meta->last_transition_ns = monotonic_ns();
}

static void pool_init_backend_meta(struct pool *p, enum pool_store_type store_type)
{
    p->backend.kind = (uint32_t)store_type;
    p->backend.flags = BACKEND_FLAG_FD_PASSABLE;
    p->backend.simulated = 1;
    p->backend.latency_ns = 0;

    if (store_type == POOL_STORE_FILE)
        p->backend.flags |= BACKEND_FLAG_PERSISTENT;
    if (store_type == POOL_STORE_DEVDAX)
        p->backend.simulated = 0;
}

/* ── pool_init ───────────────────────────────────────── */

int pool_init(struct pool *p, uint32_t total_slots, size_t slot_size)
{
    struct pool_init_opts opts;

    memset(&opts, 0, sizeof(opts));
    opts.store_type = POOL_STORE_MEMFD;
    return pool_init_with_opts(p, total_slots, slot_size, &opts);
}

int pool_init_with_opts(struct pool *p, uint32_t total_slots, size_t slot_size,
                        const struct pool_init_opts *opts)
{
    struct backing_store store;
    enum pool_store_type store_type = POOL_STORE_MEMFD;
    size_t page_size;
    size_t slots_size;
    size_t create_size;

    if (opts)
        store_type = opts->store_type;

    if (total_slots < MIN_POOL_SLOTS || total_slots > MAX_SLOTS) {
        fprintf(stderr, "pool: total_slots %u must be in [%u, %d] for a %u-slot StratoVirt CXL migration plus one hot spare\n",
                total_slots, MIN_POOL_SLOTS, MAX_SLOTS,
                STRATOVIRT_CXL_SLOT_COUNT);
        return -1;
    }
    page_size = (size_t)sysconf(_SC_PAGESIZE);
    if (page_size == 0)
        page_size = 4096;

    if (mul_overflow_size((size_t)total_slots, slot_size, &slots_size) < 0) {
        fprintf(stderr, "pool: total_slots*slot_size overflows\n");
        return -1;
    }
    if (add_overflow_size(page_size, slots_size, &create_size) < 0) {
        fprintf(stderr, "pool: page_size + total_slots*slot_size overflows\n");
        return -1;
    }

    memset(p, 0, sizeof(*p));
    p->total_slots = total_slots;
    p->slot_size   = slot_size;
    p->running     = 1;
    p->store_type  = store_type;
    p->memfd       = -1;

    /* Create backend store before final sizing so device-dax can report alignment/capacity. */
    if (backing_store_create(&store, store_type, create_size,
                             opts ? opts->store_path : NULL) < 0) {
        return -1;
    }
    p->memfd = store.fd;
    p->backing_alignment = store.alignment ? store.alignment : page_size;

    if (store_type == POOL_STORE_DEVDAX && slot_size % p->backing_alignment != 0) {
        fprintf(stderr, "pool: device-dax slot_size %zu is not a multiple of align %zu\n",
                slot_size, p->backing_alignment);
        backing_store_destroy(&store);
        p->memfd = -1;
        return -1;
    }

    p->header_size = align_up_size(page_size, p->backing_alignment);
    if (p->header_size == 0 ||
        add_overflow_size(p->header_size, slots_size, &p->map_size) < 0) {
        fprintf(stderr, "pool: header_size + total_slots*slot_size overflows\n");
        backing_store_destroy(&store);
        p->memfd = -1;
        return -1;
    }

    if (store_type == POOL_STORE_DEVDAX && p->map_size > store.size) {
        fprintf(stderr, "pool: device-dax capacity too small: need=%zu size=%zu header=%zu slots=%zu\n",
                p->map_size, store.size, p->header_size, slots_size);
        backing_store_destroy(&store);
        p->memfd = -1;
        return -1;
    }

    if (store_type != POOL_STORE_DEVDAX && p->map_size != create_size) {
        if (backing_store_ftruncate_checked(p->memfd, p->map_size, "pool") < 0) {
            backing_store_destroy(&store);
            p->memfd = -1;
            return -1;
        }
    }

    /* Watermarks */
    p->hot_target      = total_slots * 80 / 100;
    p->high_watermark = total_slots * 80 / 100;
    p->low_watermark  = total_slots * 30 / 100;
    p->critical_watermark = total_slots * 10 / 100;
    p->max_leases_per_vm = total_slots / 4;
    if (p->max_leases_per_vm < STRATOVIRT_CXL_SLOT_COUNT)
        p->max_leases_per_vm = STRATOVIRT_CXL_SLOT_COUNT;
    if (opts && opts->max_leases_per_vm != 0)
        p->max_leases_per_vm = opts->max_leases_per_vm;
    if (p->max_leases_per_vm > total_slots)
        p->max_leases_per_vm = total_slots;

    p->max_inflight = total_slots / 2;
    if (p->max_inflight < STRATOVIRT_CXL_SLOT_COUNT)
        p->max_inflight = STRATOVIRT_CXL_SLOT_COUNT;
    if (opts && opts->max_inflight != 0)
        p->max_inflight = opts->max_inflight;
    if (p->max_inflight > total_slots)
        p->max_inflight = total_slots;
    p->lease_ttl_ns      = DEFAULT_LEASE_TTL_NS;
    if (p->hot_target == 0) p->hot_target = 1;
    if (p->high_watermark == 0) p->high_watermark = 1;
    if (p->low_watermark  == 0) p->low_watermark  = 1;
    if (p->critical_watermark == 0) p->critical_watermark = 1;

    pthread_mutex_init(&p->rq_mutex, NULL);
    pthread_cond_init(&p->rq_cond, NULL);

    pool_init_backend_meta(p, store_type);

    /* mmap the whole region */
    p->map_base = mmap(NULL, p->map_size, PROT_READ | PROT_WRITE,
                       MAP_SHARED, p->memfd, 0);
    if (p->map_base == MAP_FAILED) {
        perror("mmap");
        backing_store_destroy(&store);
        p->memfd = -1;
        return -1;
    }

    /* Write magic to the header page (for debugging/identification) */
    uint32_t *hdr = (uint32_t *)p->map_base;
    hdr[0] = POOL_MAGIC;
    hdr[1] = POOL_VERSION;

    /* Init per-slot metadata (daemon-internal arrays) */
    for (uint32_t i = 0; i < total_slots; i++) {
        struct slot_meta *meta = slot_meta(p, i);
        memset(meta, 0, sizeof(*meta));
        meta->block_id = i;
        meta->state = SLOT_FREE_COLD;
        meta->cxl_offset = slot_offset(p, i);
        meta->ttl_ns = p->lease_ttl_ns;
    }
    /* bitmap starts all-zero (COLD); initial preheat will set bits */
    int bitmap_count = (total_slots + 63) / 64;
    for (int i = 0; i < bitmap_count; i++)
        atomic_store(&p->bitmap_free[i], 0ULL);

    atomic_store(&p->free_hot_count, 0);
    atomic_store(&p->inflight_count, 0);
    atomic_store(&p->attached_count, 0);
    atomic_store(&p->cooldown_count, 0);
    atomic_store(&p->alloc_count,    0);
    atomic_store(&p->reheat_count,   0);
    atomic_store(&p->next_lease_id,  1);

    /* Batch initial preheat: 4 slots at a time */
    printf("pool: Starting initial preheat of %u slots (%.0f MB each)...\n",
           total_slots, slot_size / (1024.0 * 1024.0));
    for (uint32_t i = 0; i < total_slots; i += 4) {
        uint32_t batch_end = i + 4;
        if (batch_end > total_slots) batch_end = total_slots;
        for (uint32_t j = i; j < batch_end; j++) {
            void *sp = slot_ptr(p, j);
            pool_zero_slot(p, j);
            if (madvise(sp, slot_size, MADV_POPULATE_WRITE) < 0) {
                fprintf(stderr, "pool: madvise MADV_POPULATE_WRITE slot %u: %s\n",
                        j, strerror(errno));
            }
            pool_set_state(p, j, SLOT_FREE_HOT);
            bitmap_set(p, j);
            atomic_fetch_add(&p->free_hot_count, 1);
        }
        printf("pool: Preheated slots %u-%u\n", i, batch_end - 1);
    }
    printf("pool: Initial preheat complete. free_hot_count=%u\n",
           atomic_load(&p->free_hot_count));

    /* Start reheat thread */
    if (pthread_create(&p->reheat_tid, NULL, reheat_thread_func, p) != 0) {
        perror("pthread_create reheat");
        munmap(p->map_base, p->map_size);
        backing_store_destroy(&store);
        p->memfd = -1;
        return -1;
    }

    printf("pool: backing store=%s fd=%d simulated=%u flags=0x%lx latency_ns=%lu\n",
           backing_store_type_name(store_type), p->memfd,
           p->backend.simulated, (unsigned long)p->backend.flags,
           (unsigned long)p->backend.latency_ns);
    printf("pool: layout align=%zu header=%zu map_size=%zu capacity=%zu slot_size=%zu slots=%u\n",
           p->backing_alignment, p->header_size, p->map_size, store.size,
           p->slot_size, p->total_slots);

    return 0;
}

/* ── pool_destroy ────────────────────────────────────── */

void pool_destroy(struct pool *p)
{
    p->running = 0;
    pthread_cond_signal(&p->rq_cond);
    pthread_join(p->reheat_tid, NULL);

    if (p->map_base && p->map_base != MAP_FAILED)
        munmap(p->map_base, p->map_size);
    if (p->memfd >= 0) {
        struct backing_store store = {
            .type = p->store_type,
            .fd = p->memfd,
        };
        backing_store_destroy(&store);
        p->memfd = -1;
    }

    pthread_mutex_destroy(&p->rq_mutex);
    pthread_cond_destroy(&p->rq_cond);
}

/* ── pool_acquire ────────────────────────────────────── */

int pool_acquire(struct pool *p, uint32_t vm_id,
                 uint32_t *out_slot_id, uint32_t *out_generation,
                 uint64_t *out_offset, uint64_t *out_lease_id,
                 uint64_t *out_ttl_ns)
{
    int bitmap_count = (p->total_slots + 63) / 64;
    uint32_t hot = atomic_load(&p->free_hot_count);

    if (hot == 0)
        return STATUS_NO_FREE_SLOT;
    if (hot <= p->critical_watermark)
        return STATUS_POOL_BUSY;
    if (atomic_load(&p->inflight_count) >= p->max_inflight)
        return STATUS_POOL_BUSY;

    for (int i = 0; i < bitmap_count; i++) {
        uint64_t bits = atomic_load(&p->bitmap_free[i]);
        while (bits != 0) {
            int bit = __builtin_ctzll(bits);
            uint32_t sid = i * 64 + bit;
            if (sid >= p->total_slots)
                break;

            uint64_t mask = ~(1ULL << bit);
            uint64_t old = atomic_fetch_and(&p->bitmap_free[i], mask);
            if (old & (1ULL << bit)) {
                /* Successfully claimed this slot */
                struct slot_meta *meta = slot_meta(p, sid);
                meta->owner_vm_id = vm_id;
                meta->lease_id = atomic_fetch_add(&p->next_lease_id, 1);
                meta->ttl_ns = p->lease_ttl_ns;
                pool_set_state(p, sid, SLOT_INFLIGHT);
                atomic_fetch_sub(&p->free_hot_count, 1);
                atomic_fetch_add(&p->inflight_count, 1);
                atomic_fetch_add(&p->alloc_count, 1);

                *out_slot_id    = sid;
                *out_generation = (uint32_t)meta->epoch;
                *out_offset     = meta->cxl_offset;
                *out_lease_id   = meta->lease_id;
                *out_ttl_ns     = meta->ttl_ns;
                return STATUS_OK;
            }
            /* Bit was already cleared by another thread, refresh and retry */
            bits = atomic_load(&p->bitmap_free[i]);
        }
    }

    return STATUS_NO_FREE_SLOT;
}

/* ── pool_release ────────────────────────────────────── */

int pool_release(struct pool *p, uint32_t vm_id,
                 uint32_t slot_id, uint32_t generation, uint64_t lease_id)
{
    struct slot_meta *meta;
    uint32_t state;

    if (slot_id >= p->total_slots)
        return STATUS_ERROR;
    meta = slot_meta(p, slot_id);

    /* Verify owner */
    if (meta->owner_vm_id != vm_id) {
        fprintf(stderr, "pool: release slot %u: owner mismatch (expected %u, got %u)\n",
                slot_id, meta->owner_vm_id, vm_id);
        return STATUS_ERROR;
    }

    /* Verify generation (ABA protection) */
    if (meta->epoch != generation) {
        fprintf(stderr, "pool: release slot %u: generation mismatch (expected %u, got %u)\n",
                slot_id, (unsigned)meta->epoch, generation);
        return STATUS_GENERATION_MISMATCH;
    }

    state = meta->state;
    if (meta->lease_id != lease_id ||
        (state != SLOT_INFLIGHT && state != SLOT_ATTACHED)) {
        fprintf(stderr, "pool: release slot %u: stale lease (expected lease=%lu state=%u got lease=%lu)\n",
                slot_id, (unsigned long)meta->lease_id, meta->state, (unsigned long)lease_id);
        return STATUS_LEASE_STALE;
    }

    /* Transition: INFLIGHT/ATTACHED -> COOLDOWN. Slow reclaim/reheat is async. */
    meta->owner_vm_id = 0;
    meta->lease_id = 0;
    meta->epoch++;
    pool_set_state(p, slot_id, SLOT_COOLDOWN);
    if (state == SLOT_INFLIGHT && atomic_load(&p->inflight_count) > 0)
        atomic_fetch_sub(&p->inflight_count, 1);
    if (state == SLOT_ATTACHED && atomic_load(&p->attached_count) > 0)
        atomic_fetch_sub(&p->attached_count, 1);
    atomic_fetch_add(&p->cooldown_count, 1);
    pool_enqueue_reheat(p, slot_id);

    return STATUS_OK;
}

/* ── pool_attach ─────────────────────────────────────── */

int pool_attach(struct pool *p, uint32_t vm_id,
                uint32_t slot_id, uint32_t generation, uint64_t lease_id)
{
    struct slot_meta *meta;

    if (slot_id >= p->total_slots)
        return STATUS_ERROR;
    meta = slot_meta(p, slot_id);

    if (meta->owner_vm_id != vm_id) {
        fprintf(stderr, "pool: attach slot %u: owner mismatch (expected %u, got %u)\n",
                slot_id, meta->owner_vm_id, vm_id);
        return STATUS_ERROR;
    }

    if (meta->epoch != generation) {
        fprintf(stderr, "pool: attach slot %u: generation mismatch (expected %u, got %u)\n",
                slot_id, (unsigned)meta->epoch, generation);
        return STATUS_GENERATION_MISMATCH;
    }

    if (meta->lease_id != lease_id || meta->state != SLOT_INFLIGHT) {
        fprintf(stderr, "pool: attach slot %u: stale lease (expected lease=%lu state=%u got lease=%lu)\n",
                slot_id, (unsigned long)meta->lease_id, meta->state, (unsigned long)lease_id);
        return STATUS_LEASE_STALE;
    }

    pool_set_state(p, slot_id, SLOT_ATTACHED);
    if (atomic_load(&p->inflight_count) > 0)
        atomic_fetch_sub(&p->inflight_count, 1);
    atomic_fetch_add(&p->attached_count, 1);
    return STATUS_OK;
}

/* ── pool_force_reclaim (for disconnect cleanup) ─────── */

int pool_force_reclaim(struct pool *p, uint32_t vm_id,
                       uint32_t slot_id, uint32_t generation,
                       uint64_t lease_id)
{
    struct slot_meta *meta;
    uint32_t state;

    if (slot_id >= p->total_slots)
        return STATUS_ERROR;
    meta = slot_meta(p, slot_id);

    if (meta->owner_vm_id != vm_id || meta->epoch != generation ||
        meta->lease_id != lease_id) {
        return STATUS_LEASE_STALE;
    }

    state = meta->state;
    if (state != SLOT_INFLIGHT && state != SLOT_ATTACHED)
        return STATUS_LEASE_STALE;

    bitmap_clear(p, slot_id);
    if (state == SLOT_INFLIGHT && atomic_load(&p->inflight_count) > 0)
        atomic_fetch_sub(&p->inflight_count, 1);
    if (state == SLOT_ATTACHED && atomic_load(&p->attached_count) > 0)
        atomic_fetch_sub(&p->attached_count, 1);

    meta->owner_vm_id = 0;
    meta->lease_id = 0;
    meta->epoch++;
    pool_set_state(p, slot_id, SLOT_COOLDOWN);
    atomic_fetch_add(&p->cooldown_count, 1);

    pool_enqueue_reheat(p, slot_id);
    return STATUS_OK;
}

/* ── Reheat queue ────────────────────────────────────── */

void pool_enqueue_reheat(struct pool *p, uint32_t slot_id)
{
    pthread_mutex_lock(&p->rq_mutex);
    if (p->rq_count < MAX_SLOTS) {
        p->reheat_queue[p->rq_tail] = slot_id;
        p->rq_tail = (p->rq_tail + 1) % MAX_SLOTS;
        p->rq_count++;
    }
    pthread_cond_signal(&p->rq_cond);
    pthread_mutex_unlock(&p->rq_mutex);
}

static int reheat_dequeue(struct pool *p, uint32_t *out_slot_id)
{
    pthread_mutex_lock(&p->rq_mutex);
    while (p->rq_count == 0 && p->running) {
        pthread_cond_wait(&p->rq_cond, &p->rq_mutex);
    }
    if (!p->running && p->rq_count == 0) {
        pthread_mutex_unlock(&p->rq_mutex);
        return -1;
    }
    *out_slot_id = p->reheat_queue[p->rq_head];
    p->rq_head = (p->rq_head + 1) % MAX_SLOTS;
    p->rq_count--;
    pthread_mutex_unlock(&p->rq_mutex);
    return 0;
}

/* ── Reheat thread ───────────────────────────────────── */

void *reheat_thread_func(void *arg)
{
    struct pool *p = (struct pool *)arg;
    printf("reheat: Thread started (target=%u high=%u low=%u critical=%u)\n",
           p->hot_target, p->high_watermark, p->low_watermark,
           p->critical_watermark);

    while (p->running) {
        uint32_t hot = atomic_load(&p->free_hot_count);

        if (hot >= p->high_watermark) {
            /* Plenty of hot slots, sleep */
            usleep(1000000);  /* 1 second */
            continue;
        }

        if (hot < p->low_watermark) {
            /* Urgent mode: batch reheat without sleeping */
            pthread_mutex_lock(&p->rq_mutex);
            int batch = p->rq_count;
            if (batch > 4) batch = 4;
            uint32_t batch_slots[4];
            for (int i = 0; i < batch; i++) {
                batch_slots[i] = p->reheat_queue[p->rq_head];
                p->rq_head = (p->rq_head + 1) % MAX_SLOTS;
                p->rq_count--;
            }
            pthread_mutex_unlock(&p->rq_mutex);

            for (int i = 0; i < batch; i++) {
                reheat_one(p, batch_slots[i]);
            }
            if (batch == 0)
                usleep(100000);  /* 100ms: no work in queue, avoid busy-loop */
            continue;
        }

        /* Normal mode: single slot reheat + small delay */
        uint32_t sid;
        if (reheat_dequeue(p, &sid) == 0) {
            reheat_one(p, sid);
            usleep(10000);  /* 10 ms */
        }
    }

    printf("reheat: Thread exiting\n");
    return NULL;
}
