#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

#include "pool.h"
#include "reheat.h"

void reheat_one(struct pool *p, uint32_t slot_id)
{
    struct slot_meta *meta;

    if (slot_id >= p->total_slots)
        return;
    meta = slot_meta(p, slot_id);

    void *sp = slot_ptr(p, slot_id);
    int prev_state = meta->state;

    /*
     * RELEASE now only marks the slot as COOLDOWN and returns immediately.
     * The background worker owns the slow path: reclaim old pages first, then
     * repopulate to bring the slot back to FREE_HOT.
     */
    if (prev_state == SLOT_COOLDOWN) {
        if (madvise(sp, p->slot_size, MADV_DONTNEED) < 0) {
            fprintf(stderr, "reheat: madvise DONTNEED slot %u: %s\n",
                    slot_id, strerror(errno));
        }
        if (atomic_load(&p->cooldown_count) > 0)
            atomic_fetch_sub(&p->cooldown_count, 1);
        meta->state = SLOT_FREE_COLD;
    }

    meta->state = SLOT_REHEATING;
    meta->last_transition_ns = 0;
    memset(sp, 0, p->slot_size);

    if (madvise(sp, p->slot_size, MADV_POPULATE_WRITE) < 0) {
        fprintf(stderr, "reheat: madvise slot %u: %s\n",
                slot_id, strerror(errno));
        /* Mark as COLD so it can be retried later */
        meta->state = SLOT_FREE_COLD;
        return;
    }

    /* Success: return to free_hot with a fresh epoch. */
    meta->state = SLOT_FREE_HOT;
    meta->lease_id = 0;
    meta->ttl_ns = p->lease_ttl_ns;
    meta->last_transition_ns = 0;

    /* Set bitmap bit */
    int idx = slot_id / 64;
    uint64_t mask = 1ULL << (slot_id % 64);
    atomic_fetch_or(&p->bitmap_free[idx], mask);

    atomic_fetch_add(&p->free_hot_count, 1);
    atomic_fetch_add(&p->reheat_count, 1);

    printf("reheat: Slot %u reheated (gen=%u, free_hot=%u)\n",
           slot_id, (unsigned)meta->epoch,
           atomic_load(&p->free_hot_count));
}
