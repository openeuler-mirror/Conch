#ifndef REHEAT_H
#define REHEAT_H

#include "pool.h"

/* Reheat one slot: MADV_POPULATE_WRITE + update state/bitmap/generation */
void reheat_one(struct pool *p, uint32_t slot_id);

#endif /* REHEAT_H */
