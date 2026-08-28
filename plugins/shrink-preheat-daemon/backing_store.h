#ifndef BACKING_STORE_H
#define BACKING_STORE_H

#include <stddef.h>

#include "pool.h"

struct backing_store {
    enum pool_store_type type;
    int                  fd;
    size_t               alignment;
    size_t               size;
};

const char *backing_store_type_name(enum pool_store_type type);
int backing_store_create(struct backing_store *store, enum pool_store_type type,
                         size_t size, const char *path);
int backing_store_ftruncate_checked(int fd, size_t size, const char *what);
void backing_store_destroy(struct backing_store *store);

#endif /* BACKING_STORE_H */
