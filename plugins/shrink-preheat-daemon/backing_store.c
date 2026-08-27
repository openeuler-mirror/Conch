#include <errno.h>
#include <fcntl.h>
#include <linux/memfd.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sysmacros.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "backing_store.h"

#define DEVDAX_DEFAULT_ALIGN (2UL * 1024 * 1024)

static int my_memfd_create(const char *name, unsigned int flags)
{
    return syscall(SYS_memfd_create, name, flags);
}

const char *backing_store_type_name(enum pool_store_type type)
{
    switch (type) {
    case POOL_STORE_MEMFD:
        return "memfd";
    case POOL_STORE_FILE:
        return "file";
    case POOL_STORE_DEVDAX:
        return "device-dax";
    default:
        return "unknown";
    }
}

static int read_uint64_file(const char *path, uint64_t *out)
{
    FILE *fp;
    char buf[64];
    char *end;
    unsigned long long val;

    fp = fopen(path, "r");
    if (fp == NULL)
        return -1;
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        errno = EINVAL;
        return -1;
    }
    fclose(fp);

    errno = 0;
    val = strtoull(buf, &end, 0);
    if (errno != 0 || end == buf) {
        errno = EINVAL;
        return -1;
    }
    *out = (uint64_t)val;
    return 0;
}

static int devdax_sysfs_path(const char *prefix, const char *dev_path,
                             const char *attr, char *out, size_t out_len)
{
    const char *name = strrchr(dev_path, '/');
    int n;

    name = name ? name + 1 : dev_path;
    if (name[0] == '\0') {
        errno = EINVAL;
        return -1;
    }

    n = snprintf(out, out_len, "%s/%s/%s", prefix, name, attr);
    if (n < 0 || (size_t)n >= out_len) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int devdax_read_attr(const char *dev_path, const char *attr,
                            uint64_t *out)
{
    char path[PATH_MAX];

    if (devdax_sysfs_path("/sys/class/dax", dev_path, attr, path, sizeof(path)) < 0)
        return -1;
    if (read_uint64_file(path, out) == 0)
        return 0;

    if (devdax_sysfs_path("/sys/bus/dax/devices", dev_path, attr, path, sizeof(path)) < 0)
        return -1;
    return read_uint64_file(path, out);
}

static int devdax_read_attr_by_rdev(dev_t rdev, const char *attr, uint64_t *out)
{
    char path[PATH_MAX];
    int n;

    n = snprintf(path, sizeof(path), "/sys/dev/char/%u:%u/device/%s",
                 major(rdev), minor(rdev), attr);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    if (read_uint64_file(path, out) == 0)
        return 0;

    n = snprintf(path, sizeof(path), "/sys/dev/char/%u:%u/device/dax_region/%s",
                 major(rdev), minor(rdev), attr);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return read_uint64_file(path, out);
}

static int devdax_read_attr_any(const char *dev_path, dev_t rdev,
                                const char *attr, uint64_t *out)
{
    if (devdax_read_attr_by_rdev(rdev, attr, out) == 0)
        return 0;
    return devdax_read_attr(dev_path, attr, out);
}

int backing_store_ftruncate_checked(int fd, size_t size, const char *what)
{
    if (size > (size_t)LLONG_MAX) {
        fprintf(stderr, "%s: size %zu exceeds off_t limit\n", what, size);
        errno = EOVERFLOW;
        return -1;
    }
    if (ftruncate(fd, (off_t)size) < 0) {
        fprintf(stderr, "%s: ftruncate(%zu) failed: %s\n",
                what, size, strerror(errno));
        return -1;
    }
    return 0;
}

int backing_store_create(struct backing_store *store, enum pool_store_type type,
                         size_t size, const char *path)
{
    int fd = -1;
    int need_resize = 1;
    size_t page_size = (size_t)sysconf(_SC_PAGESIZE);

    store->fd = -1;
    store->type = type;
    if (page_size == 0)
        page_size = 4096;
    store->alignment = page_size;
    store->size = size;

    switch (type) {
    case POOL_STORE_MEMFD:
        fd = my_memfd_create("shrink_preheat_pool", 0);
        if (fd < 0) {
            perror("backing_store: memfd_create");
            return -1;
        }
        break;
    case POOL_STORE_FILE:
        if (path == NULL || path[0] == '\0') {
            fprintf(stderr, "backing_store: file path is required for file backend\n");
            errno = EINVAL;
            return -1;
        }
        fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
        if (fd < 0) {
            fprintf(stderr, "backing_store: open(%s) failed: %s\n",
                    path, strerror(errno));
            return -1;
        }
        break;
    case POOL_STORE_DEVDAX:
    {
        struct stat st;
        uint64_t dax_align;
        uint64_t dax_size;

        if (path == NULL || path[0] == '\0') {
            fprintf(stderr, "backing_store: device-dax path is required (e.g. /dev/dax0.0)\n");
            errno = EINVAL;
            return -1;
        }
        fd = open(path, O_RDWR);
        if (fd < 0) {
            fprintf(stderr, "backing_store: open(%s) failed: %s\n",
                    path, strerror(errno));
            return -1;
        }
        if (fstat(fd, &st) < 0) {
            fprintf(stderr, "backing_store: fstat(%s) failed: %s\n",
                    path, strerror(errno));
            close(fd);
            return -1;
        }
        if (!S_ISCHR(st.st_mode)) {
            fprintf(stderr, "backing_store: %s is not a char device (not a device-dax node)\n",
                    path);
            close(fd);
            errno = ENODEV;
            return -1;
        }
        if (devdax_read_attr_any(path, st.st_rdev, "align", &dax_align) < 0) {
            fprintf(stderr, "backing_store: cannot read device-dax align for %s, using %zu\n",
                    path, (size_t)DEVDAX_DEFAULT_ALIGN);
            dax_align = DEVDAX_DEFAULT_ALIGN;
        }
        if (dax_align == 0 || dax_align > SIZE_MAX ||
            ((size_t)dax_align % page_size) != 0) {
            fprintf(stderr, "backing_store: invalid device-dax align %llu for %s\n",
                    (unsigned long long)dax_align, path);
            close(fd);
            errno = EINVAL;
            return -1;
        }
        if (devdax_read_attr_any(path, st.st_rdev, "size", &dax_size) < 0) {
            fprintf(stderr, "backing_store: cannot read required device-dax size for %s: %s\n",
                    path, strerror(errno));
            close(fd);
            return -1;
        }
        if (dax_size == 0 || dax_size > SIZE_MAX) {
            fprintf(stderr, "backing_store: invalid device-dax size %llu for %s\n",
                    (unsigned long long)dax_size, path);
            close(fd);
            errno = EINVAL;
            return -1;
        }
        store->alignment = (size_t)dax_align;
        store->size = (size_t)dax_size;
        need_resize = 0;
        break;
    }
    default:
        fprintf(stderr, "backing_store: unknown backend type %d\n", type);
        errno = EINVAL;
        return -1;
    }

    if (need_resize) {
        if (backing_store_ftruncate_checked(fd, size, "backing_store") < 0) {
            close(fd);
            return -1;
        }
    }

    store->fd = fd;
    return 0;
}

void backing_store_destroy(struct backing_store *store)
{
    if (store->fd >= 0) {
        close(store->fd);
        store->fd = -1;
    }
}
