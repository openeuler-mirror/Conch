# shrink-preheat-daemon

`shrink-preheat-daemon` is an optional host service for StratoVirt
virtio-shrink migration. It maintains a pool of pre-populated memory slots,
allocates slots to VMM clients, and keeps attached slots leased while they are
used as guest-memory backing.

The control plane uses a Unix stream socket. During the `HELLO` exchange the
daemon passes the pool backing file descriptor with `SCM_RIGHTS`; memory data
is then accessed through shared mappings instead of being copied over the
socket.

## Build and test

```shell
make
make test
```

The integration test creates a temporary 17-slot memfd pool and validates the
batch acquire, attach, release, and reheat paths. It temporarily pre-populates
approximately 544 MiB of memory.

## Run

```shell
./shrink-preheat-daemon \
    --slots=32 \
    --slot-size=32M \
    --sock=/run/conch/shrink_preheat.sock \
    --store=memfd \
    --max-leases-per-vm=16 \
    --max-inflight=16
```

Supported backing stores are `memfd`, `file`, and `device-dax`. The `file` and
`device-dax` modes also require `--store-path`.

Configure the matching StratoVirt device with the same socket:

```text
-device virtio-shrink-pci,...,preheat-sock=/run/conch/shrink_preheat.sock
```

Attached slots are active guest-memory backing. The daemon must remain alive
until all VMM clients using those slots have exited or explicitly released
their leases.
