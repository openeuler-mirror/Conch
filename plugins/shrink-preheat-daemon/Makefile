CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -pthread -D_GNU_SOURCE
LDFLAGS = -pthread

TARGET  = shrink-preheat-daemon
TEST    = test_client
SRCS    = main.c pool.c ipc.c reheat.c backing_store.c
OBJS    = $(SRCS:.c=.o)

.PHONY: all clean test

all: $(TARGET) $(TEST)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

$(TEST): test_client.o backing_store.o
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c pool.h ipc.h reheat.h backing_store.h
	$(CC) $(CFLAGS) -c -o $@ $<

test: all
	bash ./run_test.sh

clean:
	rm -f $(OBJS) test_client.o $(TARGET) $(TEST)
