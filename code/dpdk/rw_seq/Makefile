# Builds against an installed DPDK found via pkg-config.
#   make          -> shared build (default)
#   make static   -> static build (no driver plugins to locate at runtime)
APP = rw_seq
SRCS = rw_seq.c
PKGCONF ?= pkg-config
CFLAGS += -O3 -g -Wall -Wextra -march=native $(shell $(PKGCONF) --cflags libdpdk)
CFLAGS += -DALLOW_EXPERIMENTAL_API

all: shared
shared: $(SRCS) ringworld_hdrs.h
	$(CC) $(CFLAGS) $(SRCS) -o $(APP) $(shell $(PKGCONF) --libs libdpdk)
static: $(SRCS) ringworld_hdrs.h
	$(CC) $(CFLAGS) $(SRCS) -o $(APP) $(shell $(PKGCONF) --static --libs libdpdk)
clean:
	rm -f $(APP)
.PHONY: all shared static clean
