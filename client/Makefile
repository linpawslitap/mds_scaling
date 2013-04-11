include ../Makefile.inc
include ../backends/build_config.mk

CFLAGS += -iquote ..
LDFLAGS +=  `pkg-config fuse --libs`
LIBS =../backends/$(LEVELDBLIBRARY) ../common.a

OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
#OBJS += ../backends/*.o

TARGETS = ../giga_client

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_client : $(OBJS)
	$(CC) -o $@ $^ $(LIBS) $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS)
