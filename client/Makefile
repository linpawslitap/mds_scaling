include ../Makefile.inc
include ../backends/build_config.mk

CFLAGS += -iquote ..
LDFLAGS +=  `pkg-config fuse --libs`
LIBS =../backends/$(LEVELDBLIBRARY) ../common.a
OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
TARGETS = ../giga_client ../libgiga_client.a

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_client : $(OBJS)
	$(CC) -o $@ $^ $(LIBS) $(LDFLAGS)

../libgiga_client.a : $(OBJS)
	ar rv ../libgiga_client.a $?
	ranlib ../libgiga_client.a

clean :
	rm -f $(TARGETS) $(OBJS)
