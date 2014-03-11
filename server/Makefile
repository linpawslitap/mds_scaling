include ../Makefile.inc
include ../backends/build_config.mk

HADOOP_LIB = ../hadoop/lib/native/libhdfs.a

CFLAGS += -iquote ..
CRYPTO_LIBS = /usr/lib/libssl.so.0.9.8
#CRYPTO_LIBS = /usr/lib/x86_64-linux-gnu/libssl.so.0.9.8
#CRYPTO_LIBS = /usr/libexec/dropbox/libssl.so.0.9.8
LIBS += ../backends/$(LEVELDBLIBRARY) $(CRYPTO_LIBS) $(HADOOP_LIB) ../common.a

OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
LDFLAGS += $(HADOOP_LDFLAGS)

TARGETS = ../giga_server

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_server : $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS) $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS)
