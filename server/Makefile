include ../Makefile.inc
include ../backends/build_config.mk

CFLAGS += -iquote ..
CRYPTO_LIBS = /usr/libexec/dropbox/libssl.so.0.9.8 /usr/libexec/dropbox/libcrypto.so.0.9.8
LIBS += ../backends/$(LEVELDBLIBRARY) ${CRYPTO_LIBS} ../common.a

OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
#OBJS += ../backends/*.o

TARGETS = ../giga_server

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_server : $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS) $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS)
