CFLAGS	= -g -Wall -Wextra `pkg-config fuse --cflags` -Werror -D_GNU_SOURCE
LDFLAGS = -lm -lpthread -lstdc++ #./backends/leveldb/libleveldb.a
SRCS = $(wildcard *.c)
HDRS = $(wildcard *.h) $(RPC_H)
OBJS = $(addsuffix .o, $(basename $(SRCS)))
