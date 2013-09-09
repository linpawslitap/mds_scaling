OPT ?= -O2 -DNDEBUG
#OPT ?= -g2 -DDEBUG
CFLAGS	= -Wall -Wextra `pkg-config fuse --cflags` -Werror -D_GNU_SOURCE $(OPT) -fPIC
LDFLAGS = -lm -lpthread -lstdc++
SRCS = $(wildcard *.c)
HDRS = $(wildcard *.h) $(RPC_H)
OBJS = $(addsuffix .o, $(basename $(SRCS)))
HADOOP_LDFLAGS= -L/usr/lib/jvm/default-java/jre/lib/amd64/ \
	-L/usr/lib/jvm/default-java/jre/lib/amd64/server -ljvm
