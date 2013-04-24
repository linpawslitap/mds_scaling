#
# Makefile for Linux machines 
#

LARGE_FILE  = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE=1 -D__USE_LARGEFILE64=1
UTILS_DIR   = ../../utilities/src
#BENCH_COMM  =-DCRBENCH_COMM=MPI_COMM_WORLD

NAME = mpi_io_test


# get MPI_CC, MPI_INC, and MPI_LD from environment
CC		= ${MPI_CC}	
CFLAGS  = -g -I. ${MPI_INC} -DROMIO -I$(UTILS_DIR) ${LARGE_FILE} -DMPI_IO_TEST_VERSION2='"1.00.020"' -Wall
CLIBS   = ${MPI_LD} 

MPIIOOBJS	=	
UTILS		=	$(UTILS_DIR)/print.o $(UTILS_DIR)/utilities.o

all:    $(NAME).$(MY_MPI_HOST).x

$(NAME).$(MY_MPI_HOST).x: $(NAME).x
	cp $< $@

$(NAME).x:	$(OBJS) $(UTILS) $(MPIIOOBJS) $(NAME).c
	$(CC) $(CFLAGS) $(CLIBS) $(BENCH_COMM) \
		$(NAME).c \
		$(MPIIOOBJS) $(UTILS) $(CLIBS) \
		-o $@

%.o : %.c
	$(CC) $(CFLAGS) -c $? -o $@

$(UTILS_DIR)/%.o: $(UTILS_DIR)/%.c
	$(CC) $(CFLAGS) -c $? -o $@

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o $(UTILS_DIR)/*.o
	- /bin/rm -f $(NAME).x
	- /bin/rm -f $(NAME).$(MY_MPI_HOST).x
