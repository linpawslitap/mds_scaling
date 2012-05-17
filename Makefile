TARGETS = giga_client giga_server
DIRS	= common client server backends #util test

all: $(TARGETS) #util

$(TARGETS) : common

common : force_look
	@cd common; make

backends : force_look
	@cd backends; make

giga_client : force_look
	@cd client; make

giga_server : force_look
	@cd server; make

clean :
	@for d in $(DIRS); do (cd $$d; $(MAKE) clean ); done

tags : force_look
	ctags -R ./ /usr/include/rpc/ /usr/include/fuse/

git-ignored :
	git ls-files --others -i --exclude-standard

force_look :
	@true
