all:
	javac -classpath /usr/share/java/jna.jar:../hadoop/hadoop-common-project/hadoop-common/target/hadoop-common-0.23.7.jar GigaClient.java
	javac -classpath /usr/share/java/jna.jar BytesTest.java
	gcc -fPIC -c -o bytes_test.o bytes_test.c
	gcc -shared -o bytes_test.so bytes_test.o -lm -lpthread -lstdc++

clean:
	rm *.class *.o *.so
