TARGET=run-mapreduce
CFLAGS=-Wall
CC=gcc

all: $(TARGET)
	
$(TARGET): main.o mapreduce.o usr_functions.o 
	$(CC) $(CFLAGS) -o $@ main.o mapreduce.o usr_functions.o
	
main.o: main.c mapreduce.h usr_functions.h
	$(CC) $(CFLAGS) -c main.c
		
mapreduce.o: mapreduce.c mapreduce.h common.h 
	$(CC) $(CFLAGS) -c $*.c
	
usr_functions.o: usr_functions.c usr_functions.h common.h
	$(CC) $(CFLAGS) -c $*.c
	
clean:
	rm -rf *.o *.a $(TARGET)