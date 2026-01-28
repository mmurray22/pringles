# The name of your source file (change if yours is different)
SRC = local_append_test.c
# The name of the final executable
TARGET = local_append_test

# Compiler and Flags
CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)
