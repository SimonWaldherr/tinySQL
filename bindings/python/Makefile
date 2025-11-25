.PHONY: all build run clean

LIB     := libtinysql.so
PACKAGE := .

all: build

build:
	GO111MODULE=on go build -buildmode=c-shared -o $(LIB) $(PACKAGE)

run: build
	python3 example.py

clean:
	rm -f $(LIB) libtinysql.h
