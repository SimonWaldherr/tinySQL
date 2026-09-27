.PHONY: all build test run clean

GO      ?= go
PYTHON  ?= python3
ROOT    := ../..
LIBNAME := $(if $(filter Darwin,$(shell uname -s)),libtinysql.dylib,libtinysql.so)
LIB     := tinysql/$(LIBNAME)

all: build

# Build the shared library from the unified C ABI into the package directory.
build:
	cd $(ROOT) && $(GO) build -trimpath -buildmode=c-shared -o bindings/python/$(LIB) ./bindings/c
	rm -f tinysql/libtinysql.h

test: build
	$(PYTHON) -m unittest discover -s tests -v

run: build
	$(PYTHON) examples/quickstart.py

clean:
	rm -f tinysql/libtinysql.so tinysql/libtinysql.dylib tinysql/tinysql.dll tinysql/libtinysql.h
	rm -rf build dist *.egg-info tinysql/__pycache__ tests/__pycache__
