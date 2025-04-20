all: clean build

build:
	mkdir -p build
	echo ${QHOME}
	cp -r ${QHOME}/* build/
	gcc -shared -fPIC -o build/l64/qduck.so src/c/qduck.c -I./libduckdb -L./libduckdb -lduckdb -mavx2 -O2
	cp src/q/x.q build/

run: 
	QHOME=$(shell pwd)/build \
	rlwrap ./build/l64/q $(shell pwd)/build/x.q

clean:
	rm -rf build
