all:
	gcc -shared -fPIC -o qduck.so qduck.c -I./libduckdb -L./libduckdb -lduckdb -mavx2 -O2
