default: libsqlitefunctions.so libsqlitemd5.so

libsqlitefunctions.so:
	gcc -lm -fPIC -w -shared extension-functions.c -o $@

libsqlitemd5.so:
	gcc -lm -fPIC -w -shared md5.c -o $@