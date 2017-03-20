## Building

1. Install unixodbc.
2. Download Poco source
3. Run ```./configure --static --minimal --no-tests --cflags=-fPIC```
4. Run ```make && make install```
5. Run ```./build.sh```

## ODBC configuration

~/.odbc.ini:

[ClickHouse]
Driver = /home/milovidov/work/ClickHouse/dbms/src/ODBC/odbc.so
Description = ClickHouse driver
DATABASE = default
HOST = localhost
PORT = 9000
FRAMED = 0

## Testing
Run ```iusql -v ClickHouse```