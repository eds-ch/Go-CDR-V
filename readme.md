# Go-CDR-V

Parses Cisco Collaboration Manager (CUCM/CCM) CDR/CMR files as well as Cisco UBE (CUBE) CDR files and inserts them into a database.
Inserts CDR/CMR records in bulk to improve performance, and utilizes UTC time for insertion. If the files are not in UTC time, the time will be converted to UTC time.

This project is a fork of [ZionDials/Go-CDR](https://github.com/ZionDials/Go-CDR) with added ClickHouse database support and several minor fixes.

## Usage

``` bash
go-cdr parse --config "config.yaml"
```

## Limitations

* Only supports CUCM/CCM and CUBE CDR/CMR files
* Only supports SQLite, PostgreSQL, MySQL, Microsoft SQL Server, and ClickHouse databases
* Only supports CDR/CMR files in CSV format
* Will parse directories in single-threaded mode, i.e. one directory at a time

## Cisco UBE Gateway Configuration

```
gw-accounting file
 primary ftp (IP Address of FTP Server)/ username (username for FTP) password (password for FTP)
 acct-template callhistory-detail
 maximum buffer-size  40 ! kbytes —Maximum buffer size, in kilobytes. Range: 6 to 40. Default: 20.
 maximum fileclose-timer 60 ! minutes —Maximum time, in minutes, to write records to an accounting file. Range: 60 to 1,440. Default: 1,440 (24 hours).
 maximum cdrflush-timer 45 ! minutes —Maximum time, in minutes, to hold call records in the accounting buffer. Range: 1 to 1,435. Default: 60 (1 hour).
```

## Example Config

### PostgreSQL Example
``` yaml
database:
  autoMigrate: true # Migrate the database schema on startup
  database: cdr # Database name
  driver: postgres # Database driver (mysql|mssql|postgres|sqlite|clickhouse)
  host: localhost # Database host
  limit: 100 # Maximum number of records to insert in bulk
  password: 012345abc # Database password
  port: 5432 # Database port
  username: postgres # Database username
  SSL: disable # Database SSL mode (disable|require|verify-ca|verify-full)

### ClickHouse Example
``` yaml
database:
  autoMigrate: true # Migrate the database schema on startup
  database: cdr # Database name
  driver: clickhouse # Database driver
  host: localhost # ClickHouse host
  limit: 1000 # Maximum number of records to insert in bulk (higher for ClickHouse)
  password: "" # ClickHouse password (empty for default user)
  port: 9000 # ClickHouse native port (9000 for native, 8123 for HTTP)
  username: default # ClickHouse username
  SSL: false # Use secure connection (true/false)
logging:
  compress: false # Compress log files
  level: info # Logging level (debug|info|warn|error|fatal|panic)
  maxAge: 30 # Maximum age of log files in days
  maxSize: 100 # Maximum size of log files in megabytes
  name: go-cdr.log # Name of the log files
  path: ./logs # Path to store log files
parser:
  parseInterval: 30 # Interval in minutes to parse files
  directories:
  - input: D:\CDR\cube_cdr\home\cubecdr\ftp # Path to the CDR files
    output: D:\CDR\cube_cdr\home\cubecdr\ftp\processed # Path to move the CDR files after parsing
    type: cube # Type of CDR files (cucm|cube)
    deleteOriginal: false # Delete original files after parsing
  - input: D:\CDR\cucm_cdr\home\cucmcdr\ftp # Path to the CDR files
    output: D:\CDR\cucm_cdr\home\cucmcdr\ftp\processed # Path to move the CDR files after parsing
    type: cucm # Type of CDR files (cucm|cube)
    deleteOriginal: false # Delete original files after parsing
```
