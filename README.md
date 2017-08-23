# Orc output plugin for Embulk

[![Build Status](https://travis-ci.org/yuokada/embulk-output-orc.svg?branch=master)](https://travis-ci.org/yuokada/embulk-output-orc)

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **path_prefix**: A prefix of output path. (string, required)
- **file_ext**: An extension of output file. (string, default: `.orc`)
- **sequence_format**: (string, default: `.%03d`)
- **buffer_size**: Set the ORC buffer size (integer, default: `10000`)
- **strip_size**: Set the ORC strip size (integer,  default: `100000`)
- **compression_kind**: description (string, default: `'ZLIB'`)
- **overwrite**: (LocalFileSystem only) Overwrite if output files already exist. (boolean, default: `false`)
- **default_from_timezone** Time zone of timestamp columns. This can be overwritten for each column using column_options (DateTimeZone, default: `UTC`)

## Example

```yaml
out:
  type: orc
  path_prefix: "/tmp/output"
  buffer_size: 8000
  strip_size:  90000
  compression_kind: ZLIB
  overwrite:   true
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
