# Orc output plugin for Embulk

[![Build Status](https://travis-ci.org/yuokada/embulk-output-orc.svg?branch=master)](https://travis-ci.org/yuokada/embulk-output-orc)
[![Gem Version](https://badge.fury.io/rb/embulk-output-orc.svg)](https://badge.fury.io/rb/embulk-output-orc)

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **path_prefix**: A prefix of output path. (string, required)
  - support: `file`, `s3`, `s3n` and `s3a`.
- **file_ext**: An extension of output file. (string, default: `.orc`)
- **sequence_format**: (string, default: `.%03d`)
- **buffer_size**: Set the ORC buffer size (integer, default: `262144(256KB)` )
- **strip_size**: Set the ORC strip size (integer,  default: `67108864(64MB)` )
- **block_size**: Set the ORC block size (integer, default: `268435456(256MB)`)
- **compression_kind**: description (string, default: `'ZLIB'`)
    - `NONE`, `ZLIB`, `SNAPPY`, `LZO`, `LZ4`
- **overwrite**: Overwrite if output files already exist. (boolean, default: `false`)
    - Support: `LocalFileSystem`, `S3(s3, s3a, s3n)`
- **default_from_timezone** Time zone of timestamp columns. This can be overwritten for each column using column_options (DateTimeZone, default: `UTC`)

- **auth_method**: name of mechanism to authenticate requests (basic, env, instance, profile, properties, anonymous, or session. default: basic)  
  see: https://github.com/embulk/embulk-input-s3#configuration

    - `env`, `basic`, `profile`, `default`, `session`, `anonymous`, `properties`
    

## Example

```yaml
out:
  type: orc
  path_prefix: "/tmp/output"
  compression_kind: ZLIB
  overwrite:   true
```

## ChangeLog

### ver 0.3.2

- Update `orc` libraries to `1.4.3`

### ver 0.3.0

- Change default value : (block_size, buffer_size, strip_size)

    - default value is Hive's default value.  
      (see: https://orc.apache.org/docs/hive-config.html)

### ver 0.2.0

- support: output to s3

    - `s3n`, `s3a` protocol

### ver 0.1.0

- initial release

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## SonarQube

[embulk-output-orc](https://sonarcloud.io/dashboard?id=embulk-output-orc "embulk-output-orc - Yukihiro Okada")
