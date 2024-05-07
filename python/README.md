# sqlite-zstd
Extension for sqlite that provides transparent dictionary-based row-level compression for sqlite. This basically allows you to compress entries in a sqlite database almost as well as if you were compressing the whole DB file, but while retaining random access.

See also the announcement blog post for some motivation, benchmarks and ramblings: https://phiresky.github.io/blog/2022/sqlite-zstd

Depending on the data, this can reduce the size of the database by 80% while keeping performance mostly the same (or even improving it, since the data to be read from disk is smaller).

Note that a compression VFS such as https://github.com/mlin/sqlite_zstd_vfs might be suited better depending on the use case. That has very different tradeoffs and capabilities, but the end result is similar.

## Install
```bash
pip install sqlite-zstd
```

## Usage
```python
import sqlite3
import sqlite_zstd

conn = sqlite3.connect(':memory:')
sqlite_zstd.load(conn)
```
