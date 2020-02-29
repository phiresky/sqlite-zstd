# sqlite-zstd

Extension for sqlite that provides transparent dictionary-based row-level compression for sqlite. This basically allows you to compress entries in a sqlite database almost as well as if you were compressing the whole DB file, but while retaining random access.

Depending on the data, this can reduce the size of the database by 90% while keeping performance the same.

TODO: describe

-   how to load the extension
-   basic usage
-   data use statistics
-   performance implications
-   the whole api interface / all functions
-   how it works

## provided sql functions

-   `zstd_enable_transparent(table_name, column_name)`

    Enable transparent row-level compression of the given column on the given table. A dictionary will be automatically trained on the existing data (so make sure you have some data in there first).

    The data will be moved to `_table_name_zstd`, while `table_name` will be a view that can be queried as normally, including SELECT, INSERT, UPDATE, and DELETE queries.

    TODO: allow multiple columns and more exact configuration

-   `zstd_compress(data: text|blob, level: int = 3, dictionary: blob | int | null = null) -> blob`

    Compresses the given data, with the compression level (1 - 22, default 3)

    if dictionary is a blob it will be directly used
    if dictionary is an int i, it is functionally equivalent to `zstd_compress(data, level, (select dict from _zstd_dict where id = i))`

-   `zstd_decompress(data: blob, dictionary: blob | int | null = null) -> text|blob`

    Decompresses the given data. if the dictionary is wrong, the result is undefined

-   `zstd_train_dict(agg, dict_size: int, sample_count: int) -> blob`

    Aggregate function (like sum() or count()) to train a zstd dictionary on sample_count samples of the given aggregate data

    example use: `select zstd_train_dict(tbl.data, 100000, 1000) from tbl` will return a dictionary of size 100kB trained on 1000 samples in `tbl`

    The recommended number of samples is 100x the target dictionary size. As an example, you can train with the "optimal" sample count as follows:

    ```sql
    select zstd_train_dict(data, 100000, (select (100000 * 100 / avg(length(data))) as sample_count from tbl))
                    as dict from tbl
    ```

# Future Work / Ideas / Todo

-   tests
-   allow compression dict by grouping by other column (maybe unnecessary)
-   reduce header size of zstd compressed columns (currently makes the whole thing only worth it for fairly large columns)
    -   ContentSizeFlag
    -   ChecksumFlag
    -   DictIdFlag
-   correctly handle indices over compressed columns
-   do compression in different thread(s) for performance (may be as easy as using .multithread(1) in zstd)
-   type affinity interfers with int pass through - `insert into compressed (col) values (1)` will result in typeof(col) = text instead of integer if the type of the column was declared as text - which in turns causes decompression to fail with "got string, but zstd compressed data is always blob"
    -   either change the type of the compressed column to blob or similar or disallow integer passthrough
