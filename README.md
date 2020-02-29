# sqlite-zstd

Extension for sqlite that provides transparent dictionary-based row-level compression for sqlite. This basically allows you to compress entries in a sqlite database almost as well as if you were compressing the whole DB file, but while retaining random access.

TODO: describe

-   how to load the extension
-   basic usage
-   data use statistics
-   performance implications
-   the whole api interface / all functions
-   how it works

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
