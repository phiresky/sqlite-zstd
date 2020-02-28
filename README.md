# sqlite-zstd

Extension for sqlite that provides transparent dictionary-based row-level compression for sqlite. This basically allows you to compress entries in a sqlite database almost as well as if you were compressing the whole DB file, but while retaining random access.

TODO: describe more

# Future Work / Ideas / Todo

-   tests
-   allow compression dict by grouping by other column (maybe unnecessary)
-   correctly handle indices over compressed columns
-   do compression in different thread(s) for performance
-   type affinity interfers with int pass through - `insert into compressed (col) values (1)` will result in typeof(col) = text instead of integer if the type of the column was declared as text - which in turns causes decompression to fail with "got string, but zstd compressed data is always blob"
    -   either change the type of the compressed column to blob or similar or disallow integer passthrough
