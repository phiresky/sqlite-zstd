# Future Work / Ideas / Todo

-   allow compression dict by grouping by other column (maybe unnecessary)
-   correctly handle indices over compressed columns
-   do compression in different thread(s) for performance
-   type affinity interfers with int pass through - `insert into compressed (col) values (1)` will result in typeof(col) = text instead of integer if the type of the column was declared as text - which in turns causes decompression to fail with "got string, but zstd compressed data is always blob"
    -   either change the type of the compressed column to blob or similar or disallow integer passthrough
