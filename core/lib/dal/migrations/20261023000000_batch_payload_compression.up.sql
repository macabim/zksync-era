-- Compress new batch payloads with LZ4. Existing rows need no rewrite.
ALTER TABLE l1_batches
    ALTER COLUMN initial_bootloader_heap_content SET COMPRESSION lz4,
    ALTER COLUMN pubdata_input SET COMPRESSION lz4;
