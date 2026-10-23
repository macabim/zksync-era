ALTER TABLE l1_batches
    ALTER COLUMN initial_bootloader_heap_content SET COMPRESSION default,
    ALTER COLUMN pubdata_input SET COMPRESSION default;
