# Data Management and Persistence

### Memory-Mapped Files:
* The database uses memory-mapped files (mmap) to manage chunks of the database in memory. This allows efficient access and modification of the database contents. 
* The mmapInit function initializes the memory-mapped file, mapping the entire file into memory.
<br><br>


### Page Management:
* The KV struct contains a page field that tracks the number of flushed pages (flushed) and temporarily allocated pages (temp).
* Pages are allocated and managed using the pageGet, pageNew, and pageDel methods.
<br><br>


### Writing Data to Disk:
* The writePages function is responsible for extending the file and memory-mapped regions if needed and copying data from the temporary pages to the file.
* The extendFile function ensures that the file is large enough to accommodate the required number of pages.
* The extendMmap function ensures that the memory-mapped region is large enough to accommodate the required number of pages.
* The copy operation in writePages copies data from the temporary pages to the appropriate location in the memory-mapped region.
<br><br>

### Flushing Data to Disk:
* The flushPages function calls writePages to copy data to the file and then calls syncPages to ensure that the data is persisted to disk.
* The syncPages function calls db.fp.Sync() to flush the file's contents to disk, ensuring that all changes are written to the physical storage.
* After flushing the data, syncPages updates the meta-page by calling metapageStore and then flushes the file again to ensure the meta-page is also persisted.
<br><br>

### Meta-Page Management:
* The meta-page contains important metadata about the database, such as the root pointer of the B-tree and the number of flushed pages.
* The metapageStore function updates the meta-page with the current state of the database and writes it to the beginning of the file using WriteAt.
* The metapageLoad function reads the meta-page from the file and verifies its contents to ensure the database is in a consistent state.
<br><br>

### Responsibility for Data Persistence:
1. `pager.go`:
* The primary responsibility for data persistence lies in the pager.go file. It contains functions like writePages, flushPages, syncPages, extendFile, and extendMmap, which handle the actual writing of data to disk and managing the memory-mapped regions.
* The metapageStore and metapageLoad functions in pager.go manage the meta-page, ensuring that the database's metadata is correctly persisted and loaded.
<br><br>

2. `btree.go`:
* The btree.go file is responsible for the logical structure and operations of the B-tree, such as inserting, deleting, and retrieving keys and values.
* It interacts with the pager to allocate, deallocate, and retrieve pages but does not directly handle the persistence of data to disk.