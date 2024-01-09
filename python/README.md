# PalletJack
PalletJack was created as a workaround for apache/arrow#38149. The standard parquet reader is not ideal for files with numerous columns and row groups, as it requires parsing the entire metadata section each time the file is opened. The size of this metadata section is proportional to the number of columns and row groups in the file.

PalletJack reduces the amount of metadata bytes that need to be read and decoded by storing metadata in a different format. This approach enables reading only the essential subset of metadata as required.


## Features

- Storing parquet metadata in an indexed format
- Reading parquet metadata for a single row group
- Reading parquet metadata for multiple row groups

## Getting Started

To get started with PalletJack, clone the repository and build the project according to the instructions in the build section.

## Required:

 - pyarrow  >= 14
 
PalletJack is built on top of pyarrow, thus pyarrow is required to build and use PalletJack.
Our source package is comaptible with any recent version of pyarrow, but the binary distribution package is only compatible with latest major verison of pyarrow.


##  Installation

- pip install palletjack

------------

## How to use:

```
import palletjack as pj
import pyarrow.parquet as pq
import polars as pl
import numpy as np

rows = 5
columns = 10
chunk_size = 1 # A row group per

path = "my.parquet"
table = pl.DataFrame(
    data=np.random.randn(rows, columns),
    schema=[f"c{i}" for i in range(columns)]).to_arrow()

pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

# Reading using the original metadata
pr = pq.ParquetReader()
pr.open(path)
res_data = pr.read_row_groups([i for i in range(pr.num_row_groups)], column_indices=[0,1,2], use_threads=False)
print (res_data)

# Reading using the indexed metadata
index_path = path + '.index'
pj.generate_metadata_index(path, index_path)
for r in range(0, rows):
    metadata = pj.read_row_group_metadata(index_path, r)
    pr = pq.ParquetReader()
    pr.open(path, metadata=metadata)
    
    res_data = pr.read_row_groups([0], column_indices=[0,1,2], use_threads=False)
    print (res_data)
```