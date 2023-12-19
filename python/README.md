# PalletJack

How to use:

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