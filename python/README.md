# PalletJack
PalletJack was created as a workaround for apache/arrow#38149. The standard parquet reader is not efficient for files with numerous columns and row groups, as it requires parsing the entire metadata section each time the file is opened. The size of this metadata section is proportional to the number of columns and row groups in the file.

PalletJack reduces the amount of metadata bytes that need to be read and decoded by storing metadata in a different format. This approach enables reading only the essential subset of metadata as required.

## Features

- Storing parquet metadata in an indexed format
- Reading parquet metadata for a subset of row groups and columns

## Required:

- pyarrow  ~= 15.0
 
PalletJack operates on top of pyarrow, making it an essential requirement for both building and using PalletJack. While our source package is compatible with recent versions of pyarrow, the binary distribution package specifically requires the latest major version of pyarrow.

##  Installation

```
pip install palletjack
```

## How to use:


### Generating a sample parquet file:
```
import palletjack as pj
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np

row_groups = 200
columns = 200
chunk_size = 1000
rows = row_groups * chunk_size
path = "my.parquet"

data = np.random.rand(rows, columns)
pa_arrays = [pa.array(data[:, i]) for i in range(columns)]
column_names = [f'column_{i}' for i in range(columns)]
table = pa.Table.from_arrays(pa_arrays, names=column_names)
pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)
```

### Generating the indexed metadata file:
```
index_path = path + '.index'
pj.generate_metadata_index(path, index_path)
```

### Reading some row groups using the indexed metadata:
```
metadata = pj.read_metadata(index_path, row_groups = [5, 7])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)

data = pr.read_all()
```

### Reading some columns using the indexed metadata:
```
metadata = pj.read_metadata(index_path, column_indices = [1, 3])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)

data = pr.read_all()
```
```
metadata = pj.read_metadata(index_path, column_names = ['column_1', 'column_3'])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)

data = pr.read_all()
```

### Reading some row groups and some columns using the indexed metadata:
```
metadata = pj.read_metadata(index_path, row_groups = [5, 7], column_indices = [1, 3])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)

data = pr.read_all()
```
```
metadata = pj.read_metadata(index_path, row_groups = [5, 7], column_names = ['column_1', 'column_3'])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)

data = pr.read_all()
```
