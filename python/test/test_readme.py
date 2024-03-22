### Generating a sample parquet file:
# ```
import palletjack as pj
import pyarrow.parquet as pq
import pyarrow.fs as fs
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
# ```

### Generating the metadata index file:
# ```
index_path = path + '.index'
pj.generate_metadata_index(path, index_path)
# ```

### Generating the in-memory metadata index:
# ```
index_data = pj.generate_metadata_index(path)
# ```

### Writing the in-memory metadata index to a file using pyarrow's fs:
# ```
fs.LocalFileSystem().open_output_stream(index_path).write(index_data)
# ```

### Reading the in-memory metadata index from a file using pyarrow's fs:
# ```
index_data = fs.LocalFileSystem().open_input_stream(index_path).readall()
# ```

### Reading data with help of index file:
# ```
metadata = pj.read_metadata(index_path, row_groups = [5, 7])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)
data = pr.read_all()
# ```

### Reading data with help of in-memory index:
# ```
metadata = pj.read_metadata(index_data = index_data, row_groups = [5, 7])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)
data = pr.read_all()
# ```

### Reading subset of columns using column indices:
# ```
metadata = pj.read_metadata(index_path, column_indices = [1, 3])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)
data = pr.read_all()
# ```

### Reading subset of columns using column names:
# ```
metadata = pj.read_metadata(index_path, column_names = ['column_1', 'column_3'])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)
data = pr.read_all()
# ```

### Reading subset of row groups and columns:
# ```
metadata = pj.read_metadata(index_path, row_groups = [5, 7], column_indices = [1, 3])
pr = pq.ParquetReader()
pr.open(path, metadata=metadata)

data = pr.read_all()
# ```
