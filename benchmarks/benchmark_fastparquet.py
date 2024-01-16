import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import time

from fastparquet import ParquetFile

row_groups = 200
columns = 200
chunk_size = 1000
rows = row_groups * chunk_size
work_items = 1
batch_size = 10

parquet_path = "/tmp/my.parquet"
index_path = parquet_path + '.index'

def get_table():
    # Generate a random 2D array of floats using NumPy
    # Each column in the array represents a column in the final table
    data = np.random.rand(rows, columns)

    # Convert the NumPy array to a list of PyArrow Arrays, one for each column
    pa_arrays = [pa.array(data[:, i]) for i in range(columns)]

    # Optionally, create column names
    column_names = [f'column_{i}' for i in range(columns)]

    # Create a PyArrow Table from the Arrays
    return pa.Table.from_arrays(pa_arrays, names=column_names)

def worker_arrow():
   for r in range(0, row_groups):
        pr = pq.ParquetReader()
        pr.open(parquet_path)        
        res_data = pr.read_row_groups([0], use_threads=False)

def worker_fastparquet():
    for r in range(0, row_groups):
         pf = ParquetFile(parquet_path)
         res_data = pf[0].to_pandas()

def genrate_data(table, store_schema):

   t = time.time()
   print(f"writing parquet file, columns={columns}, row_groups={row_groups}, rows={rows}")
   pq.write_table(table, parquet_path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=store_schema, compression=None)
   dt = time.time() - t
   print(f"finished writing parquet file in {dt:.2f} seconds")

def measure_reading(worker):

    t = time.time()
    for i in range(0, work_items):
        worker()

    return time.time() - t

table = get_table()
genrate_data(table, False)

print(f"Reading all row groups using arrow (single-threaded) {measure_reading(worker_arrow):.3f} seconds")
print(f"Reading all row groups using fastparquet (single-threaded) {measure_reading(worker_fastparquet):.3f} seconds")
