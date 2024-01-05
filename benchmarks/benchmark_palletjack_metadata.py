import palletjack as pj
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import concurrent.futures
import unittest
import time
import os

row_groups = 200
columns = 200
chunk_size = 100
rows = row_groups * chunk_size
work_items = 64

parquet_path = "my.parquet"
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

def worker_palletjack():
    
    for r in range(0, rows):
        metadata = pj.read_row_group_metadata(index_path, r)
        pr = pq.ParquetReader()
        pr.open(parquet_path, metadata=metadata)
        res_data = pr.read_row_groups([0], column_indices=[0,1,2], use_threads=False)

def worker_arrow():
    
    for r in range(0, rows):
        pr = pq.ParquetReader()
        pr.open(parquet_path)        
        res_data = pr.read_row_groups([0], column_indices=[0,1,2], use_threads=False)

def genrate_data(table, store_schema):

    t = time.time()
    print(f"writing parquet file, columns={columns}, row_groups={row_groups}, rows={rows}")
    pq.write_table(table, parquet_path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=store_schema, compression=None)
    dt = time.time() - t
    print(f"finsihed writing parquet file in {dt:.2f} seconds")

    t = time.time()
    print("Generating metadata index")
    pj.generate_metadata_index(parquet_path, index_path)
    dt = time.time() - t
    print(f"Metadata index generated in {dt:.2f} seconds")

    parquet_size = os.stat(parquet_path).st_size
    index_size = os.stat(index_path).st_size
    index_size_percentage = 100 * index_size / parquet_size
    print(f"Parquet size={parquet_size}, store_schema={store_schema}, index size={index_size}({index_size_percentage:.2f}%)")
    print("")

def measure_reading(max_workers, worker):

    t = time.time()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    for i in range(0, work_items):
        pool.submit(worker)

    pool.shutdown(wait=True)
    return time.time() - t

table = get_table()
genrate_data(table, True)
genrate_data(table, False)

print(f"Reading single threaded using arrow {measure_reading(1, worker_arrow):.2f} seconds")
print(f"Reading single threaded using palletjack {measure_reading(1, worker_palletjack):.2f} seconds")
print(f"Reading multithreaded using arrow {measure_reading(1, worker_arrow):.2f} seconds")
print(f"Reading multithreaded using palletjack {measure_reading(1, worker_palletjack):.2f}seconds")
