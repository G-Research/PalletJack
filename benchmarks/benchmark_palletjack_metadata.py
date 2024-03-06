import palletjack as pj
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import concurrent.futures
import time
import os

row_groups = 200
columns = 200
chunk_size = 1000
rows = row_groups * chunk_size
work_items = 32
batch_size = 10

all_columns = list(range(columns))
all_row_groups = list(range(row_groups))
columns_batches = [all_columns[i:i+batch_size] for i in range(0, len(all_columns), batch_size)]
row_groups_batches = [all_row_groups[i:i+batch_size] for i in range(0, len(all_row_groups), batch_size)]

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

def worker_arrow_row_group():
    
    for r in range(0, row_groups):
        pr = pq.ParquetReader()
        pr.open(parquet_path)
        pr.read_row_groups([r], use_threads=False)

def worker_palletjack_row_group():
    
    for r in range(0, row_groups):
        metadata = pj.read_metadata(index_path, row_groups = [r])
        pr = pq.ParquetReader()
        pr.open(parquet_path, metadata = metadata)
        pr.read_row_groups([0], use_threads=False)

def worker_arrow_column():
    
    for c in range(0, columns):
        pr = pq.ParquetReader()
        pr.open(parquet_path)        
        pr.read_all(column_indices = [c], use_threads=False)

def worker_palletjack_column():
    
    for c in range(0, columns):
        metadata = pj.read_metadata(index_path, columns = [c])
        pr = pq.ParquetReader()
        pr.open(parquet_path, metadata = metadata)
        pr.read_all(use_threads=False)

def worker_arrow_row_groups():

    for row_groups_batch in row_groups_batches:
        pr = pq.ParquetReader()
        pr.open(parquet_path)        
        pr.read_row_groups(row_groups_batch, use_threads=False)

def worker_palletjack_rowgroups():
    
    for row_groups_batch in row_groups_batches:
        metadata = pj.read_metadata(index_path, row_groups = row_groups_batch)
        pr = pq.ParquetReader()
        pr.open(parquet_path, metadata=metadata)
        pr.read_row_groups(range(len(row_groups_batch)), use_threads=False)

def worker_arrow_columns():

    for columns_batch in columns_batches:
        pr = pq.ParquetReader()
        pr.open(parquet_path)
        pr.read_all(column_indices = columns_batch, use_threads=False)

def worker_palletjack_columns():

    for columns_batch in columns_batches:
        metadata = pj.read_metadata(index_path, columns=columns_batch)
        pr = pq.ParquetReader()
        pr.open(parquet_path, metadata=metadata)
        pr.read_all(use_threads=False)

def worker_palletjack_row_group_metadata():
    
    for r in range(0, row_groups):
       pj.read_metadata(index_path, row_groups = [r])

def worker_palletjack_column_metadata():
    
    for c in range(0, columns):
        pj.read_metadata(index_path, columns = [c])

def worker_palletjack_row_group_column_metadata():

    for r in range(0, row_groups):
        for c in range(0, columns):
            pj.read_metadata(index_path, row_groups = [r], columns = [c])

def worker_arrow_metadata():
    
    for r in range(0, row_groups):
        pr = pq.ParquetReader()
        pr.open(parquet_path)
        metadata = pr.metadata

def genrate_data(table):

    t = time.time()
    print(f"writing parquet file, columns={columns}, row_groups={row_groups}, rows={rows}")
    pq.write_table(table, parquet_path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None)
    dt = time.time() - t
    print(f"finished writing parquet file in {dt:.2f} seconds")

    t = time.time()
    print("Generating metadata index")
    pj.generate_metadata_index(parquet_path, index_path)
    dt = time.time() - t
    print(f"Metadata index generated in {dt:.2f} seconds")

    parquet_size = os.stat(parquet_path).st_size
    index_size = os.stat(index_path).st_size
    index_size_percentage = 100 * index_size / parquet_size
    print(f"Parquet size={parquet_size}, index size={index_size}({index_size_percentage:.2f}%)")
    print("")

def measure_reading(max_workers, worker):

    t = time.time()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    for i in range(0, work_items):
        pool.submit(worker)

    pool.shutdown(wait=True)
    return time.time() - t

table = get_table()
genrate_data(table)

print(f"Reading a single row group using arrow (single-threaded) {measure_reading(1, worker_arrow_row_group):.2f} seconds")
print(f"Reading a single row group using palletjack (single-threaded) {measure_reading(1, worker_palletjack_row_group):.2f} seconds")
print(f"Reading a single row group using arrow (multi-threaded) {measure_reading(8, worker_arrow_row_group):.2f} seconds")
print(f"Reading a single row group using palletjack (multi-threaded) {measure_reading(8, worker_palletjack_row_group):.2f} seconds")

print(f"Reading a single column using arrow (single-threaded) {measure_reading(1, worker_arrow_column):.2f} seconds")
print(f"Reading a single column using palletjack (single-threaded) {measure_reading(1, worker_palletjack_column):.2f} seconds")
print(f"Reading a single column using arrow (multi-threaded) {measure_reading(8, worker_arrow_column):.2f} seconds")
print(f"Reading a single column using palletjack (multi-threaded) {measure_reading(8, worker_palletjack_column):.2f} seconds")

print(f"Reading multiple row groups using arrow (single-threaded) {measure_reading(1, worker_arrow_row_groups):.2f} seconds")
print(f"Reading multiple row groups using palletjack (single-threaded) {measure_reading(1, worker_palletjack_rowgroups):.2f} seconds")
print(f"Reading multiple row groups using arrow (multi-threaded) {measure_reading(8, worker_arrow_row_groups):.3f} seconds")
print(f"Reading multiple row groups using palletjack (multi-threaded) {measure_reading(8, worker_palletjack_rowgroups):.3f} seconds")

print(f"Reading multiple columns using arrow (single-threaded) {measure_reading(1, worker_arrow_columns):.3f} seconds")
print(f"Reading multiple columns using palletjack (single-threaded) {measure_reading(1, worker_palletjack_columns):.3f} seconds")
print(f"Reading multiple columns using arrow (multi-threaded) {measure_reading(8, worker_arrow_columns):.3f} seconds")
print(f"Reading multiple columns using palletjack (multi-threaded) {measure_reading(8, worker_palletjack_columns):.3f} seconds")

print(f"Reading a single row group metadata using palletjack (single-threaded) {measure_reading(1, worker_palletjack_row_group_metadata):.3f} seconds")
print(f"Reading a single column metadata using palletjack (single-threaded) {measure_reading(1, worker_palletjack_column_metadata):.3f} seconds")
print(f"Reading a single row group and column metadata using palletjack (single-threaded) {measure_reading(1, worker_palletjack_row_group_column_metadata):.3f} seconds")
print(f"Reading a metadata using arrow (single-threaded) {measure_reading(1, worker_arrow_metadata):.3f} seconds")
