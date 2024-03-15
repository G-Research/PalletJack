import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import time
import pyarrow.dataset as ds
import pyarrow.fs as fs
import subprocess

row_groups = 10
columns = 200
chunk_size = 100_000
rows = row_groups * chunk_size
work_items = 1

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

def genrate_data(table):

    t = time.time()
    print(f"writing parquet file, columns={columns}, row_groups={row_groups}, rows={rows}")
    pq.write_table(table, parquet_path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None, store_schema=False)
    dt = time.time() - t
    print(f"finished writing parquet file in {dt:.2f} seconds")

def measure_reading(worker):

    tt = []
    # measure multiple times and take the fastest run
    for _ in range(0, 10):

        # Submit the work
        t = time.time()
        for i in range(0, work_items):
            worker()

        tt.append(time.time() - t)

    return min (tt)

def clear_cache():
    # print('clearing cache')
    p = subprocess.run(
                'sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"',
                shell=True,
                timeout=10,
            )
    if p.returncode != 0:
        raise Exception(f"Exit code:{p.returncode}, {p.stderr}, {p.stdout}")

def worker_parquet_file(memory_map):
    
    for r in range(0, row_groups):
        clear_cache()
        pq.ParquetFile(parquet_path, memory_map=memory_map, pre_buffer=True, filesystem=fs.LocalFileSystem(use_mmap=True)).read_row_group(r)

def worker_file_fragment(memory_map):
    
    for r in range(0, row_groups):
        clear_cache()
        ds.ParquetFileFormat(default_fragment_scan_options = ds.ParquetFragmentScanOptions(pre_buffer=True)).make_fragment(parquet_path, filesystem=fs.LocalFileSystem(use_mmap=memory_map), row_groups=[r]).to_table()

table = get_table()
genrate_data(table)

print(f"Reading a single row group using parquet file (map=false, single-threaded) {measure_reading(lambda:worker_parquet_file(False)):.2f} seconds")
print(f"Reading a single row group using parquet file (map=true, single-threaded) {measure_reading(lambda:worker_parquet_file(True)):.2f} seconds")
print(f"Reading a single row group using file fragment (map=false, single-threaded) {measure_reading(lambda:worker_file_fragment(False)):.2f} seconds")
print(f"Reading a single row group using file fragment (map=true, single-threaded) {measure_reading(lambda:worker_file_fragment(True)):.2f} seconds")
print(".")
print(".")
