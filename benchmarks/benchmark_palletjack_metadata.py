
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import concurrent.futures
import time
import pyarrow.dataset as ds
from pyarrow import fs

row_groups = 200
columns = 200
chunk_size = 1000
rows = row_groups * chunk_size
work_items = 32
batch_size = 40
n_reads = 600

all_columns = list(range(columns))
all_row_groups = list(range(row_groups))
columns_batches = [all_columns[i:i+batch_size] for i in range(0, len(all_columns), batch_size)]
row_groups_batches = [all_row_groups[i:i+batch_size] for i in range(0, len(all_row_groups), batch_size)]

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


def measure_reading(max_workers, worker):

    def dummy_worker():
        time.sleep(0.01)

    tt = []
    # measure multiple times and take the fastest run
    for _ in range(0, 3):
        # Create the pool and warm it up 
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        dummy_items = [pool.submit(dummy_worker) for i in range(0, len(all_columns), batch_size)]
        for dummy_item in dummy_items: 
            dummy_item.result()

        # warm up the OS cache
        worker()

        # Submit the work
        t = time.time()
        for i in range(0, work_items):
            pool.submit(worker)

        pool.shutdown(wait=True)
        tt.append(time.time() - t)

    return min (tt)

def genrate_data(table):

    t = time.time()
    print(f"writing parquet file, columns={columns}, row_groups={row_groups}, rows={rows}")
    pq.write_table(table, parquet_path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, compression=None, store_schema=False)
    dt = time.time() - t
    print(f"finished writing parquet file in {dt:.2f} seconds")
    
# #table = get_table()
# genrate_data(table)

for i in range (0, 1000000):
    fragment = ds.ParquetFileFormat().make_fragment(parquet_path, filesystem=fs.LocalFileSystem(), row_groups=[1,2])
    fragment.take([0,1])

