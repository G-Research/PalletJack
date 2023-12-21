import pyarrow.parquet as pq
import polars as pl
import numpy as np
import time
import concurrent.futures
import os
import pickle 

rows = 100
columns = 1000
chunk_size = 1 # A row group per
workers = 8
work_items = 100

path = "my.parquet"

def worker_pickle():
    
    for r in range(0, rows):
        m = pickle.loads(pickle.dumps(metadata))
        {}

def worker_org_metadata():
    
    for r in range(0, rows):
        pr = pq.ParquetReader()
        pr.open(path)
        m = pr.metadata
        {}

table = pl.DataFrame(
    data=np.random.randn(rows, columns),
    schema=[f"c{i}" for i in range(columns)]).to_arrow()

if not os.path.isfile(path):
    print ("writing table begin")
    pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)
    print ("writing table end")

    # Reading using the indexed metadata
    print ("generate_metadata_index begin")
    pj.generate_metadata_index(path, index_path)
    print ("generate_metadata_index end")
 
for i in range(0, 1000):
    for r in range(0, rows):
        pr = pq.ParquetReader()
        pr.open(path)
        m = pr.metadata
        {}