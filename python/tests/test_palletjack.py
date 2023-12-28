import unittest

import palletjack as pj
import pyarrow.parquet as pq
import polars as pl
import numpy as np

rows = 5
columns = 10
chunk_size = 1 # A row group per

class TestPalletJack(unittest.TestCase):

    def test_reading_with_index(self):
        
        path = "my.parquet"
        table = pl.DataFrame(
            data=np.random.randn(rows, columns),
            schema=[f"c{i}" for i in range(columns)]).to_arrow()

        pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

        index_path = path + '.index'
        pj.generate_metadata_index(path, index_path)
        for r in range(0, rows):

            # Reading using the original metadata
            pr = pq.ParquetReader()
            pr.open(path)
            res_data_org = pr.read_row_groups([r], use_threads=False)

            # Reading using the indexed metadata
            metadata = pj.read_row_group_metadata(index_path, r)
            pr = pq.ParquetReader()
            pr.open(path, metadata=metadata)
            
            res_data_index = pr.read_row_groups([0], use_threads=False)
            self.assertEqual(res_data_org, res_data_index, f"Row={r}")

    def test_reading_with_invalid_index(self):
        path = "my.parquet"
        table = pl.DataFrame(
            data=np.random.randn(rows, columns),
            schema=[f"c{i}" for i in range(columns)]).to_arrow()

        pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

        index_path = path + '.index'
        pj.generate_metadata_index(path, index_path)
        
        with self.assertRaises(Exception) as context:
            metadata = pj.read_row_group_metadata(index_path, rows)

        self.assertTrue(f"Requested row_group={rows}, but only 0-{rows-1} are available!" in str(context.exception), context.exception)
            
if __name__ == '__main__':
    unittest.main()