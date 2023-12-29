import unittest
import tempfile

import palletjack as pj
import pyarrow.parquet as pq
import polars as pl
import numpy as np
import os

rows = 5
columns = 10
chunk_size = 1 # A row group per

class TestPalletJack(unittest.TestCase):

    def test_reading(self):
        
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
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

    def test_reading_invalid_row_group(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = pl.DataFrame(
                data=np.random.randn(rows, columns),
                schema=[f"c{i}" for i in range(columns)]).to_arrow()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)
            
            with self.assertRaises(Exception) as context:
                metadata = pj.read_row_group_metadata(index_path, rows)

            self.assertTrue(f"Requested row_group={rows}, but only 0-{rows-1} are available!" in str(context.exception), context.exception)

    def test_reading_invalid_index_file(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = pl.DataFrame(
                data=np.random.randn(rows, columns),
                schema=[f"c{i}" for i in range(columns)]).to_arrow()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            with self.assertRaises(Exception) as context:
                metadata = pj.read_row_group_metadata(path, rows)

            self.assertTrue(f"File '{path}' has unexpected format!" in str(context.exception), context.exception)

    def test_index_file_endianness_compatibility(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            index_path = os.path.join(tmpdirname, 'my.parquet.index')
            path = 'test/data/sample.parquet'
            expected_index_path = 'test/data/sample.parquet.index'
            pj.generate_metadata_index(path, index_path)

            # Read the expected output
            with open(expected_index_path, 'rb') as file:
                expected_output = file.read()

            # Read the expected output
            with open(index_path, 'rb') as file:
                actual_output = file.read()

            # Compare the actual output to the expected output
            self.assertEqual(actual_output, expected_output)

        pr = pq.ParquetReader()
        pr.open(path)
        metadata = pr.metadata
        row_groups = metadata.num_row_groups

        for r in range(0, row_groups):

            # Reading using the original metadata
            pr = pq.ParquetReader()
            pr.open(path)
            res_data_org = pr.read_row_groups([r], use_threads=False)

            # Reading using the indexed metadata
            metadata = pj.read_row_group_metadata(expected_index_path, r)
            pr = pq.ParquetReader()
            pr.open(path, metadata=metadata)

            res_data_index = pr.read_row_groups([0], use_threads=False)
            self.assertEqual(res_data_org, res_data_index, f"Row={r}")

if __name__ == '__main__':
    unittest.main()