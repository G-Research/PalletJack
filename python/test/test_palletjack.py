import unittest
import tempfile

import palletjack as pj
import pyarrow.parquet as pq
import numpy as np
import pyarrow as pa
import os
import itertools as it

n_row_groups = 5
n_columns = 7
chunk_size = 1 # A row group per
current_dir = os.path.dirname(os.path.realpath(__file__))

def get_table():
    # Generate a random 2D array of floats using NumPy
    # Each column in the array represents a column in the final table
    data = np.random.rand(n_row_groups, n_columns)

    # Convert the NumPy array to a list of PyArrow Arrays, one for each column
    pa_arrays = [pa.array(data[:, i]) for i in range(n_columns)]

    # Optionally, create column names
    column_names = [f'column_{i}' for i in range(n_columns)]

    # Create a PyArrow Table from the Arrays
    return pa.Table.from_arrays(pa_arrays, names=column_names)

class TestPalletJack(unittest.TestCase):
      
    def test_read_metadata_columns_rows(self):

        def validate_reading(parquet_path, index_path, row_groups, columns):
            # Reading using the original metadata
            pr = pq.ParquetReader()
            pr.open(parquet_path)
            org_data = pr.read_row_groups(row_groups)
            if len(columns) > 0:                
                org_data = org_data.select(columns)

            # Reading using the indexed metadata
            metadata = pj.read_metadata(index_path, row_groups=row_groups, columns=columns)
            pr = pq.ParquetReader()
            pr.open(parquet_path, metadata=metadata)

            pj_data = pr.read_row_groups(list(range(0, len(row_groups))))
            self.assertEqual(org_data, pj_data, f"row_groups={row_groups}, columns={columns}")

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)
            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)

            all_columns = list(range(0, n_columns))
            all_row_groups = list(range(0, n_row_groups))
            for r in range(0, 4):
                for rp in it.permutations(all_row_groups, r):
                    for c in range(0, 4):
                        for cp in it.permutations(all_columns, c):
                            validate_reading(path, index_path, row_groups = rp, columns = cp)

    def test_read_row_group_metadata(self):
        
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)
            for r in range(0, n_row_groups):

                # Reading using the original metadata
                pr = pq.ParquetReader()
                pr.open(path)
                res_data_org = pr.read_row_groups([r], use_threads=False)

                # Reading using the indexed metadata
                metadata = pj.read_metadata(index_path, row_groups = [r])
                pr = pq.ParquetReader()
                pr.open(path, metadata=metadata)

                res_data_index = pr.read_row_groups([0], use_threads=False)
                self.assertEqual(res_data_org, res_data_index, f"Row={r}")

    def test_reading_row_groups(self):
        
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)

            # Reading using the original metadata
            pr = pq.ParquetReader()
            pr.open(path)
            res_data_org = pr.read_row_groups([2, 3, 4], use_threads=False)
            
            # Reading using the indexed metadata
            metadata = pj.read_metadata(index_path, row_groups = [2, 3, 4])
            pr = pq.ParquetReader()
            pr.open(path, metadata=metadata)

            res_data_index = pr.read_row_groups([0, 1, 2], use_threads=False)
            self.assertEqual(res_data_org, res_data_index)

    def test_reading_invalid_row_group(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)
            
            with self.assertRaises(RuntimeError) as context:
                metadata = pj.read_metadata(index_path, [n_row_groups])

            self.assertTrue(f"Requested row_group={n_row_groups}, but only 0-{n_row_groups-1} are available!" in str(context.exception), context.exception)

    def test_reading_invalid_column(self):
            with tempfile.TemporaryDirectory() as tmpdirname:
                path = os.path.join(tmpdirname, "my.parquet")
                table = get_table()

                pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

                index_path = path + '.index'
                pj.generate_metadata_index(path, index_path)
                
                with self.assertRaises(RuntimeError) as context:
                    metadata = pj.read_metadata(index_path, row_groups=[], columns=[n_columns])

                self.assertTrue(f"Requested column={n_columns}, but only 0-{n_columns-1} are available!" in str(context.exception), context.exception)

    def test_reading_invalid_index_file(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            with self.assertRaises(Exception) as context:
                metadata = pj.read_metadata(path, [0])

            self.assertTrue(f"File '{path}' has unexpected format!" in str(context.exception), context.exception)

    def test_index_file_golden_master(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            index_path = os.path.join(tmpdirname, 'my.parquet.index')
            path = os.path.join(current_dir, 'data/sample.parquet')
            expected_index_path = os.path.join(current_dir, 'data/sample.parquet.index')
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
            metadata = pj.read_metadata(expected_index_path, row_groups = [r])
            pr = pq.ParquetReader()
            pr.open(path, metadata=metadata)

            res_data_index = pr.read_row_groups([0], use_threads=False)
            self.assertEqual(res_data_org, res_data_index, f"Row={r}")

if __name__ == '__main__':
    unittest.main()
