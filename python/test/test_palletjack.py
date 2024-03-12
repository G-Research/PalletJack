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

if __name__ == '__main__':
    unittest.main()
