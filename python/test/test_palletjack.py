import unittest
import tempfile
import base64

import palletjack as pj
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pyarrow as pa
import numpy as np
import itertools as it
import pyarrow.fs as fs
import os
from parameterized import parameterized

n_row_groups = 5
n_columns = 7
chunk_size = 1 # One row group per chunk
current_dir = os.path.dirname(os.path.realpath(__file__))

class InMemoryKmsClient(pe.KmsClient):
    def __init__(self, config):
        super().__init__()
        self.master_keys = config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        master = self.master_keys[master_key_identifier].encode()
        padded = master * (len(key_bytes) // len(master) + 1)
        return base64.b64encode(bytes(a ^ b for a, b in zip(key_bytes, padded[:len(key_bytes)]))).decode()

    def unwrap_key(self, wrapped_key, master_key_identifier):
        key_bytes = base64.b64decode(wrapped_key)
        master = self.master_keys[master_key_identifier].encode()
        padded = master * (len(key_bytes) // len(master) + 1)
        return bytes(a ^ b for a, b in zip(key_bytes, padded[:len(key_bytes)]))

crypto_factory = pe.CryptoFactory(lambda config: InMemoryKmsClient(config))

def get_kms_connection_config():
    config = pe.KmsConnectionConfig()
    config.custom_kms_conf = {'footer_key': 'masterkey1234567', 'col_key': 'colmaster1234567'}
    return config

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

        def validate_reading(parquet_path, index_path, row_groups, column_indices):

            # Passing an empty list to the read_row_groups method is an invalid operation since Arrow 18.0.
            if (len(row_groups) == 0): return

            # Reading using the original metadata
            pr = pq.ParquetReader()
            pr.open(parquet_path)
            org_data = pr.read_row_groups(row_groups)
            if len(column_indices) > 0:                
                org_data = org_data.select(column_indices)

            # Reading using the indexed metadata
            metadata = pj.read_metadata(index_path, row_groups=row_groups, column_indices=column_indices)
            metadata_names = pj.read_metadata(index_path, row_groups=row_groups, column_names=[f'column_{i}' for i in column_indices])

            metadata_names_data = pj.read_metadata(index_data = fs.LocalFileSystem().open_input_stream(index_path).readall()
                                                   , row_groups=row_groups, column_names=[f'column_{i}' for i in column_indices])
            
            self.assertEqual(metadata, metadata_names, f"row_groups={row_groups}, column_indices={column_indices}")
            self.assertEqual(metadata_names_data, metadata_names, f"row_groups={row_groups}, column_indices={column_indices}")

            pr.close()

            pr = pq.ParquetReader()
            pr.open(parquet_path, metadata=metadata)

            pj_data = pr.read_row_groups(list(range(0, len(row_groups))))
            self.assertEqual(org_data, pj_data, f"row_groups={row_groups}, column_indices={column_indices}")

            pr.close()

        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)

            all_columns = list(range(n_columns))
            all_row_groups = list(range(n_row_groups))
            for r in range(3):
                for rp in it.permutations(all_row_groups, r):
                    for c in range(3):
                        for cp in it.permutations(all_columns, c):
                            validate_reading(path, index_path, row_groups = rp, column_indices = cp)

    def test_metadata_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)

            pr = pq.ParquetReader()
            pr.open(path)

            row_groups_columns = [
                ([], []),
                ([], range(n_columns)),
                (range(n_row_groups), []),
                (range(n_row_groups), range(n_columns)),
            ]

            for (row_groups, columns) in row_groups_columns:
                pj_metadata = pj.read_metadata(index_path, row_groups=row_groups, column_indices=columns)
                self.assertEqual(pr.metadata, pj_metadata)

            pr.close()

    def test_reading_non_pyarrow_files(self):
        
            path = os.path.join(current_dir, 'data/no_column_orders.parquet')
            pr = pq.ParquetReader()
            pr.open(path)

            row_groups_columns = [
                ([], []),
                ([], range(pr.metadata.num_columns)),
                (range(pr.metadata.num_row_groups), []),
                (range(pr.metadata.num_row_groups), range(pr.metadata.num_columns)),
            ]

            index_data = pj.generate_metadata_index(path)
            for (row_groups, columns) in row_groups_columns:
                pj_metadata = pj.read_metadata(index_data = index_data, row_groups=row_groups, column_indices=columns)
                self.assertEqual(pr.metadata, pj_metadata)

            pr.close()

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
                    metadata = pj.read_metadata(index_path, row_groups=[], column_indices=[n_columns])
                self.assertTrue(f"Requested column={n_columns}, but only 0-{n_columns-1} are available!" in str(context.exception), context.exception)

                with self.assertRaises(RuntimeError) as context:
                    metadata = pj.read_metadata(index_path, row_groups=[], column_names=["no_such_column"])
                self.assertTrue("Couldn't find a column with a name 'no_such_column'!" in str(context.exception), context.exception)

                with self.assertRaises(RuntimeError) as context:
                    metadata = pj.read_metadata(index_path, row_groups=[], column_indices=[n_columns], column_names=["n_columns0"])
                self.assertTrue("Cannot specify both column indices and column names at the same time!" in str(context.exception), context.exception)

    def test_reading_invalid_index_file(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            with self.assertRaises(Exception) as context:
                metadata = pj.read_metadata(path, [0])

            self.assertTrue(f"File '{path}' has unexpected format!" in str(context.exception), context.exception)

    def test_reading_missing_index_file(self):
            with self.assertRaises(Exception) as context:
                metadata = pj.read_metadata("not_existing_file.parquet.index", [0])

            self.assertTrue(f"IOError: Failed to open local file 'not_existing_file.parquet.index'" in str(context.exception), context.exception)

    def test_index_file_golden_master(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            index_path = os.path.join(tmpdirname, 'my.parquet.index')
            golden_master_path = os.path.join(current_dir, 'data/golden_master.parquet')
            pj.generate_metadata_index(golden_master_path, index_path)

            with open(os.path.join(current_dir, 'data/golden_master.parquet.index.v2'), 'rb') as file:
                golden_master_index_v2 = file.read()

            with open(os.path.join(current_dir, 'data/golden_master.parquet.index.v3'), 'rb') as file:
                golden_master_index_v3 = file.read()
                
            with open(index_path, 'rb') as file:
                actual_index = file.read()

            # Compare the actual output to the expected output
            self.assertEqual(actual_index, golden_master_index_v3)

            pr = pq.ParquetReader()
            pr.open(golden_master_path)
            org_metadata = pr.metadata
            pr.close()

            pj_v2_metadata = pj.read_metadata(index_data=golden_master_index_v2)
            self.assertEqual(org_metadata, pj_v2_metadata)
            
            pj_v3_metadata = pj.read_metadata(index_data=golden_master_index_v3)
            self.assertEqual(org_metadata, pj_v3_metadata)
            
            for r in range(org_metadata.num_row_groups):

                # Reading using the original metadata
                pr = pq.ParquetReader()
                pr.open(golden_master_path)
                expected_data = pr.read_row_groups([r])
                pr.close()

                # Reading using the indexed metadata(v2)
                pr = pq.ParquetReader()
                pr.open(golden_master_path, metadata=pj.read_metadata(index_data=golden_master_index_v2, row_groups = [r]))

                actual_data_v2 = pr.read_row_groups([0])
                self.assertEqual(expected_data, actual_data_v2, f"Row={r}")
                pr.close()

                pr = pq.ParquetReader()
                pr.open(golden_master_path, metadata=pj.read_metadata(index_data=golden_master_index_v3, row_groups = [r]))

                actual_data_v3 = pr.read_row_groups([0])
                self.assertEqual(expected_data, actual_data_v3, f"Row={r}")
                pr.close()

    def test_read_schema(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)

            pr = pq.ParquetReader()
            pr.open(path)
            expected_schema = pr.metadata.schema.to_arrow_schema()
            pr.close()

            # Full schema from file
            schema = pj.read_schema(index_path)
            self.assertEqual(schema, expected_schema)

            # Filtered by column_indices
            for c in range(n_columns):
                schema_col = pj.read_schema(index_path, column_indices=[c])
                self.assertEqual(len(schema_col), 1)
                self.assertEqual(schema_col.field(0), expected_schema.field(c))

            # Filtered by column_names
            for c in range(n_columns):
                name = f'column_{c}'
                schema_name = pj.read_schema(index_path, column_names=[name])
                self.assertEqual(len(schema_name), 1)
                self.assertEqual(schema_name.field(0).name, name)

            # Multiple columns
            indices = [0, 2, 4]
            schema_multi = pj.read_schema(index_path, column_indices=indices)
            self.assertEqual(len(schema_multi), len(indices))
            for i, idx in enumerate(indices):
                self.assertEqual(schema_multi.field(i), expected_schema.field(idx))

    def test_read_schema_errors(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)

            with self.assertRaises(RuntimeError):
                pj.read_schema(index_path, column_indices=[n_columns])

            with self.assertRaises(RuntimeError):
                pj.read_schema(index_path, column_names=["no_such_column"])

            with self.assertRaises(RuntimeError):
                pj.read_schema(index_path, column_indices=[0], column_names=["column_0"])

    def test_read_schema_non_pyarrow_files(self):
            path = os.path.join(current_dir, 'data/no_column_orders.parquet')
            pr = pq.ParquetReader()
            pr.open(path)
            expected_schema = pr.metadata.schema.to_arrow_schema()
            pr.close()

            index_data = pj.generate_metadata_index(path)
            schema = pj.read_schema(index_data=index_data)
            self.assertEqual(schema, expected_schema)

    def test_inmemory_index_data(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()

            pq.write_table(table, path, row_group_size=chunk_size, use_dictionary=False, write_statistics=False, store_schema=False)

            index_path = path + '.index'
            pj.generate_metadata_index(path, index_path)
            index_data1 = pj.generate_metadata_index(path)
            index_data2 = fs.LocalFileSystem().open_input_stream(index_path).readall()
            # Compare the actual output to the expected output
            self.assertEqual(index_data1, index_data2)

    def test_preserve_indices_row_groups(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()
            pq.write_table(table, path, row_group_size=chunk_size,
                           use_dictionary=False, write_statistics=False, store_schema=False)
            index_data = pj.generate_metadata_index(path)

            for r in range(n_row_groups):
                metadata = pj.read_metadata(index_data=index_data, row_groups=[r], preserve_indices=True)
                self.assertEqual(metadata.num_row_groups, n_row_groups)
                self.assertEqual(metadata.num_columns, n_columns)
                for rg_idx in range(n_row_groups):
                    rg = metadata.row_group(rg_idx)
                    if rg_idx == r:
                        self.assertEqual(rg.num_rows, 1)
                    else:
                        self.assertEqual(rg.num_rows, 0)

                pr = pq.ParquetReader()
                pr.open(path, metadata=metadata)
                actual = pr.read_row_groups([r])
                pr.close()

                pr = pq.ParquetReader()
                pr.open(path)
                expected = pr.read_row_groups([r])
                pr.close()

                self.assertEqual(actual, expected, f"row_group={r}")

            # Multiple row groups
            metadata = pj.read_metadata(index_data=index_data, row_groups=[1, 3], preserve_indices=True)
            self.assertEqual(metadata.num_row_groups, n_row_groups)
            self.assertEqual(metadata.row_group(0).num_rows, 0)
            self.assertEqual(metadata.row_group(1).num_rows, 1)
            self.assertEqual(metadata.row_group(2).num_rows, 0)
            self.assertEqual(metadata.row_group(3).num_rows, 1)
            self.assertEqual(metadata.row_group(4).num_rows, 0)

            pr = pq.ParquetReader()
            pr.open(path, metadata=metadata)
            actual = pr.read_row_groups([1, 3])
            pr.close()

            pr = pq.ParquetReader()
            pr.open(path)
            expected = pr.read_row_groups([1, 3])
            pr.close()
            self.assertEqual(actual, expected)

    def test_preserve_indices_columns(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()
            pq.write_table(table, path, row_group_size=chunk_size,
                           use_dictionary=False, write_statistics=False, store_schema=False)
            index_data = pj.generate_metadata_index(path)

            for c in range(n_columns):
                metadata = pj.read_metadata(index_data=index_data, column_indices=[c], preserve_indices=True)
                self.assertEqual(metadata.num_row_groups, n_row_groups)
                self.assertEqual(metadata.num_columns, n_columns)

                pf = pq.ParquetFile(path, metadata=metadata)
                actual = pf.read_row_groups(list(range(n_row_groups)), columns=[f'column_{c}'])

                pf2 = pq.ParquetFile(path)
                expected = pf2.read_row_groups(list(range(n_row_groups)), columns=[f'column_{c}'])

                self.assertEqual(actual, expected, f"column={c}")

            # Multiple columns
            metadata = pj.read_metadata(index_data=index_data, column_indices=[0, 2], preserve_indices=True)
            self.assertEqual(metadata.num_columns, n_columns)

            pf = pq.ParquetFile(path, metadata=metadata)
            actual = pf.read_row_groups(list(range(n_row_groups)), columns=['column_0', 'column_2'])

            pf2 = pq.ParquetFile(path)
            expected = pf2.read_row_groups(list(range(n_row_groups)), columns=['column_0', 'column_2'])
            self.assertEqual(actual, expected)

    def test_preserve_indices_combined(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "my.parquet")
            table = get_table()
            pq.write_table(table, path, row_group_size=chunk_size,
                           use_dictionary=False, write_statistics=False, store_schema=False)
            index_data = pj.generate_metadata_index(path)

            for r in range(n_row_groups):
                for c in range(n_columns):
                    metadata = pj.read_metadata(index_data=index_data, row_groups=[r],
                                                column_indices=[c], preserve_indices=True)
                    self.assertEqual(metadata.num_row_groups, n_row_groups)
                    self.assertEqual(metadata.num_columns, n_columns)

                    pf = pq.ParquetFile(path, metadata=metadata)
                    actual = pf.read_row_groups([r], columns=[f'column_{c}'])

                    pf2 = pq.ParquetFile(path)
                    expected = pf2.read_row_groups([r], columns=[f'column_{c}'])

                    self.assertEqual(actual, expected, f"row_group={r}, column={c}")

    def test_encrypted_footer_no_key(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "encrypted_footer.parquet")
            table = get_table()
            enc_config = pe.EncryptionConfiguration(
                footer_key='footer_key',
                uniform_encryption=True,
                plaintext_footer=False,
            )
            props = crypto_factory.file_encryption_properties(get_kms_connection_config(), enc_config)
            pq.write_table(table, path, encryption_properties=props)

            with self.assertRaises(RuntimeError) as context:
                pj.generate_metadata_index(path)
            self.assertIn("encrypted footer", str(context.exception))

    def test_encrypted_footer_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "encrypted_footer.parquet")
            table = get_table()
            enc_config = pe.EncryptionConfiguration(
                footer_key='footer_key',
                uniform_encryption=True,
                plaintext_footer=False,
            )
            enc_props = crypto_factory.file_encryption_properties(get_kms_connection_config(), enc_config)
            pq.write_table(table, path, row_group_size=chunk_size, encryption_properties=enc_props)

            dec_config = pe.DecryptionConfiguration(cache_lifetime=300)
            dec_props = crypto_factory.file_decryption_properties(get_kms_connection_config(), dec_config)

            index_data = pj.generate_metadata_index(path, decryption_properties=dec_props)
            self.assertIsNotNone(index_data)

            metadata = pj.read_metadata(index_data=index_data)
            self.assertEqual(metadata.num_row_groups, n_row_groups)
            self.assertEqual(metadata.num_columns, n_columns)

            for r in range(n_row_groups):
                metadata_r = pj.read_metadata(index_data=index_data, row_groups=[r])
                self.assertEqual(metadata_r.num_row_groups, 1)
                self.assertEqual(metadata_r.num_columns, n_columns)

                # Read actual data using the original file with decryption
                dec_props_read = crypto_factory.file_decryption_properties(
                    get_kms_connection_config(), pe.DecryptionConfiguration(cache_lifetime=300))
                pr = pq.ParquetReader()
                pr.open(path, decryption_properties=dec_props_read)
                expected_data = pr.read_row_groups([r])
                pr.close()

                # Read actual data using PalletJack metadata
                dec_props_pj = crypto_factory.file_decryption_properties(
                    get_kms_connection_config(), pe.DecryptionConfiguration(cache_lifetime=300))
                pr = pq.ParquetReader()
                pr.open(path, metadata=metadata_r, decryption_properties=dec_props_pj)
                actual_data = pr.read_row_groups([0])
                pr.close()

                self.assertEqual(expected_data, actual_data, f"Row group {r} data mismatch")

    def test_encrypted_column_metadata_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "encrypted_plaintext_footer.parquet")
            table = get_table()
            enc_config = pe.EncryptionConfiguration(
                footer_key='footer_key',
                column_keys={'col_key': [f'column_{i}' for i in range(n_columns)]},
                plaintext_footer=True,
            )
            props = crypto_factory.file_encryption_properties(get_kms_connection_config(), enc_config)
            pq.write_table(table, path, row_group_size=chunk_size, encryption_properties=props)

            dec_config = pe.DecryptionConfiguration(cache_lifetime=300)
            dec_props = crypto_factory.file_decryption_properties(get_kms_connection_config(), dec_config)

            pr = pq.ParquetReader()           
            # Read actual data using the original file with decryption
            dec_props_read = crypto_factory.file_decryption_properties(get_kms_connection_config(), pe.DecryptionConfiguration(cache_lifetime=300))
            pr.open(path, decryption_properties=dec_props_read)
            org_metadata = pr.metadata
            pr.close()
            
            pr = pq.ParquetReader()
            pr.open(path, metadata = org_metadata)
            expected_data = pr.read_row_groups([0])
            pr.close()

            index_data = pj.generate_metadata_index(path)
            self.assertIsNotNone(index_data)

            metadata = pj.read_metadata(index_data=index_data, decryption_properties=dec_props)
            self.assertEqual(metadata.num_row_groups, n_row_groups)
            self.assertEqual(metadata.num_columns, n_columns)

            for r in range(n_row_groups):
                
                pr = pq.ParquetReader()
                pr.open(path, decryption_properties=dec_props_read)
                expected_data = pr.read_row_groups([r])
                pr.close()

                metadata_r = pj.read_metadata(index_data=index_data, row_groups=[r], decryption_properties=dec_props_read)
                self.assertEqual(metadata_r.num_row_groups, 1)
                self.assertEqual(metadata_r.num_columns, n_columns)

                pr = pq.ParquetReader()
                pr.open(path, metadata=metadata_r)
                actual_data = pr.read_row_groups([0])
                pr.close()

                self.assertEqual(expected_data, actual_data, f"Row group {r} data mismatch")

            for c in range(n_columns):
                dec_props_c = crypto_factory.file_decryption_properties(
                    get_kms_connection_config(), pe.DecryptionConfiguration(cache_lifetime=300))
                metadata_c = pj.read_metadata(index_data=index_data, column_indices=[c], decryption_properties=dec_props_c)
                self.assertEqual(metadata_c.num_columns, 1)

                # Read actual column data using PalletJack metadata
                dec_props_read = crypto_factory.file_decryption_properties(
                    get_kms_connection_config(), pe.DecryptionConfiguration(cache_lifetime=300))
                pr = pq.ParquetReader()
                pr.open(path, decryption_properties=dec_props_read)
                expected_col = pr.read_row_groups(list(range(n_row_groups))).select([c])
                pr.close()

                dec_props_pj = crypto_factory.file_decryption_properties(
                    get_kms_connection_config(), pe.DecryptionConfiguration(cache_lifetime=300))
                pr = pq.ParquetReader()
                pr.open(path, metadata=metadata_c, decryption_properties=dec_props_pj)
                actual_col = pr.read_row_groups(list(range(n_row_groups)))
                pr.close()

                self.assertEqual(expected_col, actual_col, f"Column {c} data mismatch")

if __name__ == '__main__':
    # unittest.main()
    unittest.main(argv=['first-arg-is-ignored', '-k', 'TestPalletJack.test_preserve_indices_columns'])

