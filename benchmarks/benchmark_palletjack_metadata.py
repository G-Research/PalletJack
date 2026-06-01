import palletjack as pj
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pyarrow as pa
import numpy as np
import pyarrow.fs as fs
import concurrent.futures
import tempfile
import humanize
import time
import os
import base64

from pydantic import computed_field
from pydantic_settings import BaseSettings, EnvSettingsSource, SettingsConfigDict


class CommaSeparatedEnvSource(EnvSettingsSource):
    def decode_complex_value(self, field_name, field, value):
        try:
            return super().decode_complex_value(field_name, field, value)
        except Exception:
            return value.split(",")


class BenchmarkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PJB_")

    row_groups: int = 200
    columns: int = 400
    chunk_size: int = 1000
    n_repeats: int = 1000
    measure_iterations: int = 5
    worker_counts: list[int] = [1, 2]
    dtype: str = "float32"
    parquet_path: str = os.path.join(tempfile.gettempdir(), "my.parquet")

    @computed_field
    @property
    def rows(self) -> int:
        return self.row_groups * self.chunk_size

    @computed_field
    @property
    def index_path(self) -> str:
        return self.parquet_path + '.index'

    @computed_field
    @property
    def encrypted_plaintext_footer_path(self) -> str:
        return self.parquet_path + '.enc_plain_footer.parquet'

    @computed_field
    @property
    def encrypted_plaintext_footer_index_path(self) -> str:
        return self.encrypted_plaintext_footer_path + '.index'

    @computed_field
    @property
    def encrypted_footer_path(self) -> str:
        return self.parquet_path + '.enc_footer.parquet'

    @computed_field
    @property
    def encrypted_footer_index_path(self) -> str:
        return self.encrypted_footer_path + '.index'

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            init_settings,
            CommaSeparatedEnvSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )


cfg = BenchmarkSettings()


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


def make_decryption_properties():
    dec_config = pe.DecryptionConfiguration(cache_lifetime=300)
    return crypto_factory.file_decryption_properties(get_kms_connection_config(), dec_config)

def worker_arrow_row_group():

    pr = pq.ParquetReader()
    pr.open(cfg.parquet_path)
    pr.read_row_groups([0], use_threads=False)

def worker_palletjack_row_group():

    metadata = pj.read_metadata(cfg.index_path, row_groups = [0])
    pr = pq.ParquetReader()
    pr.open(cfg.parquet_path, metadata = metadata)
    pr.read_row_groups([0], use_threads=False)

def worker_palletjack_row_group_metadata():

    pj.read_metadata(cfg.index_path, row_groups = [0])

def worker_palletjack_column_metadata():

    pj.read_metadata(cfg.index_path, column_indices = [0])

def worker_palletjack_column_name_metadata():

    pj.read_metadata(cfg.index_path, column_names = ['column_0'])

def worker_inmemory_palletjack_row_group_column_metadata(index_data):

    pj.read_metadata(index_data = index_data, row_groups = [0], column_indices = [0], preserve_indices=True)

def worker_palletjack_row_group_column_metadata():

    pj.read_metadata(cfg.index_path, row_groups = [0], column_indices = [0])

def worker_palletjack_encrypted_plaintext_footer_metadata():
    pj.read_metadata(cfg.encrypted_plaintext_footer_index_path, row_groups=[0], column_indices=[0])

def worker_palletjack_encrypted_footer_metadata():
    dec_props = make_decryption_properties()
    pj.read_metadata(cfg.encrypted_footer_index_path, row_groups=[0], column_indices=[0], decryption_properties=dec_props)

def worker_arrow_metadata():

    pr = pq.ParquetReader()
    pr.open(cfg.parquet_path)
    metadata = pr.metadata

def parquet_matches(path, n_columns, n_row_groups, chunk_size, dtype):
    if not os.path.exists(path):
        return False
    try:
        meta = pq.read_metadata(path)
        schema = pq.read_schema(path)
    except Exception:
        return False
    return (
        meta.num_columns == n_columns
        and meta.num_row_groups == n_row_groups
        and meta.row_group(0).num_rows == chunk_size
        and schema[0].type == dtype
    )

def generate_data():

    dtype = pa.from_numpy_dtype(cfg.dtype)

    if (
        parquet_matches(cfg.parquet_path, cfg.columns, cfg.row_groups, cfg.chunk_size, dtype)
        and os.path.exists(cfg.index_path)
    ):
        print(f"Reusing existing parquet file: {cfg.parquet_path}")
        parquet_size = os.stat(cfg.parquet_path).st_size
        index_size = os.stat(cfg.index_path).st_size
        index_size_percentage = 100 * index_size / parquet_size
        print(f"Parquet size={humanize.naturalsize(parquet_size)}, index size={humanize.naturalsize(index_size)}({index_size_percentage:.2f}%)")
        print("")
        return

    schema = pa.schema([pa.field(f'column_{i}', dtype) for i in range(cfg.columns)])
    data = np.random.rand(cfg.rows, cfg.columns).astype(cfg.dtype)
    pa_arrays = [pa.array(data[:, i], type=dtype) for i in range(cfg.columns)]
    table = pa.Table.from_arrays(pa_arrays, schema=schema)

    t = time.time()
    print(f"writing parquet file, columns={cfg.columns}, row_groups={cfg.row_groups}, rows={cfg.rows}")
    pq.write_table(table, cfg.parquet_path, row_group_size=cfg.chunk_size, use_dictionary=False, write_statistics=False, compression=None, store_schema=False)
    dt = time.time() - t
    print(f"finished writing parquet file in {dt:.2f} seconds")

    t = time.time()
    print("Generating metadata index")
    pj.generate_metadata_index(cfg.parquet_path, cfg.index_path)
    dt = time.time() - t
    print(f"Metadata index generated in {dt:.2f} seconds")

    parquet_size = os.stat(cfg.parquet_path).st_size
    index_size = os.stat(cfg.index_path).st_size
    index_size_percentage = 100 * index_size / parquet_size   
    print(f"Parquet size={humanize.naturalsize(parquet_size)}, index size={humanize.naturalsize(index_size)}({index_size_percentage:.2f}%)")
    
    print("")

def generate_encrypted_data():

    dtype = pa.from_numpy_dtype(cfg.dtype)
    schema = pa.schema([pa.field(f'column_{i}', dtype) for i in range(cfg.columns)])
    data = np.random.rand(cfg.rows, cfg.columns).astype(cfg.dtype)
    pa_arrays = [pa.array(data[:, i], type=dtype) for i in range(cfg.columns)]
    table = pa.Table.from_arrays(pa_arrays, schema=schema)

    # Encrypted with plaintext footer (column-level encryption)
    if not os.path.exists(cfg.encrypted_plaintext_footer_index_path):
        enc_config = pe.EncryptionConfiguration(
            footer_key='footer_key',
            column_keys={'col_key': [f'column_{i}' for i in range(cfg.columns)]},
            plaintext_footer=True,
        )
        enc_props = crypto_factory.file_encryption_properties(get_kms_connection_config(), enc_config)

        t = time.time()
        print(f"writing encrypted (plaintext footer) parquet file")
        pq.write_table(table, cfg.encrypted_plaintext_footer_path,
                       row_group_size=cfg.chunk_size, use_dictionary=False,
                       write_statistics=False, compression=None, store_schema=False,
                       encryption_properties=enc_props)
        dt = time.time() - t
        print(f"finished in {dt:.2f} seconds")

        t = time.time()
        print("Generating metadata index (plaintext footer)")
        pj.generate_metadata_index(cfg.encrypted_plaintext_footer_path, cfg.encrypted_plaintext_footer_index_path)
        dt = time.time() - t
        print(f"Metadata index generated in {dt:.2f} seconds")

        parquet_size = os.stat(cfg.encrypted_plaintext_footer_path).st_size
        index_size = os.stat(cfg.encrypted_plaintext_footer_index_path).st_size
        index_size_percentage = 100 * index_size / parquet_size
        print(f"Parquet size={humanize.naturalsize(parquet_size)}, index size={humanize.naturalsize(index_size)}({index_size_percentage:.2f}%)")
        print("")
    else:
        print(f"Reusing existing encrypted (plaintext footer) index: {cfg.encrypted_plaintext_footer_index_path}")

    # Encrypted with encrypted footer (uniform encryption)
    if not os.path.exists(cfg.encrypted_footer_index_path):
        enc_config = pe.EncryptionConfiguration(
            footer_key='footer_key',
            uniform_encryption=True,
            plaintext_footer=False,
        )
        enc_props = crypto_factory.file_encryption_properties(get_kms_connection_config(), enc_config)

        t = time.time()
        print(f"writing encrypted (encrypted footer) parquet file")
        pq.write_table(table, cfg.encrypted_footer_path,
                       row_group_size=cfg.chunk_size, use_dictionary=False,
                       write_statistics=False, compression=None, store_schema=False,
                       encryption_properties=enc_props)
        dt = time.time() - t
        print(f"finished in {dt:.2f} seconds")

        t = time.time()
        print("Generating metadata index (encrypted footer)")
        dec_props = make_decryption_properties()
        pj.generate_metadata_index(cfg.encrypted_footer_path, cfg.encrypted_footer_index_path, decryption_properties=dec_props)
        dt = time.time() - t
        print(f"Metadata index generated in {dt:.2f} seconds")

        parquet_size = os.stat(cfg.encrypted_footer_path).st_size
        index_size = os.stat(cfg.encrypted_footer_index_path).st_size
        index_size_percentage = 100 * index_size / parquet_size
        print(f"Parquet size={humanize.naturalsize(parquet_size)}, index size={humanize.naturalsize(index_size)}({index_size_percentage:.2f}%)")
        print("")
    else:
        print(f"Reusing existing encrypted (encrypted footer) index: {cfg.encrypted_footer_index_path}")

def measure_reading(max_workers, worker):

    tt = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    for _ in range(cfg.measure_iterations):

        # warm up the OS cache
        worker()

        # Submit the work
        t = time.time()
        futures = [pool.submit(worker) for i in range(cfg.n_repeats)]
        for f in futures:
            f.result()
        tt.append(time.time() - t)

    pool.shutdown(wait=True)

    tts = [f"{t:.2f}" for t in tt]
    tts = f"[{', '.join(tts)}]"
    return f"{min(tt):.2f}s -> {tts}"

print(".")
print(f"palletjack.version = {pj.__version__}")
print(f"pyarrow.version = {pa.__version__}")
print(".")
for name, value in cfg.model_dump().items():
    print(f"{name} = {value}")
print(".")

generate_data()
generate_encrypted_data()
index_data = fs.LocalFileSystem().open_input_stream(cfg.index_path).readall()

for n_workers in cfg.worker_counts:

    print(".")
    print(f"pj.read_metadata(in_memory, row_groups[0]+columns[0]) n_workers:{n_workers}, duration:{measure_reading(n_workers, lambda:worker_inmemory_palletjack_row_group_column_metadata(index_data))}")
    print(f"pj.read_metadata(row_groups[0]+columns[0]) n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_row_group_column_metadata)}")
    print(f"pj.read_metadata(row_groups[0]) n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_row_group_metadata)}")
    print(f"pj.read_metadata(column[0]) n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_column_metadata)}")
    print(f"pj.read_metadata(column['column_0']) n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_column_name_metadata)}")
    print(".")
    print(f"pj.read_metadata(enc_plaintext_footer, row_groups[0]+columns[0]) n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_encrypted_plaintext_footer_metadata)}")
    print(f"pj.read_metadata(enc_footer, row_groups[0]+columns[0]) n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_encrypted_footer_metadata)}")
    print(".")
    print(f"pq.ParquetReader.metadata n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_arrow_metadata)}")
    print(".")
    print(f"pq.read_row_groups[0] n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_arrow_row_group)}")
    print(f"pj.read_row_groups[0] n_workers:{n_workers}, duration:{measure_reading(n_workers, worker_palletjack_row_group)}")
