# distutils: include_dirs = .

import pyarrow as pa
import pyarrow.parquet as pq
from cython.cimports.palletjack import cpalletjack
from pyarrow.lib cimport Buffer

def generate_metadata_index(parquet_path, index_file_path):
    cpalletjack.GenerateMetadataIndex(parquet_path.encode('utf8'), index_file_path.encode('utf8'))

cpdef read_row_group_metadata(index_file_path, row_group):
    v = cpalletjack.ReadRowGroupMetadata(index_file_path.encode('utf8'), row_group)
    cdef char[::1] mv = <char[:v.size()]>&v[0]
    cdef Buffer pyarrow_buffer = pa.py_buffer(mv)
    return pq.core._parquet._reconstruct_filemetadata(pyarrow_buffer)
