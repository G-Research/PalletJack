# distutils: include_dirs = .

import pyarrow as pa
import pyarrow.parquet as pq
from cython.cimports.palletjack import cpalletjack
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t
from pyarrow._parquet cimport *

def generate_metadata_index(parquet_path, index_file_path):
    cpalletjack.GenerateMetadataIndex(parquet_path.encode('utf8'), index_file_path.encode('utf8'))

cpdef read_row_group_metadata(index_file_path, row_group):
    return read_row_groups_metadata(index_file_path, [row_group])

cpdef read_row_groups_metadata(index_file_path, row_groups):

    return read_metadata(index_file_path, row_groups, [])

cpdef read_metadata(index_file_path, row_groups, columns):

    cdef shared_ptr[CFileMetaData] c_metadata
    cdef string encoded_path = index_file_path.encode('utf8')
    cdef vector[uint32_t] crow_groups = row_groups
    cdef vector[uint32_t] ccolumns = columns
    with nogil:
        c_metadata = cpalletjack.ReadMetadata(encoded_path.c_str(), crow_groups, ccolumns)

    cdef FileMetaData m = FileMetaData.__new__(FileMetaData)
    m.init(c_metadata)
    return m