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

    cdef shared_ptr[CFileMetaData] c_metadata
    cdef string encoded_path = index_file_path.encode('utf8')
    cdef uint32_t crow_group = row_group
    with nogil:
        c_metadata = cpalletjack.ReadRowGroupMetadata(encoded_path.c_str(), crow_group)

    # Can we jsut create our own copy of file metadata ? It is a python, so no one cares if we reurn another type of object 
    cdef FileMetaData m = FileMetaData.__new__(FileMetaData)
    m.init(c_metadata)
    return m

cpdef read_row_groups_metadata(index_file_path, row_groups):

    cdef shared_ptr[CFileMetaData] c_metadata
    cdef string encoded_path = index_file_path.encode('utf8')
    cdef vector[uint32_t] crow_groups = row_groups
    with nogil:
        c_metadata = cpalletjack.ReadRowGroupsMetadata(encoded_path.c_str(), crow_groups)

    # Can we jsut create our own copy of file metadata ? It is a python, so no one cares if we reurn another type of object 
    cdef FileMetaData m = FileMetaData.__new__(FileMetaData)
    m.init(c_metadata)
    return m
