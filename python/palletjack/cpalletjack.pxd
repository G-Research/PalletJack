
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr
from libc.stdint cimport uint32_t
from pyarrow._parquet cimport *

cdef extern from "palletjack.h":
    cdef void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path) nogil except +
    cdef shared_ptr[CFileMetaData] ReadRowGroupsMetadata(const char *index_file_path, const vector[uint32_t] row_groups) nogil except +
