from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr
from libc.stdint cimport uint32_t, int64_t
from pyarrow._parquet cimport *

cdef extern from "arrow/buffer.h" namespace "arrow":
    cdef cppclass CArrowBuffer "arrow::Buffer":
        const unsigned char* data()
        int64_t size()

cdef extern from "palletjack.h":
    cdef shared_ptr[CArrowBuffer] GenerateMetadataIndex(const char *parquet_path) except + nogil
    cdef void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path) except + nogil
    cdef shared_ptr[CFileMetaData] ReadMetadata(const char *index_file_path, const vector[uint32_t] row_groups, const vector[uint32_t] column_indices, const vector[string] column_names) except + nogil
    cdef shared_ptr[CFileMetaData] ReadMetadata(const unsigned char *index_data, size_t index_data_length, const vector[uint32_t] row_groups, const vector[uint32_t] column_indices, const vector[string] column_names) except + nogil
