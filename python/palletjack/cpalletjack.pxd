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
    cdef shared_ptr[CFileMetaData] ReadMetadata(const char *index_file_path, const vector[uint32_t] row_groups, const vector[uint32_t] column_indices, const vector[string] column_names, bint schema_only) except + nogil
    cdef shared_ptr[CFileMetaData] ReadMetadata(const unsigned char *index_data, size_t index_data_length, const vector[uint32_t] row_groups, const vector[uint32_t] column_indices, const vector[string] column_names, bint schema_only) except + nogil

cdef extern from "palletjack_capi.h":
    ctypedef shared_ptr[CArrowBuffer] (*PalletJackGenerateMetadataIndexToBufferV1Func)(const char *parquet_path) except + nogil
    ctypedef void (*PalletJackGenerateMetadataIndexToFileV1Func)(const char *parquet_path, const char *index_file_path) except + nogil
    ctypedef shared_ptr[CFileMetaData] (*PalletJackReadMetadataFromBufferV1Func)(const unsigned char *index_data, size_t index_data_length, const vector[uint32_t] &row_groups, const vector[uint32_t] &column_indices, const vector[string] &column_names, bint schema_only) except + nogil
    ctypedef shared_ptr[CFileMetaData] (*PalletJackReadMetadataFromFileV1Func)(const char *index_file_path, const vector[uint32_t] &row_groups, const vector[uint32_t] &column_indices, const vector[string] &column_names, bint schema_only) except + nogil
    cdef struct PalletJackCAPI:
        int version
        PalletJackGenerateMetadataIndexToBufferV1Func GenerateMetadataIndexToBufferV1
        PalletJackGenerateMetadataIndexToFileV1Func GenerateMetadataIndexToFileV1
        PalletJackReadMetadataFromBufferV1Func ReadMetadataFromBufferV1
        PalletJackReadMetadataFromFileV1Func ReadMetadataFromFileV1
    cdef const PalletJackCAPI* GetCAPI()
    cdef const char* PALLETJACK_CAPI_CAPSULE_NAME
    cdef int PALLETJACK_CAPI_VERSION
