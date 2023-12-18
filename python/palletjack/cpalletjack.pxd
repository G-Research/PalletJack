
from libcpp.vector cimport vector

cdef extern from "palletjack.h":
    cdef void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path)
    cdef vector[char] ReadRowGroupMetadata(const char *index_file_path, int row_group)
