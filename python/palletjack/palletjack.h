#include "arrow/buffer.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

std::shared_ptr<arrow::Buffer> GenerateMetadataIndex(const char *parquet_path);
void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path);
std::shared_ptr<parquet::FileMetaData> ReadMetadata(const char *index_file_path,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names,
                                                    bool schema_only = false);

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const unsigned char *index_data,
                                                    size_t index_data_length,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names,
                                                    bool schema_only = false);

// Type of the index-data overload of ReadMetadata above, exported to other extension modules as the PyCapsule
// palletjack.palletjack_cython._read_metadata_capi. A caller holding the capsule can decode row group metadata
// from a mapped index without the GIL and without calling back into Python.
#define PALLETJACK_READ_METADATA_CAPSULE_NAME "palletjack.ReadMetadata"

using PalletJackReadMetadataFunc = std::shared_ptr<parquet::FileMetaData> (*)(const unsigned char *index_data,
                                                                             size_t index_data_length,
                                                                             const std::vector<uint32_t> &row_groups,
                                                                             const std::vector<uint32_t> &column_indices,
                                                                             const std::vector<std::string> &column_names,
                                                                             bool schema_only);

PalletJackReadMetadataFunc GetReadMetadataFunc();
