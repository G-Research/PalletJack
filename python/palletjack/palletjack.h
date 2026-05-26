#include "arrow/buffer.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"
#include "parquet/encryption/encryption.h"

std::shared_ptr<arrow::Buffer> GenerateMetadataIndex(
    const char *parquet_path,
    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties);

void GenerateMetadataIndex(
    const char *parquet_path,
    const char *index_file_path,
    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties);

std::shared_ptr<parquet::FileMetaData> ReadMetadata(
    const char *index_file_path,
    const std::vector<uint32_t> &row_groups,
    const std::vector<uint32_t> &column_indices,
    const std::vector<std::string> &column_names,
    bool schema_only,
    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties);

std::shared_ptr<parquet::FileMetaData> ReadMetadata(
    const unsigned char *index_data,
    size_t index_data_length,
    const std::vector<uint32_t> &row_groups,
    const std::vector<uint32_t> &column_indices,
    const std::vector<std::string> &column_names,
    bool schema_only,
    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties);
