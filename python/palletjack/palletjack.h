#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

std::vector<char> GenerateMetadataIndex(const char *parquet_path);
void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path);
std::shared_ptr<parquet::FileMetaData> ReadMetadata(const char *index_file_path,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names);

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const unsigned char *index_data,
                                                    size_t index_data_length,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names);
