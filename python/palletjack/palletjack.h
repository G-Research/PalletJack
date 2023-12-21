#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path);
std::vector<char> ReadRowGroupMetadata(const char *index_file_path, uint32_t row_group);
std::shared_ptr<parquet::FileMetaData> ReadRowGroupFileMetadata(const char *index_file_path, uint32_t row_group);
