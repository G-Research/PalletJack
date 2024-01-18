#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path);
std::shared_ptr<parquet::FileMetaData> ReadRowGroupsMetadata(const char *index_file_path, const std::vector<uint32_t>& row_groups);
