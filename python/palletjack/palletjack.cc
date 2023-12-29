#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

#include "palletjack.h"

#include <iostream>
#include <fstream>

using arrow::Status;

#define TO_FILE_ENDIANESS(x) (x)
#define FROM_FILE_ENDIANESS(x) (x)
const char* HEADER_V1 = "PJ_1";

/* File format: (Thrift-encoded metadata stored separately for each row group)
--------------------------
| 0 - 3 | PQT1           | File header in ASCI
|------------------------|
| 4 - 7 | Row groups (n) | (uint32)
|------------------------|
| 8 - 11| Offset [0]     | (uint32)
|------------------------|
|12 - 15| Length [0]     | (uint32)
|------------------------|
|16 - 19| Offset [1]     | (uint32)
|------------------------|
|20 - 23| Length [1]     | (uint32)
|------------------------|

         .   .   .

|------------------------|
|.. - ..| Offset [n-1]   | (uint32)
|------------------------|
|.. - ..| Length [n-1]   | (uint32)
|------------------------|
|    Row group [0]       | Thrift data
|------------------------|
|    Row group [1]       | Thrift data
|------------------------|

         .   .   .

|------------------------|
|   Row group [n-1]      | Thrift data
--------------------------
*/
void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(parquet_path)));
    auto metadata = parquet::ReadMetaData(infile);
    uint32_t row_groups = (metadata->num_row_groups());

    std::ofstream fs(index_file_path, std::ios::out | std::ios::binary);
    fs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    fs.write(&HEADER_V1[0], strlen(HEADER_V1));
    fs.write((char *)&TO_FILE_ENDIANESS(row_groups), sizeof(row_groups));
    auto offset0 = fs.tellp();

    // Write placeholders for offset and length
    for (uint32_t row_group = 0; row_group < row_groups; row_group++)
    {
        uint32_t zero = 0;
        fs.write((char *)&TO_FILE_ENDIANESS(zero), sizeof(zero));
        fs.write((char *)&TO_FILE_ENDIANESS(zero), sizeof(zero));
    }

    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    uint32_t offset = fs.tellp();
    for (uint32_t row_group = 0; row_group < row_groups; row_group++)
    {
        std::vector<int> rows_groups_subset = {(int)row_group};
        auto metadata_subset = metadata->Subset(rows_groups_subset);
        std::shared_ptr<arrow::io::BufferOutputStream> stream;
        PARQUET_ASSIGN_OR_THROW(stream, arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool()));
        metadata_subset.get()->WriteTo(stream.get());
        std::shared_ptr<arrow::Buffer> thrift_buffer;
        PARQUET_ASSIGN_OR_THROW(thrift_buffer, stream.get()->Finish());
        uint32_t length = thrift_buffer.get()->size();
        fs.write((const char *)thrift_buffer.get()->data(), length);
        offsets.push_back(offset);
        lengths.push_back(length);
        offset += length;
    }

    // Now move the file poitner back and write offsets and lengths
    fs.seekp(offset0, std::ios_base::beg);
    for (uint32_t row_group = 0; row_group < row_groups; row_group++)
    {
        fs.write((char *)&TO_FILE_ENDIANESS(offsets[row_group]), sizeof(offsets[row_group]));
        fs.write((char *)&TO_FILE_ENDIANESS(lengths[row_group]), sizeof(lengths[row_group]));
    }
}

std::shared_ptr<parquet::FileMetaData> ReadRowGroupMetadata(const char *index_file_path, uint32_t row_group)
{
    std::ifstream fs(index_file_path, std::ios::binary);
    fs.exceptions(std::ifstream::failbit | std::ifstream::badbit);

    std::vector<char> header(strlen(HEADER_V1));
    fs.read(&header[0], header.size());

    if (memcmp(&HEADER_V1[0], &header[0], strlen(HEADER_V1)) != 0)
    {
        auto msg = std::string("File '") + index_file_path + "' has unexpected format!";
        throw std::runtime_error(msg);
    }
    
    uint32_t row_groups;
    fs.read((char *)&row_groups, sizeof(row_groups));
    row_groups = FROM_FILE_ENDIANESS(row_groups);
    if (row_group >= row_groups)
    {
        auto msg = std::string("Requested row_group=") + std::to_string(row_group) + ", but only 0-" + std::to_string(row_groups-1) + " are available!";
        throw std::runtime_error(msg);
    }

    // Seek to the offset and length
    fs.seekg(2 * row_group * sizeof(uint32_t), std::ios_base::cur);

    uint32_t offset;
    fs.read((char *)&offset, sizeof(offset));
    offset = FROM_FILE_ENDIANESS(offset);

    uint32_t length;
    fs.read((char *)&length, sizeof(length));
    length = FROM_FILE_ENDIANESS(length);

    std::vector<char> buffer(length);
    fs.seekg(offset, std::ios_base::beg);
    fs.read(&buffer[0], length);

    return parquet::FileMetaData::Make(&buffer[0], &length);
}

