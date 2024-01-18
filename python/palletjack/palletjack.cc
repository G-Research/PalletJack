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
const int HEADER_V1_LENGTH = 4;
const char HEADER_V1[HEADER_V1_LENGTH] = {'P', 'J', '_', '1'};

struct DataHeader {
  char header[HEADER_V1_LENGTH];
  uint32_t rowGroups;
};

struct DataItem {
  uint32_t offset;
  uint32_t length;
};

/* File format: (Thrift-encoded metadata stored separately for each row group)
--------------------------
| 0 - 3 | PJ_1           | File header in ASCI
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
/*  Notes (https://en.cppreference.com/w/cpp/io/basic_filebuf/setbuf):
    
    The conditions when this function may be used and the way in which the provided buffer is used is implementation-defined.

    GCC 4.6 libstdc++
    setbuf() may only be called when the std::basic_filebuf is not associated with a file (has no effect otherwise). With a user-provided buffer, reading from file reads n-1 bytes at a time.

    Clang++3.0 libc++
    setbuf() may be called after opening the file, but before any I/O (may crash otherwise). With a user-provided buffer, reading from file reads largest multiples of 4096 that fit in the buffer.

    Visual Studio 2010
    setbuf() may be called at any time, even after some I/O took place. Current contents of the buffer, if any, are lost.
    The standard does not define any behavior for this function except that setbuf(0, 0) called before any I/O has taken place is required to set unbuffered output.
    */

void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(parquet_path)));
    auto metadata = parquet::ReadMetaData(infile);
    uint32_t row_groups = (metadata->num_row_groups());

    std::vector<char> buf(4 * 1024 * 1024);  // 4 MiB
    std::ofstream fs(index_file_path, std::ios::out | std::ios::binary);    
    fs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    fs.rdbuf()->pubsetbuf(&buf[0], buf.size());

    fs.write(&HEADER_V1[0], HEADER_V1_LENGTH);
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

std::shared_ptr<parquet::FileMetaData> ReadRowGroupsMetadata(const char *index_file_path, const std::vector<uint32_t>& row_groups)
{
    std::vector<char> buf(4 * 1024 * 1024);  // 4 MiB
    std::ifstream fs(index_file_path, std::ios::binary);
    fs.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    fs.rdbuf()->pubsetbuf(&buf[0], buf.size());

    DataHeader dataHeader;
    fs.read((char *)&dataHeader, sizeof(dataHeader));

    if (memcmp(HEADER_V1, dataHeader.header, HEADER_V1_LENGTH) != 0)
    {
        auto msg = std::string("File '") + index_file_path + "' has unexpected format!";
        throw std::logic_error(msg);
    }
    
    uint32_t max_row_groups = FROM_FILE_ENDIANESS(dataHeader.rowGroups);
    std::vector<DataItem> dataItems (max_row_groups);
    fs.read((char *)&dataItems[0], sizeof(DataItem) * max_row_groups);
    
    std::shared_ptr<parquet::FileMetaData> result = nullptr;
    for(auto row_group : row_groups)
    {
        if (row_group >= max_row_groups)
        {
            auto msg = std::string("Requested row_group=") + std::to_string(row_group) + ", but only 0-" + std::to_string(max_row_groups-1) + " are available!";
            throw std::logic_error(msg);
        }
        
        uint32_t offset = FROM_FILE_ENDIANESS(dataItems[row_group].offset);
        uint32_t length = FROM_FILE_ENDIANESS(dataItems[row_group].length);
        std::vector<char> buffer(length);
        fs.seekg(offset, std::ios_base::beg);
        fs.read(&buffer[0], length);

        std::shared_ptr<parquet::FileMetaData> metadata = parquet::FileMetaData::Make(&buffer[0], &length);
        if (!result)
        {
            result = metadata;
        }
        else
        {
            result->AppendRowGroups(*metadata);
        }
    }

    return result;
}