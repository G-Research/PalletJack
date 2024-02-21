#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"

#include "palletjack.h"

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "parquet/exception.h"

#include "parquet_types_palletjack.h"

#include <iostream>
#include <fstream>

using arrow::Status;

#define TO_FILE_ENDIANESS(x) (x)
#define FROM_FILE_ENDIANESS(x) (x)
const int HEADER_V1_LENGTH = 4;
const char HEADER_V1[HEADER_V1_LENGTH] = {'P', 'J', '_', '2'};

struct DataHeader
{
    char header[HEADER_V1_LENGTH] = {'P', 'J', '_', '2'};
    uint32_t row_groups = 0;
    uint32_t columns = 0;
    uint32_t metadata_length = 0;
    uint32_t get_row_numbers_size() { return row_groups; }                               // rg
    uint32_t get_schema_offsets_size() { return 1 + 1 + columns + 1; }                   // 1 + 1 + c + 1
    uint32_t get_schema_num_children_offsets_size() { return (columns + 1) * (1 + 1); }  // (c + 1) * (1 + 1) 
    uint32_t get_row_groups_offsets_size() { return 1 + row_groups + 1; }                // 1 + rg + 1
    uint32_t get_column_orders_offsets_size() { return 1 + columns + 1; }                // 1 + c + 1
    uint32_t get_column_chunks_offsets_size() { return row_groups * (1 + columns + 1); } // rg * (1 + c + 1)
    uint32_t get_body_size()
    {
        return get_row_numbers_size() * sizeof(uint32_t) +
               get_schema_offsets_size() * sizeof(uint32_t) +
               get_schema_num_children_offsets_size() * sizeof(uint32_t) +
               get_row_groups_offsets_size() * sizeof(uint32_t) +
               get_column_orders_offsets_size() * sizeof(uint32_t) +
               get_column_chunks_offsets_size() * sizeof(uint32_t) +
               metadata_length;
    }
};

/* File format: (Thrift-encoded metadata stored separately for each row group)
-----------------------------
| 0 - 3 | PJ_2              | (char[4]) - File header in ASCI
|---------------------------|
| 4 - 7 | Row groups        | (uint32) - Number of row groups
|---------------------------|
| 8 - 11| Columns           | (uint32) - Number of columns
|---------------------------|
|12 - 15| Metadata length   | (uint32) - Metadata length
|---------------------------|
|16 -   | Row numbers       | (uint32*) - number of rows per row group
|---------------------------|
| . . . | Schema offsets    | (uint32*) - offsets of schema element (1 + 1 + c + 1)
|---------------------------|
| . . . | Row group offsets | (uint32*) - offsets of row grpups (1 + rg + 1)
|---------------------------|
| . . . | Chunks offsets    | (uint32*) - offsets of column chunks rg * (1 + c + 1)
|---------------------------|
| . . . | Metadata          | (uint8*) - Original metadata (thrift compact protocol)
-----------------------------
*/

constexpr int32_t kDefaultThriftStringSizeLimit = 100 * 1000 * 1000;
constexpr int32_t kDefaultThriftContainerSizeLimit = 1000 * 1000;

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

std::shared_ptr<ThriftBuffer> CreateReadOnlyMemoryBuffer(uint8_t *buf, uint32_t len)
{
#if PARQUET_THRIFT_VERSION_MAJOR > 0 || PARQUET_THRIFT_VERSION_MINOR >= 14
    auto conf = std::make_shared<apache::thrift::TConfiguration>();
    conf->setMaxMessageSize(std::numeric_limits<int>::max());
    return std::make_shared<ThriftBuffer>(buf, len, ThriftBuffer::OBSERVE, conf);
#else
    return std::make_shared<ThriftBuffer>(buf, len);
#endif
}

template <class T>
void DeserializeUnencryptedMessage(const uint8_t *buf, uint32_t *len,
                                   T *deserialized_msg)
{
    // Deserialize msg bytes into c++ thrift msg using memory transport.
    auto tmem_transport = CreateReadOnlyMemoryBuffer(const_cast<uint8_t *>(buf), *len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    // Protect against CPU and memory bombs
    tproto_factory.setStringSizeLimit(kDefaultThriftStringSizeLimit);
    tproto_factory.setContainerSizeLimit(kDefaultThriftContainerSizeLimit);
    auto tproto = tproto_factory.getProtocol(tmem_transport);
    try
    {
        deserialized_msg->read(tproto.get());
    }
    catch (std::exception &e)
    {
        std::stringstream ss;
        ss << "Couldn't deserialize thrift: " << e.what() << "\n";
        throw parquet::ParquetException(ss.str());
    }
    uint32_t bytes_left = tmem_transport->available_read();
    *len = *len - bytes_left;
}

palletjack::parquet::FileMetaData DeserializeFileMetadata(const void *buf, uint32_t len)
{
    palletjack::parquet::FileMetaData fileMetaData;
    DeserializeUnencryptedMessage((const uint8_t *)buf, &len, &fileMetaData);
    return fileMetaData;
}

size_t WriteListBegin(void *dst, const ::apache::thrift::protocol::TType elemType, uint32_t size)
{
    std::shared_ptr<ThriftBuffer> mem_buffer(new ThriftBuffer(16));
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    // Protect against CPU and memory bombs
    tproto_factory.setStringSizeLimit(kDefaultThriftStringSizeLimit);
    tproto_factory.setContainerSizeLimit(kDefaultThriftContainerSizeLimit);
    auto tproto = tproto_factory.getProtocol(mem_buffer);
    mem_buffer->resetBuffer();
    tproto->writeListBegin(elemType, static_cast<uint32_t>(size));
    uint8_t *ptr;
    uint32_t len;
    mem_buffer->getBuffer(&ptr, &len);
    memcpy(dst, ptr, len);
    return len;
}

size_t WriteI32(void *dst, uint32_t value)
{
    std::shared_ptr<ThriftBuffer> mem_buffer(new ThriftBuffer(16));
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    // Protect against CPU and memory bombs
    tproto_factory.setStringSizeLimit(kDefaultThriftStringSizeLimit);
    tproto_factory.setContainerSizeLimit(kDefaultThriftContainerSizeLimit);
    auto tproto = tproto_factory.getProtocol(mem_buffer);
    mem_buffer->resetBuffer();
    tproto->writeI32(value);
    uint8_t *ptr;
    uint32_t len;
    mem_buffer->getBuffer(&ptr, &len);
    memcpy(dst, ptr, len);
    return len;
}

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
    std::shared_ptr<arrow::Buffer> thrift_buffer;
    DataHeader data_header;

    {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(parquet_path)));
        auto metadata = parquet::ReadMetaData(infile);

        std::shared_ptr<arrow::io::BufferOutputStream> metadata_stream;
        PARQUET_ASSIGN_OR_THROW(metadata_stream, arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool()));
        metadata.get()->WriteTo(metadata_stream.get());
        PARQUET_ASSIGN_OR_THROW(thrift_buffer, metadata_stream.get()->Finish());
        data_header.row_groups = metadata->num_row_groups();
        data_header.columns = metadata->num_columns();
        data_header.metadata_length = thrift_buffer.get()->size();
    }

    auto metadata = DeserializeFileMetadata(thrift_buffer.get()->data(), thrift_buffer.get()->size());

    // Validate data
    {
        if (data_header.row_groups == 0)
            throw new std::logic_error("Number of row groups is not set!");
        if (data_header.columns == 0)
            throw new std::logic_error("Number of columns is not set!");
        if (data_header.metadata_length == 0)
            throw new std::logic_error("Number of metadata length is not set!");
        if (data_header.row_groups != metadata.row_numbers.size())
        {
            auto msg = std::string("Row numbers information is invalid ") + std::to_string(data_header.row_groups) + " != " + std::to_string(metadata.row_numbers.size()) + " !";

            throw new std::logic_error(msg);
        }

        if (data_header.get_schema_offsets_size() != metadata.schema_offsets.size())
        {
            auto msg = std::string("Schema offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", schema_offsets=" + std::to_string(metadata.schema_offsets.size()) + " !";

            throw new std::logic_error(msg);
        }

        for (auto &schema_elemnt : metadata.schema)
        {
            if (schema_elemnt.num_children_offsets.size() == 0)
            {
                schema_elemnt.num_children_offsets.push_back(0);
                schema_elemnt.num_children_offsets.push_back(0);
            }  
            else if (schema_elemnt.num_children_offsets.size() != 2)
            {
                auto msg = std::string("Num children offsets information is invalid, num_children_offsets=") + std::to_string(schema_elemnt.num_children_offsets.size()) + " !";

                throw new std::logic_error(msg);
            }            
        }
        
        if (data_header.get_row_groups_offsets_size() != metadata.row_groups_offsets.size())
        {
            auto msg = std::string("Row group offsets information is invalid, columns=") + std::to_string(data_header.row_groups) + ", row_groups_offsets=" + std::to_string(metadata.row_groups_offsets.size()) + " !";

            throw new std::logic_error(msg);
        }

        if (data_header.get_column_orders_offsets_size() != metadata.column_orders_offsets.size())
        {
            auto msg = std::string("Column orders offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", column_orders_offsets=" + std::to_string(metadata.column_orders_offsets.size()) + " !";

            throw new std::logic_error(msg);
        }

        for (const auto &row_group : metadata.row_groups)
        {
            if (data_header.get_column_chunks_offsets_size() / metadata.row_groups.size() != row_group.column_chunks_offsets.size())
            {
                auto msg = std::string("Column chunk offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", column_chunks_offsets=" + std::to_string(row_group.column_chunks_offsets.size()) + " !";

                throw new std::logic_error(msg);
            }
        }
    }

    {
        std::vector<char> buf(4 * 1024 * 1024); // 4 MiB
        std::ofstream fs(index_file_path, std::ios::out | std::ios::binary);
        fs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        fs.rdbuf()->pubsetbuf(&buf[0], buf.size());
        fs.write((const char *)&data_header, sizeof(data_header));
        fs.write((const char *)&metadata.row_numbers[0], sizeof(metadata.row_numbers[0]) * metadata.row_numbers.size());
        fs.write((const char *)&metadata.schema_offsets[0], sizeof(metadata.schema_offsets[0]) * metadata.schema_offsets.size());
        for (const auto &schema_element : metadata.schema)
        {
            fs.write((const char *)&schema_element.num_children_offsets[0], sizeof(schema_element.num_children_offsets[0]) * schema_element.num_children_offsets.size());
        }

        fs.write((const char *)&metadata.row_groups_offsets[0], sizeof(metadata.row_groups_offsets[0]) * metadata.row_groups_offsets.size());
        fs.write((const char *)&metadata.column_orders_offsets[0], sizeof(metadata.column_orders_offsets[0]) * metadata.column_orders_offsets.size());
        for (const auto &row_group : metadata.row_groups)
        {
            fs.write((const char *)&row_group.column_chunks_offsets[0], sizeof(row_group.column_chunks_offsets[0]) * row_group.column_chunks_offsets.size());
        }

        uint32_t offset = fs.tellp();
        std::cerr << " Writing thrift offset: " << offset << std::endl;

        fs.write((const char *)thrift_buffer.get()->data(), thrift_buffer.get()->size());
    }
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const char *index_file_path, const std::vector<uint32_t> &row_groups, const std::vector<uint32_t> &columns)
{
    std::vector<char> buf(4 * 1024 * 1024); // 4 MiB
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

    auto body_size = dataHeader.get_body_size();
    std::vector<uint8_t> data_body(body_size);
    std::vector<uint8_t> data_body_dst(dataHeader.metadata_length);
    fs.read((char *)&data_body[0], data_body.size());

    auto row_numbers = (uint32_t *)&data_body[0];
    auto schema_offsets = (uint32_t *)&row_numbers[dataHeader.get_row_numbers_size()];
    auto schema_num_children_offsets = (uint32_t *)&schema_offsets[dataHeader.get_schema_offsets_size()];
    auto row_groups_offsets = (uint32_t *)&schema_num_children_offsets[dataHeader.get_schema_num_children_offsets_size()];
    auto column_orders_offsets = (uint32_t *)&row_groups_offsets[dataHeader.get_row_groups_offsets_size()];
    auto column_chunks_offsets = (uint32_t *)&column_orders_offsets[dataHeader.get_column_orders_offsets_size()];
    auto src = (uint8_t *)&column_chunks_offsets[dataHeader.get_column_chunks_offsets_size()];
    auto dst = (uint8_t *)&data_body_dst[0];

    uint32_t index_src = 0;
    uint32_t index_dst = 0;
    size_t toCopy = 0;

    if (columns.size () > 0)
    {
        auto schema_list = &schema_offsets[0];
        toCopy = schema_list[0] - index_src;
        memcpy(&dst[index_dst], &src[index_src], toCopy);
        index_src += toCopy;
        index_dst += toCopy;

        index_dst += WriteListBegin(&dst[index_dst], ::apache::thrift::protocol::T_STRUCT, columns.size() + 1); // one extra element for root
        index_src = schema_list[1]; 

        auto root_schema_element = &schema_list[1];
        toCopy = root_schema_element[0] + schema_num_children_offsets[0] - index_src;
        memcpy(&dst[index_dst], &src[index_src], toCopy); 
        index_dst += toCopy;
        index_src = root_schema_element[0] + schema_num_children_offsets[1];

        // Update the num children in the schema
        index_dst += WriteI32(&dst[index_dst], columns.size());

        toCopy = root_schema_element[1] - index_src;
        memcpy(&dst[index_dst], &src[index_src], toCopy); 
        index_dst += toCopy;
        index_src += toCopy;
        
        auto schema_elements = &schema_offsets[2];
        for (auto column : columns)
        {
            toCopy = schema_elements[column + 1] - schema_elements[column];
            memcpy(&dst[index_dst], &src[schema_elements[column]], toCopy);
            index_dst += toCopy;
        }

        index_src = schema_elements[dataHeader.columns];
    }

    for (auto row_group = 0; row_group < dataHeader.row_groups; row_group++)
    {
        auto row_group_offset = row_groups_offsets[1 + row_group];
        auto chunks_list = &column_chunks_offsets[(1 + dataHeader.columns + 1) * row_group];
        auto chunks = &chunks_list[1];

        // START HERE
        toCopy = row_group_offset + chunks_list[0] - index_src;
        memcpy(&dst[index_dst], &src[index_src], toCopy);
        index_dst += toCopy;

        index_dst += WriteListBegin(&dst[index_dst], ::apache::thrift::protocol::T_STRUCT, columns.size());

        for (auto column_to_copy : columns)
        {
            toCopy = chunks[column_to_copy + 1] - chunks[column_to_copy];
            memcpy(&dst[index_dst], &src[row_group_offset + chunks[column_to_copy]], toCopy);
            index_dst += toCopy;
        }

        index_src = row_group_offset + chunks[dataHeader.columns];
    }

    if (columns.size () > 0)
    {
        auto column_orders_list = &column_orders_offsets[0];
        toCopy = column_orders_list[0] - index_src;
        memcpy(&dst[index_dst], &src[index_src], toCopy);
        index_src += toCopy;
        index_dst += toCopy;

        index_dst += WriteListBegin(&dst[index_dst], ::apache::thrift::protocol::T_STRUCT, columns.size()); // one extra element for root
        index_src = column_orders_list[1]; 

        auto column_orders = &column_orders_offsets[1];
        for (auto column : columns)
        {
            toCopy = column_orders[column + 1] - column_orders[column];
            memcpy(&dst[index_dst], &src[column_orders[column]], toCopy);
            index_dst += toCopy;
        }
        index_src = column_orders[dataHeader.columns];
    }
    
    // Copy leftovers
    toCopy = dataHeader.metadata_length - index_src;
    memcpy(&dst[index_dst], &src[index_src], toCopy);
    index_dst += toCopy;

    // TODO - remove this
    std::cerr << " Reading body_size: " << body_size << std::endl;
    std::cerr << " Reading thrift offset: " << src - &data_body[0] << std::endl;
    std::cerr << " Reading thrift length: " << dataHeader.metadata_length << std::endl;

    uint32_t length = index_dst;
    // DeserializeFileMetadata(&dst[0], length);
    return parquet::FileMetaData::Make(&dst[0], &length);
}