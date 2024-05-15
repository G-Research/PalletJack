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
#include <chrono>
#include <memory>

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
    uint32_t column_names_length = 0;
    uint32_t metadata_length = 0;

    uint32_t get_num_rows_offsets_size() const { return 2; }                                   // 2
    uint32_t get_row_numbers_size() const { return row_groups; }                               // rg
    uint32_t get_schema_offsets_size() const { return 1 + 1 + columns + 1; }                   // 1 + 1 + c + 1
    uint32_t get_schema_num_children_offsets_size() const { return (columns + 1) * (1 + 1); }  // (c + 1) * (1 + 1)
    uint32_t get_row_groups_offsets_size() const { return 1 + row_groups + 1; }                // 1 + rg + 1
    uint32_t get_column_orders_offsets_size() const { return 1 + columns + 1; }                // 1 + c + 1
    uint32_t get_column_chunks_offsets_size() const { return row_groups * (1 + columns + 1); } // rg * (1 + c + 1)
    uint32_t get_body_size() const
    {
        return get_num_rows_offsets_size() * sizeof(uint32_t) +
               get_row_numbers_size() * sizeof(uint32_t) +
               get_schema_offsets_size() * sizeof(uint32_t) +
               get_schema_num_children_offsets_size() * sizeof(uint32_t) +
               get_row_groups_offsets_size() * sizeof(uint32_t) +
               get_column_orders_offsets_size() * sizeof(uint32_t) +
               get_column_chunks_offsets_size() * sizeof(uint32_t) +
               column_names_length +
               metadata_length;
    }
};

/* File format: (Thrift-encoded metadata stored separately for each row group)
-----------------------------
| 0 ... | DataHeader        |
|---------------------------|
|       | 'PJ_2'            | (char[4]) - File header in ASCI
|       --------------------|
|       | row groups        | (uint32) - Number of row groups
|       --------------------|
|       | columns           | (uint32) - Number of columns
|       --------------------|
|       | col. names length | (uint32) - Length of column names section
|       --------------------|
|       | metadata length   | (uint32) - Length of metadata section
|---------------------------|
| . . . | column names      | ['col_0', '\0', 'col_1', '\0', ....] - Section with column names
|---------------------------|
| . . . | metadata          | [bytes] - Section with original metadata (thrift compact protocol)
-----------------------------
*/

constexpr int32_t kDefaultThriftStringSizeLimit = 100 * 1000 * 1000;
constexpr int32_t kDefaultThriftContainerSizeLimit = 1000 * 1000;

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

std::shared_ptr<ThriftBuffer> CreateReadOnlyMemoryBuffer(uint8_t *buf, uint32_t len)
{
    auto conf = std::make_shared<apache::thrift::TConfiguration>();
    conf->setMaxMessageSize(std::numeric_limits<int>::max());
    return std::make_shared<ThriftBuffer>(buf, len, ThriftBuffer::OBSERVE, conf);
}

void DeserializeUnencryptedMessage(const uint8_t *buf, uint32_t *len,
                                   palletjack::parquet::FileMetaData *deserialized_msg)
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

class ThriftCopier
{
    const uint8_t *src;
    const uint8_t *src_end;
    std::vector<uint8_t> dst_data;
    uint8_t *dst;
    uint8_t *dst_end;
    size_t dst_idx;
    std::shared_ptr<ThriftBuffer> mem_buffer;
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto;

    inline void CopyFrom(const uint8_t *src, size_t to_copy)
    {
        if (dst + dst_idx + to_copy > dst_end)
        {
            auto msg = std::string("No space left in the destination buffer, dst_idx=") + std::to_string(dst_idx) + ", to_copy=" + std::to_string(to_copy) + ", size=" + std::to_string(dst_end - dst);
            throw std::logic_error(msg);
        }

        memcpy(&dst_data[dst_idx], src, to_copy);
        dst_idx += to_copy;
    }

public:
    ThriftCopier(const uint8_t *src, size_t size) : src(src),
                                                    src_end(src + size),
                                                    dst_data(size),
                                                    dst(&dst_data[0]),
                                                    dst_end(dst + size),
                                                    dst_idx(0),
                                                    mem_buffer(new ThriftBuffer(16))
    {
        // Protect against CPU and memory bombs
        tproto_factory.setStringSizeLimit(kDefaultThriftStringSizeLimit);
        tproto_factory.setContainerSizeLimit(kDefaultThriftContainerSizeLimit);
        tproto = tproto_factory.getProtocol(mem_buffer);
    }

    inline void CopyFrom(size_t src_idx, size_t to_copy)
    {
        if (src + src_idx + to_copy > src_end)
        {
            auto msg = std::string("Requested reading outside source range, src_idx=") + std::to_string(src_idx) + ", to_copy=" + std::to_string(to_copy) + ", size=" + std::to_string(src_end - src);
            throw std::logic_error(msg);
        }

        CopyFrom(src + src_idx, to_copy);
    }

    void WriteListBegin(const ::apache::thrift::protocol::TType elemType, uint32_t size)
    {
        mem_buffer->resetBuffer();
        tproto->writeListBegin(elemType, static_cast<uint32_t>(size));
        uint8_t *ptr;
        uint32_t len;
        mem_buffer->getBuffer(&ptr, &len);
        CopyFrom(ptr, len);
    }

    void WriteI32(int32_t value)
    {
        mem_buffer->resetBuffer();
        tproto->writeI32(value);
        uint8_t *ptr;
        uint32_t len;
        mem_buffer->getBuffer(&ptr, &len);
        CopyFrom(ptr, len);
    }

    void WriteI64(int64_t value)
    {
        mem_buffer->resetBuffer();
        tproto->writeI64(value);
        uint8_t *ptr;
        uint32_t len;
        mem_buffer->getBuffer(&ptr, &len);
        CopyFrom(ptr, len);
    }

    size_t GetDataSize() { return dst_idx; }

    const uint8_t *GetData() { return &dst_data[0]; }
};

palletjack::parquet::FileMetaData DeserializeFileMetadata(const void *buf, uint32_t len)
{
    palletjack::parquet::FileMetaData fileMetaData;
    DeserializeUnencryptedMessage((const uint8_t *)buf, &len, &fileMetaData);
    return fileMetaData;
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

std::vector<char> GenerateMetadataIndex(const char *parquet_path)
{
    std::shared_ptr<arrow::Buffer> thrift_buffer;
    DataHeader data_header = {};

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

        for (uint32_t c = 0; c < data_header.columns; c++)
        {
            data_header.column_names_length += metadata.get()->schema()->Column(c)->name().length() + 1;
        }
    }

    auto metadata = DeserializeFileMetadata(thrift_buffer.get()->data(), thrift_buffer.get()->size());

    // Validate data
    {
        if (data_header.row_groups == 0)
            throw std::logic_error("Number of row groups is not set!");
        if (data_header.columns == 0)
            throw std::logic_error("Number of columns is not set!");
        if (data_header.metadata_length == 0)
            throw std::logic_error("Number of metadata length is not set!");

        if (data_header.get_num_rows_offsets_size() != metadata.num_rows_offsets.size())
        {
            auto msg = std::string("Number of rows offset information is invalid ") + std::to_string(data_header.get_num_rows_offsets_size()) + " != " + std::to_string(metadata.num_rows_offsets.size()) + " !";
            throw std::logic_error(msg);
        }

        if (data_header.row_groups != metadata.row_numbers.size())
        {
            auto msg = std::string("Row numbers information is invalid ") + std::to_string(data_header.row_groups) + " != " + std::to_string(metadata.row_numbers.size()) + " !";
            throw std::logic_error(msg);
        }

        if (data_header.get_schema_offsets_size() != metadata.schema_offsets.size())
        {
            auto msg = std::string("Schema offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", schema_offsets=" + std::to_string(metadata.schema_offsets.size()) + " !";
            throw std::logic_error(msg);
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

                throw std::logic_error(msg);
            }
        }

        if (data_header.get_row_groups_offsets_size() != metadata.row_groups_offsets.size())
        {
            auto msg = std::string("Row group offsets information is invalid, columns=") + std::to_string(data_header.row_groups) + ", row_groups_offsets=" + std::to_string(metadata.row_groups_offsets.size()) + " !";

            throw std::logic_error(msg);
        }

        // column_orders is optional
        if (metadata.column_orders_offsets.size() == 0)
        {
            metadata.column_orders_offsets.resize(data_header.get_column_orders_offsets_size());
        }

        if (data_header.get_column_orders_offsets_size() != metadata.column_orders_offsets.size())
        {
            auto msg = std::string("Column orders offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", column_orders_offsets=" + std::to_string(metadata.column_orders_offsets.size()) + " !";

            throw std::logic_error(msg);
        }

        for (const auto &row_group : metadata.row_groups)
        {
            if (data_header.get_column_chunks_offsets_size() / metadata.row_groups.size() != row_group.column_chunks_offsets.size())
            {
                auto msg = std::string("Column chunk offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", column_chunks_offsets=" + std::to_string(row_group.column_chunks_offsets.size()) + " !";

                throw std::logic_error(msg);
            }
        }
    }

    // Use ostringstream as a binary buffer
    std::ostringstream fs(std::ios::binary);

    fs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    fs.write((const char *)&data_header, sizeof(data_header));
    fs.write((const char *)&metadata.num_rows_offsets[0], sizeof(metadata.num_rows_offsets[0]) * metadata.num_rows_offsets.size());
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

    uint32_t written_column_names_length = 0;
    for (uint32_t c = 1; c <= data_header.columns; c++)
    {
        auto name = metadata.schema[c].name;
        auto cname = name.c_str();
        auto to_write = name.length() + 1;
        fs.write((const char *)cname, to_write);
        written_column_names_length += to_write;
    }

    if (data_header.column_names_length != written_column_names_length)
    {
        throw std::logic_error("Error when writign the index file, data_header.column_names_length != written_column_names_length !");
    }

    fs.write((const char *)thrift_buffer.get()->data(), thrift_buffer.get()->size());
    auto s = fs.str();
    if (sizeof(data_header) + data_header.get_body_size() != s.size())
    {
        auto msg = std::string("Error when writign the index file, exexted size=") + std::to_string(sizeof(data_header) + data_header.get_body_size()) + ", actual size=" + std::to_string(s.size()) + " !";
        throw std::logic_error(msg);
    }

    std::vector<char> v(s.size());
    memcpy(&v[0], s.data(), s.size());
    return v;
}

void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path)
{
    std::vector<char> buf(4 * 1024 * 1024); // 4 MiB
    std::ofstream fs(index_file_path, std::ios::out | std::ios::binary);
    fs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    fs.rdbuf()->pubsetbuf(&buf[0], buf.size());
    auto v = std::move(GenerateMetadataIndex(parquet_path));
    fs.write(v.data(), v.size());
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const DataHeader &dataHeader,
                                                    const uint8_t *data_body,
                                                    size_t body_size,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names)
{
    if (memcmp(HEADER_V1, dataHeader.header, HEADER_V1_LENGTH) != 0)
    {
        auto msg = std::string("Index file has unexpected format!");
        throw std::logic_error(msg);
    }

    if (row_groups.size() > 0)
    {
        for (auto row_group : row_groups)
        {
            if (row_group >= dataHeader.row_groups)
            {
                auto msg = std::string("Requested row_group=") + std::to_string(row_group) + ", but only 0-" + std::to_string(dataHeader.row_groups - 1) + " are available!";
                throw std::logic_error(msg);
            }
        }
    }

    if (column_indices.size() > 0)
    {
        if (column_names.size() > 0)
        {
            auto msg = std::string("Cannot specify both column indices and column names at the same time!");
            throw std::logic_error(msg);
        }

        for (auto column : column_indices)
        {
            if (column >= dataHeader.columns)
            {
                auto msg = std::string("Requested column=") + std::to_string(column) + ", but only 0-" + std::to_string(dataHeader.columns - 1) + " are available!";
                throw std::logic_error(msg);
            }
        }
    }

    auto num_row_offsets = (uint32_t *)&data_body[0];
    auto row_numbers = (uint32_t *)&num_row_offsets[dataHeader.get_num_rows_offsets_size()];
    auto schema_offsets = (uint32_t *)&row_numbers[dataHeader.get_row_numbers_size()];
    auto schema_num_children_offsets = (uint32_t *)&schema_offsets[dataHeader.get_schema_offsets_size()];
    auto row_groups_offsets = (uint32_t *)&schema_num_children_offsets[dataHeader.get_schema_num_children_offsets_size()];
    auto column_orders_offsets = (uint32_t *)&row_groups_offsets[dataHeader.get_row_groups_offsets_size()];
    auto column_chunks_offsets = (uint32_t *)&column_orders_offsets[dataHeader.get_column_orders_offsets_size()];
    auto column_names_ptr = (uint8_t *)&column_chunks_offsets[dataHeader.get_column_chunks_offsets_size()];
    auto src = (uint8_t *)&column_names_ptr[dataHeader.column_names_length];
    ThriftCopier thriftCopier(src, dataHeader.metadata_length);

    uint32_t index_src = 0;
    size_t toCopy = 0;

    std::vector<uint32_t> columns = column_indices;
    if (column_names.size() > 0)
    {
        columns.reserve(column_names.size());

        std::unordered_map<std::string, uint32_t> columns_map;
        for (uint32_t c = 0; c < dataHeader.columns; c++)
        {
            std::string s = (const char *)column_names_ptr;
            column_names_ptr += s.length() + 1;
            columns_map[s] = c;
        }

        if (column_names_ptr != src)
        {
            auto msg = std::string("Internal error, when reading column names!");
            throw std::logic_error(msg);
        }

        for (const auto &column_name : column_names)
        {
            auto kvp = columns_map.find(column_name);
            if (kvp == columns_map.end())
            {
                auto msg = std::string("Couldn't find a column with a name '") + column_name + "'!";
                throw std::logic_error(msg);
            }

            columns.emplace_back(kvp->second);
        }
    }

    if (columns.size() > 0)
    {
        //> 2:required list<SchemaElement> schema;
        auto schema_list = &schema_offsets[0];
        toCopy = schema_list[0] - index_src;
        thriftCopier.CopyFrom(index_src, toCopy);
        index_src += toCopy;

        thriftCopier.WriteListBegin(::apache::thrift::protocol::T_STRUCT, columns.size() + 1); // one extra schema element for root
        index_src = schema_list[1]; // skip the list header and jump to the first schema element (which is the root element)

        auto root_schema_element = &schema_list[1];
        toCopy = root_schema_element[0] + schema_num_children_offsets[0] - index_src;
        thriftCopier.CopyFrom(index_src, toCopy);

        // Write updated num children in the root element
        //> 5: optional i32 num_children; 
        thriftCopier.WriteI32(columns.size());
        index_src = root_schema_element[0] + schema_num_children_offsets[1];
        toCopy = root_schema_element[1] - index_src;
        thriftCopier.CopyFrom(index_src, toCopy);
        index_src += toCopy;

        auto schema_elements = &schema_offsets[2];
        for (auto column : columns)
        {
            toCopy = schema_elements[column + 1] - schema_elements[column];
            thriftCopier.CopyFrom(schema_elements[column], toCopy);
        }

        index_src = schema_elements[dataHeader.columns];
    }

    if (row_groups.size() > 0)
    {
        //> 3: required i64 num_rows
        int64_t num_rows = 0;
        for (auto row_group : row_groups)
        {
            num_rows += row_numbers[row_group];
        }

        toCopy = num_row_offsets[0] - index_src;
        thriftCopier.CopyFrom(index_src, toCopy);
        index_src += toCopy;

        thriftCopier.WriteI64(num_rows);
        index_src = num_row_offsets[1];
    }

    auto row_group_filtering = row_groups.size() > 0;
    if (row_group_filtering)
    {
        //> 4: required list<RowGroup> row_groups
        auto row_groups_list = &row_groups_offsets[0];
        toCopy = row_groups_list[0] - index_src;
        thriftCopier.CopyFrom(index_src, toCopy);
        index_src += toCopy;

        thriftCopier.WriteListBegin(::apache::thrift::protocol::T_STRUCT, row_groups.size());
        index_src = row_groups_list[1];
    }
    else
    {
        // Copy to here, including the list header
        auto row_groups_list = &row_groups_offsets[0];
        toCopy = row_groups_list[1] - index_src;
        thriftCopier.CopyFrom(index_src, toCopy);
        index_src += toCopy;
    }

    for (auto idx = 0u;; idx++)
    {
        size_t row_group_idx = 0;

        if (row_group_filtering)
        {
            if (idx >= row_groups.size())
                break;

            row_group_idx = row_groups[idx];
        }
        else
        {
            if (idx >= dataHeader.row_groups)
                break;

            row_group_idx = idx;
        }

        auto row_group_offset = row_groups_offsets[1 + row_group_idx];
        index_src = row_groups_offsets[1 + row_group_idx];
        if (columns.size() > 0)
        {
            //> 1: required list<ColumnChunk> columns
            auto chunks_list = &column_chunks_offsets[(1 + dataHeader.columns + 1) * row_group_idx];
            auto chunks = &chunks_list[1];
            toCopy = row_group_offset + chunks_list[0] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            thriftCopier.WriteListBegin(::apache::thrift::protocol::T_STRUCT, columns.size());

            for (auto column_to_copy : columns)
            {
                toCopy = chunks[column_to_copy + 1] - chunks[column_to_copy];
                thriftCopier.CopyFrom(row_group_offset + chunks[column_to_copy], toCopy);
            }

            index_src = row_group_offset + chunks[dataHeader.columns];
            toCopy = row_groups_offsets[1 + row_group_idx + 1] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;
        }
        else
        {
            toCopy = row_groups_offsets[1 + row_group_idx + 1] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;
        }
    }

    index_src = row_groups_offsets[1 + dataHeader.row_groups];

    if (columns.size() > 0)
    {
        //> 7: optional list<ColumnOrder> column_orders;
        if (column_orders_offsets[0] != 0)
        {
            auto column_orders_list = &column_orders_offsets[0];
            toCopy = column_orders_list[0] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;

            thriftCopier.WriteListBegin(::apache::thrift::protocol::T_STRUCT, columns.size()); // one extra element for root
            index_src = column_orders_list[1];

            auto column_orders = &column_orders_offsets[1];
            for (auto column : columns)
            {
                toCopy = column_orders[column + 1] - column_orders[column];
                thriftCopier.CopyFrom(column_orders[column], toCopy);
            }
            index_src = column_orders[dataHeader.columns];
        }
    }

    // Copy leftovers
    toCopy = dataHeader.metadata_length - index_src;
    thriftCopier.CopyFrom(index_src, toCopy);

#ifdef DEBUG
    std::cerr << " Reading body_size: " << body_size << std::endl;
    std::cerr << " Reading thrift offset: " << src - &data_body[0] << std::endl;
    std::cerr << " Reading thrift length: " << dataHeader.metadata_length << std::endl;
#endif

    uint32_t length = thriftCopier.GetDataSize();
    return parquet::FileMetaData::Make(thriftCopier.GetData(), &length);
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const char *index_file_path,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names)
{
    auto f = std::unique_ptr<FILE, decltype(&fclose)>(fopen(index_file_path, "rb"), &fclose);
    if (!f)
    {
        auto msg = std::string("I/O error when opening '") + index_file_path + "'";
        throw std::logic_error(msg);
    }

    DataHeader dataHeader;
    size_t read_bytes = fread(&dataHeader, 1, sizeof(dataHeader), f.get());
    if (read_bytes != sizeof(dataHeader))
    {
        auto msg = std::string("I/O error when reading '") + index_file_path + "'";
        throw std::logic_error(msg);
    }

    if (memcmp(HEADER_V1, dataHeader.header, HEADER_V1_LENGTH) != 0)
    {
        auto msg = std::string("File '") + index_file_path + "' has unexpected format!";
        throw std::logic_error(msg);
    }

    auto body_size = dataHeader.get_body_size();
    std::vector<uint8_t> data_body(body_size);

    read_bytes = fread(&data_body[0], 1, data_body.size(), f.get());
    if (read_bytes != data_body.size())
    {
        auto msg = std::string("I/O error when reading '") + index_file_path + "'";
        throw std::logic_error(msg);
    }

    return ReadMetadata(dataHeader, &data_body[0], data_body.size(), row_groups, column_indices, column_names);
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const unsigned char *index_data,
                                                    size_t index_data_length,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names)
{
    if (index_data_length < sizeof(DataHeader))
    {
        auto msg = std::string("Index data is too small, length=") + std::to_string(index_data_length);
        throw std::logic_error(msg);
    }

    const DataHeader *p_data_header = (const DataHeader *)index_data;
    size_t expected_length = sizeof(DataHeader) + p_data_header->get_body_size();
    if (index_data_length != expected_length)
    {
        auto msg = std::string("Index data has unexpected length, length=") + std::to_string(index_data_length) + ", expected=" + std::to_string(expected_length);
        throw std::logic_error(msg);
    }

    return ReadMetadata(*p_data_header, &index_data[sizeof(DataHeader)], index_data_length - sizeof(DataHeader), row_groups, column_indices, column_names);
}