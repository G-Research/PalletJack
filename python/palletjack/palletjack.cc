#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/encryption/encryption.h"

#include "palletjack.h"

#include "internal_file_decryptor.h"

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "parquet/exception.h"

#include "parquet_types_palletjack.h"

#include <iostream>
#include <chrono>
#include <memory>

using arrow::Status;

#define TO_FILE_ENDIANESS(x) (x)
#define FROM_FILE_ENDIANESS(x) (x)
const int HEADER_LENGTH = 4;
const char HEADER_PJ2[HEADER_LENGTH] = {'P', 'J', '_', '2'};
const char HEADER_PJ3[HEADER_LENGTH] = {'P', 'J', '_', '3'};

struct DataHeaderV2
{
    char header[HEADER_LENGTH] = {'P', 'J', '_', '2'};
    uint32_t row_groups = 0;
    uint32_t columns = 0;
    uint32_t column_names_length = 0;
    uint32_t metadata_length = 0;

    uint32_t get_num_rows_offsets_size() const { return 2; }
    uint32_t get_row_numbers_size() const { return row_groups; }
    uint32_t get_schema_offsets_size() const { return 1 + 1 + columns + 1; }
    uint32_t get_schema_num_children_offsets_size() const { return (columns + 1) * (1 + 1); }
    uint32_t get_row_groups_offsets_size() const { return 1 + row_groups + 1; }
    uint32_t get_column_orders_offsets_size() const { return 1 + columns + 1; }
    uint32_t get_column_chunks_offsets_size() const { return row_groups * (1 + columns + 1); }
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

struct DataHeaderV3
{
    char header[HEADER_LENGTH] = {'P', 'J', '_', '3'};
    uint32_t row_groups = 0;
    uint32_t columns = 0;
    uint32_t column_names_length = 0;
    uint32_t metadata_length = 0;
    uint32_t encrypted_metadata_length = 0;
    uint32_t crypto_metadata_length = 0;

    uint32_t get_num_rows_offsets_size() const { return 2; }
    uint32_t get_row_numbers_size() const { return row_groups; }
    uint32_t get_schema_offsets_size() const { return 1 + 1 + columns + 1; }
    uint32_t get_schema_num_children_offsets_size() const { return (columns + 1) * (1 + 1); }
    uint32_t get_row_groups_offsets_size() const { return 1 + row_groups + 1; }
    uint32_t get_column_orders_offsets_size() const { return 1 + columns + 1; }
    uint32_t get_column_chunks_offsets_size() const { return row_groups * (1 + columns + 1); }
    uint32_t get_offsets_size() const
    {
        return get_num_rows_offsets_size() * sizeof(uint32_t) +
               get_row_numbers_size() * sizeof(uint32_t) +
               get_schema_offsets_size() * sizeof(uint32_t) +
               get_schema_num_children_offsets_size() * sizeof(uint32_t) +
               get_row_groups_offsets_size() * sizeof(uint32_t) +
               get_column_orders_offsets_size() * sizeof(uint32_t) +
               get_column_chunks_offsets_size() * sizeof(uint32_t);
    }
    uint32_t get_body_size() const
    {
        uint32_t size = get_offsets_size() + column_names_length +
                        encrypted_metadata_length + crypto_metadata_length;
        // For encrypted footer files, metadata_length holds the decrypted size
        // (used for offset reference) but the decrypted bytes are not stored.
        if (encrypted_metadata_length == 0)
            size += metadata_length;
        return size;
    }
};

/* PJ_2 file format:
+----------------------------+
| DataHeaderV2                 |
|   'PJ_2'            (4B)  |
|   row_groups       (u32)  |
|   columns          (u32)  |
|   col_names_length (u32)  |
|   metadata_length  (u32)  |
+----------------------------+
| Offset arrays      (u32[]) |
+----------------------------+
| Column names  (NUL-term)  |
+----------------------------+
| Metadata (Thrift compact) |
+----------------------------+

PJ_3 file format (encryption support):
+----------------------------+
| DataHeaderV3               |
|   'PJ_3'            (4B)  |
|   row_groups       (u32)  |
|   columns          (u32)  |
|   col_names_length (u32)  |
|   metadata_length  (u32)  |
|   enc_meta_length  (u32)  |
|   crypto_meta_len  (u32)  |
+----------------------------+
| Offset arrays      (u32[]) |
+----------------------------+
| Column names  (NUL-term)  |
+----------------------------+
| Metadata (Thrift compact) |
|   For non-encrypted or     |
|   plaintext-footer files:  |
|     plaintext Thrift bytes |
|   For encrypted-footer:    |
|     plaintext Thrift bytes |
|     (offsets reference this)|
+----------------------------+
| Encrypted metadata (bytes) |
|   (only for encrypted       |
|    footer files; 0 if none) |
+----------------------------+
| FileCryptoMetaData (bytes) |
|   (only for encrypted       |
|    footer files; 0 if none) |
+----------------------------+
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
    std::shared_ptr<arrow::ResizableBuffer> dst_buffer;
    size_t dst_idx;
    std::shared_ptr<ThriftBuffer> mem_buffer;
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto;

    inline void EnsureCapacity(size_t needed)
    {
        if (dst_idx + needed > static_cast<size_t>(dst_buffer->size()))
        {
            auto new_size = std::max(static_cast<size_t>(dst_buffer->size()) * 2, dst_idx + needed);
            PARQUET_THROW_NOT_OK(dst_buffer->Resize(new_size, false));
        }
    }

public:
    inline void CopyFrom(const uint8_t *src, size_t to_copy)
    {
        EnsureCapacity(to_copy);
        memcpy(dst_buffer->mutable_data() + dst_idx, src, to_copy);
        dst_idx += to_copy;
    }

    ThriftCopier(const uint8_t *src, size_t size) : src(src),
                                                    src_end(src + size),
                                                    dst_idx(0),
                                                    mem_buffer(new ThriftBuffer(16))
    {
        PARQUET_ASSIGN_OR_THROW(dst_buffer, arrow::AllocateResizableBuffer(size));
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

    void WriteThrift(const ::apache::thrift::TBase &obj)
    {
        mem_buffer->resetBuffer();
        obj.write(tproto.get());
        uint8_t *ptr;
        uint32_t len;
        mem_buffer->getBuffer(&ptr, &len);
        CopyFrom(ptr, len);
    }

    size_t GetDataSize() { return dst_idx; }

    const uint8_t *GetData() { return dst_buffer->data(); }
};

palletjack::parquet::FileMetaData DeserializeFileMetadata(const void *buf, uint32_t len)
{
    palletjack::parquet::FileMetaData fileMetaData;
    DeserializeUnencryptedMessage((const uint8_t *)buf, &len, &fileMetaData);
    return fileMetaData;
}

std::shared_ptr<arrow::Buffer> ReadRawFooter(
    const std::shared_ptr<arrow::io::RandomAccessFile> &infile,
    bool &is_encrypted_footer)
{
    int64_t file_size;
    PARQUET_ASSIGN_OR_THROW(file_size, infile->GetSize());

    constexpr int64_t kFooterSize = 8;
    if (file_size < kFooterSize)
    {
        throw std::logic_error("File is too small to be a valid Parquet file");
    }

    std::shared_ptr<arrow::Buffer> footer_buf;
    PARQUET_ASSIGN_OR_THROW(footer_buf, infile->ReadAt(file_size - kFooterSize, kFooterSize));

    const uint8_t *footer_data = footer_buf->data();
    is_encrypted_footer = (memcmp(footer_data + 4, parquet::kParquetEMagic, 4) == 0);
    bool is_parquet = is_encrypted_footer || (memcmp(footer_data + 4, parquet::kParquetMagic, 4) == 0);
    if (!is_parquet)
    {
        throw std::logic_error("Not a valid Parquet file (magic bytes not found)");
    }

    uint32_t metadata_len;
    memcpy(&metadata_len, footer_data, sizeof(uint32_t));

    int64_t metadata_start = file_size - kFooterSize - metadata_len;
    if (metadata_start < 0)
    {
        throw std::logic_error("Invalid Parquet footer: metadata length exceeds file size");
    }

    std::shared_ptr<arrow::Buffer> raw_footer;
    PARQUET_ASSIGN_OR_THROW(raw_footer, infile->ReadAt(metadata_start, metadata_len));
    return raw_footer;
}

std::shared_ptr<arrow::Buffer> GenerateMetadataIndex(const char *parquet_path,
                                                      std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties)
{
    std::shared_ptr<arrow::Buffer> thrift_buffer;
    std::shared_ptr<arrow::Buffer> encrypted_metadata_buf;
    std::shared_ptr<arrow::Buffer> crypto_metadata_buf;
    bool is_encrypted_footer = false;
    uint32_t num_row_groups = 0;
    uint32_t num_columns = 0;
    uint32_t column_names_length = 0;

    // Column names extracted from schema
    std::vector<std::string> col_names;

    {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(parquet_path)));

        auto raw_footer = ReadRawFooter(infile, is_encrypted_footer);

        if (is_encrypted_footer)
        {
            if (!decryption_properties)
            {
                throw std::logic_error(
                    "File has an encrypted footer (PARE). "
                    "Decryption properties with the footer key must be provided.");
            }

            // Parse FileCryptoMetaData from the start of raw_footer to get its length.
            uint32_t crypto_metadata_len = raw_footer->size();
            auto file_crypto_metadata = parquet::FileCryptoMetaData::Make(
                raw_footer->data(), &crypto_metadata_len);

            uint32_t encrypted_metadata_len = raw_footer->size() - crypto_metadata_len;

            PARQUET_ASSIGN_OR_THROW(crypto_metadata_buf, raw_footer->CopySlice(0, crypto_metadata_len));
            PARQUET_ASSIGN_OR_THROW(encrypted_metadata_buf, raw_footer->CopySlice(crypto_metadata_len, encrypted_metadata_len));

            // Use Arrow's reader to decrypt and get structured metadata for offset computation.
            parquet::ReaderProperties reader_props;
            reader_props.file_decryption_properties(decryption_properties);
            auto reader = parquet::ParquetFileReader::Open(infile, reader_props);
            auto metadata = reader->metadata();

            // Serialize the decrypted metadata to plaintext Thrift for offset computation.
            // For encrypted-footer files, encryption_algorithm is NOT set in the footer
            // (it is in FileCryptoMetaData), so WriteTo uses the plain serialization path.
            std::shared_ptr<arrow::io::BufferOutputStream> metadata_stream;
            PARQUET_ASSIGN_OR_THROW(metadata_stream, arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool()));
            metadata->WriteTo(metadata_stream.get());
            PARQUET_ASSIGN_OR_THROW(thrift_buffer, metadata_stream->Finish());

            num_row_groups = metadata->num_row_groups();
            num_columns = metadata->num_columns();
            for (uint32_t c = 0; c < num_columns; c++)
            {
                auto name = metadata->schema()->Column(c)->name();
                column_names_length += name.length() + 1;
                col_names.push_back(name);
            }
        }
        else
        {
            // PAR1: plaintext footer (possibly with encrypted column metadata).
            // Read the raw Thrift bytes directly. DeserializeFileMetadata adjusts
            // thrift_len to exclude any trailing footer signature bytes.
            uint32_t thrift_len = raw_footer->size();
            auto temp_metadata = DeserializeFileMetadata(raw_footer->data(), thrift_len);
            (void)temp_metadata;

            PARQUET_ASSIGN_OR_THROW(thrift_buffer, raw_footer->CopySlice(0, thrift_len));

            // Use Arrow to get structured metadata for row_groups/columns/names.
            auto metadata = parquet::ReadMetaData(infile);
            num_row_groups = metadata->num_row_groups();
            num_columns = metadata->num_columns();
            for (uint32_t c = 0; c < num_columns; c++)
            {
                auto name = metadata->schema()->Column(c)->name();
                column_names_length += name.length() + 1;
                col_names.push_back(name);
            }
        }
    }

    auto pj_metadata = DeserializeFileMetadata(thrift_buffer->data(), thrift_buffer->size());

    // Build the appropriate header
    DataHeaderV3 data_header = {};
    data_header.row_groups = num_row_groups;
    data_header.columns = num_columns;
    data_header.column_names_length = column_names_length;
    data_header.metadata_length = thrift_buffer->size();
    data_header.encrypted_metadata_length = encrypted_metadata_buf ? encrypted_metadata_buf->size() : 0;
    data_header.crypto_metadata_length = crypto_metadata_buf ? crypto_metadata_buf->size() : 0;


    // Validate offsets
    {
        if (data_header.row_groups == 0)
            throw std::logic_error("Number of row groups is not set!");
        if (data_header.columns == 0)
            throw std::logic_error("Number of columns is not set!");
        if (data_header.metadata_length == 0)
            throw std::logic_error("Metadata length is not set!");

        if (data_header.get_num_rows_offsets_size() != pj_metadata.num_rows_offsets.size())
        {
            auto msg = std::string("Number of rows offset information is invalid, ") + std::to_string(data_header.get_num_rows_offsets_size()) + " != " + std::to_string(pj_metadata.num_rows_offsets.size()) + " !";
            throw std::logic_error(msg);
        }

        if (data_header.row_groups != pj_metadata.row_numbers.size())
        {
            auto msg = std::string("Row numbers information is invalid, ") + std::to_string(data_header.row_groups) + " != " + std::to_string(pj_metadata.row_numbers.size()) + " !";
            throw std::logic_error(msg);
        }

        if (data_header.get_schema_offsets_size() != pj_metadata.schema_offsets.size())
        {
            auto msg = std::string("Schema offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", schema_offsets=" + std::to_string(pj_metadata.schema_offsets.size()) + " !";
            throw std::logic_error(msg);
        }

        for (auto &schema_element : pj_metadata.schema)
        {
            if (schema_element.num_children_offsets.size() == 0)
            {
                schema_element.num_children_offsets.push_back(0);
                schema_element.num_children_offsets.push_back(0);
            }
            else if (schema_element.num_children_offsets.size() != 2)
            {
                auto msg = std::string("Num children offsets information is invalid, num_children_offsets=") + std::to_string(schema_element.num_children_offsets.size()) + " !";
                throw std::logic_error(msg);
            }
        }

        if (data_header.get_row_groups_offsets_size() != pj_metadata.row_groups_offsets.size())
        {
            auto msg = std::string("Row group offsets information is invalid, columns=") + std::to_string(data_header.row_groups) + ", row_groups_offsets=" + std::to_string(pj_metadata.row_groups_offsets.size()) + " !";
            throw std::logic_error(msg);
        }

        if (pj_metadata.column_orders_offsets.size() == 0)
        {
            pj_metadata.column_orders_offsets.resize(data_header.get_column_orders_offsets_size());
        }

        if (data_header.get_column_orders_offsets_size() != pj_metadata.column_orders_offsets.size())
        {
            auto msg = std::string("Column orders offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", column_orders_offsets=" + std::to_string(pj_metadata.column_orders_offsets.size()) + " !";
            throw std::logic_error(msg);
        }

        for (const auto &row_group : pj_metadata.row_groups)
        {
            if (data_header.get_column_chunks_offsets_size() / pj_metadata.row_groups.size() != row_group.column_chunks_offsets.size())
            {
                auto msg = std::string("Column chunk offsets information is invalid, columns=") + std::to_string(data_header.columns) + ", column_chunks_offsets=" + std::to_string(row_group.column_chunks_offsets.size()) + " !";
                throw std::logic_error(msg);
            }
        }
    }

    // Compute total size and write the index
    size_t header_size = sizeof(DataHeaderV3);
    auto total_size = header_size + data_header.get_body_size();
    std::shared_ptr<arrow::io::BufferOutputStream> fs;
    PARQUET_ASSIGN_OR_THROW(fs, arrow::io::BufferOutputStream::Create(total_size, arrow::default_memory_pool()));

    PARQUET_THROW_NOT_OK(fs->Write(&data_header, header_size));
    PARQUET_THROW_NOT_OK(fs->Write(&pj_metadata.num_rows_offsets[0], sizeof(pj_metadata.num_rows_offsets[0]) * pj_metadata.num_rows_offsets.size()));
    PARQUET_THROW_NOT_OK(fs->Write(&pj_metadata.row_numbers[0], sizeof(pj_metadata.row_numbers[0]) * pj_metadata.row_numbers.size()));
    PARQUET_THROW_NOT_OK(fs->Write(&pj_metadata.schema_offsets[0], sizeof(pj_metadata.schema_offsets[0]) * pj_metadata.schema_offsets.size()));
    for (const auto &schema_element : pj_metadata.schema)
    {
        PARQUET_THROW_NOT_OK(fs->Write(&schema_element.num_children_offsets[0], sizeof(schema_element.num_children_offsets[0]) * schema_element.num_children_offsets.size()));
    }

    PARQUET_THROW_NOT_OK(fs->Write(&pj_metadata.row_groups_offsets[0], sizeof(pj_metadata.row_groups_offsets[0]) * pj_metadata.row_groups_offsets.size()));
    PARQUET_THROW_NOT_OK(fs->Write(&pj_metadata.column_orders_offsets[0], sizeof(pj_metadata.column_orders_offsets[0]) * pj_metadata.column_orders_offsets.size()));
    for (const auto &row_group : pj_metadata.row_groups)
    {
        PARQUET_THROW_NOT_OK(fs->Write(&row_group.column_chunks_offsets[0], sizeof(row_group.column_chunks_offsets[0]) * row_group.column_chunks_offsets.size()));
    }

    uint32_t written_column_names_length = 0;
    for (const auto &name : col_names)
    {
        auto to_write = name.length() + 1;
        PARQUET_THROW_NOT_OK(fs->Write(name.c_str(), to_write));
        written_column_names_length += to_write;
    }

    if (data_header.column_names_length != written_column_names_length)
    {
        throw std::logic_error("Error when writing the index file, data_header.column_names_length != written_column_names_length !");
    }

    if (!is_encrypted_footer)
    {
        PARQUET_THROW_NOT_OK(fs->Write(thrift_buffer->data(), thrift_buffer->size()));
    }

    if (encrypted_metadata_buf)
    {
        PARQUET_THROW_NOT_OK(fs->Write(encrypted_metadata_buf->data(), encrypted_metadata_buf->size()));
    }
    if (crypto_metadata_buf)
    {
        PARQUET_THROW_NOT_OK(fs->Write(crypto_metadata_buf->data(), crypto_metadata_buf->size()));
    }

    std::shared_ptr<arrow::Buffer> result;
    PARQUET_ASSIGN_OR_THROW(result, fs->Finish());
    if (total_size != static_cast<size_t>(result->size()))
    {
        auto msg = std::string("Error when writing the index file, expected size=") + std::to_string(total_size) + ", actual size=" + std::to_string(result->size()) + " !";
        throw std::logic_error(msg);
    }

    return result;
}

void GenerateMetadataIndex(const char *parquet_path, const char *index_file_path,
                            std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties)
{
    auto buffer = GenerateMetadataIndex(parquet_path, decryption_properties);
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(std::string(index_file_path)));
    PARQUET_THROW_NOT_OK(outfile->Write(buffer->data(), buffer->size()));
    PARQUET_THROW_NOT_OK(outfile->Close());
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const DataHeaderV3 &dataHeader,
                                                    const uint8_t *data_body,
                                                    size_t body_size,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names,
                                                    bool schema_only,
                                                    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties,
                                                    bool preserve_indices)
{

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
    auto column_names_start = (uint8_t *)&column_chunks_offsets[dataHeader.get_column_chunks_offsets_size()];
    auto column_names_ptr = column_names_start;
    std::shared_ptr<arrow::Buffer> decrypted_metadata_buf;
    uint8_t *metadata_src;
    uint32_t metadata_src_length;
    std::shared_ptr<parquet::InternalFileDecryptor> file_decryptor;

    if (dataHeader.encrypted_metadata_length > 0)
    {
        if (!decryption_properties)
        {
            throw std::logic_error(
                "Index was generated from an encrypted-footer file. "
                "Decryption properties must be provided to read_metadata.");
        }

        auto encrypted_meta_ptr = &column_names_ptr[dataHeader.column_names_length];
        auto crypto_meta_ptr = &encrypted_meta_ptr[dataHeader.encrypted_metadata_length];

        uint32_t crypto_len = dataHeader.crypto_metadata_length;
        auto file_crypto_metadata = parquet::FileCryptoMetaData::Make(
            crypto_meta_ptr, &crypto_len);
        auto enc_algo = file_crypto_metadata->encryption_algorithm();

        std::string file_aad = enc_algo.aad.aad_prefix + enc_algo.aad.aad_file_unique;
        file_decryptor = std::make_shared<parquet::InternalFileDecryptor>(
            decryption_properties, file_aad, enc_algo.algorithm,
            file_crypto_metadata->key_metadata(), arrow::default_memory_pool());

        auto footer_decryptor = file_decryptor->GetFooterDecryptor();
        int32_t ciphertext_len = static_cast<int32_t>(dataHeader.encrypted_metadata_length);
        int32_t plaintext_len = footer_decryptor->PlaintextLength(ciphertext_len);
        PARQUET_ASSIGN_OR_THROW(decrypted_metadata_buf, arrow::AllocateBuffer(plaintext_len));
        footer_decryptor->Decrypt(
            {encrypted_meta_ptr, static_cast<size_t>(ciphertext_len)},
            {const_cast<uint8_t *>(decrypted_metadata_buf->data()), static_cast<size_t>(plaintext_len)});

        metadata_src = const_cast<uint8_t *>(decrypted_metadata_buf->data());
        metadata_src_length = static_cast<uint32_t>(plaintext_len);
    }
    else
    {
        metadata_src = (uint8_t *)&column_names_ptr[dataHeader.column_names_length];
        metadata_src_length = dataHeader.metadata_length;
    }

    ThriftCopier thriftCopier(metadata_src, metadata_src_length);

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

        if (column_names_ptr != column_names_start + dataHeader.column_names_length)
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

    if (columns.size() > 0 && !preserve_indices)
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

    // Build a set of selected row groups for fast lookup in preserve_indices mode
    std::unordered_set<uint32_t> selected_row_groups(row_groups.begin(), row_groups.end());
    std::unordered_set<uint32_t> selected_columns(columns.begin(), columns.end());

    auto row_group_filtering = row_groups.size() > 0 || schema_only;
    if (row_group_filtering)
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

        if (preserve_indices)
        {
            // Sum all row group rows to keep total consistent
            int64_t total_rows = 0;
            for (uint32_t rg = 0; rg < dataHeader.row_groups; rg++)
                total_rows += row_numbers[rg];
            thriftCopier.WriteI64(total_rows);
        }
        else
        {
            thriftCopier.WriteI64(num_rows);
        }
        index_src = num_row_offsets[1];
    }

    if (row_group_filtering && !preserve_indices)
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

    // Pre-serialize minimal dummy templates for preserve_indices mode.
    std::vector<uint8_t> dummy_rg_template;

    if (preserve_indices)
    {
        auto tmem = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        auto tprot = std::make_shared<apache::thrift::protocol::TCompactProtocol>(tmem);
        uint8_t *ptr; uint32_t len;

        palletjack::parquet::RowGroup dummy_rg;
        dummy_rg.num_rows = 0;
        dummy_rg.total_byte_size = 0;
        dummy_rg.__set_ordinal(0);
        dummy_rg.write(tprot.get());
        tmem->getBuffer(&ptr, &len);
        dummy_rg_template.assign(ptr, ptr + len);
    }

    uint32_t rg_loop_count = preserve_indices ? dataHeader.row_groups
                           : (row_group_filtering ? static_cast<uint32_t>(row_groups.size()) : dataHeader.row_groups);

    for (uint32_t loop_i = 0; loop_i < rg_loop_count; loop_i++)
    {
        uint32_t idx = preserve_indices ? loop_i
                     : (row_group_filtering ? row_groups[loop_i] : loop_i);
        bool is_selected_rg = !row_group_filtering || selected_row_groups.count(idx);

        if (!is_selected_rg && preserve_indices)
        {
            thriftCopier.CopyFrom(dummy_rg_template.data(), dummy_rg_template.size());
            continue;
        }

        // Selected row group — copy real data
        auto row_group_offset = row_groups_offsets[1 + idx];
        index_src = row_groups_offsets[1 + idx];

        if (columns.size() > 0 && !preserve_indices)
        {
            //> 1: required list<ColumnChunk> columns
            auto chunks_list = &column_chunks_offsets[(1 + dataHeader.columns + 1) * idx];
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
            toCopy = row_groups_offsets[1 + idx + 1] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;
        }
        else if (columns.size() > 0 && preserve_indices)
        {
            auto chunks_list = &column_chunks_offsets[(1 + dataHeader.columns + 1) * idx];
            auto chunks = &chunks_list[1];
            toCopy = row_group_offset + chunks_list[0] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            thriftCopier.WriteListBegin(::apache::thrift::protocol::T_STRUCT, dataHeader.columns);

            // Serialize one dummy CC for this row group's num_values, reuse for all non-selected columns.
            std::vector<uint8_t> dummy_cc_bytes;
            {
                auto tmem = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
                auto tprot = std::make_shared<apache::thrift::protocol::TCompactProtocol>(tmem);
                palletjack::parquet::ColumnChunk dummy_cc;
                dummy_cc.__isset.meta_data = true;
                dummy_cc.meta_data.num_values = row_numbers[idx];
                dummy_cc.write(tprot.get());
                uint8_t *ptr; uint32_t len;
                tmem->getBuffer(&ptr, &len);
                dummy_cc_bytes.assign(ptr, ptr + len);
            }

            for (uint32_t c = 0; c < dataHeader.columns; c++)
            {
                if (selected_columns.count(c))
                {
                    toCopy = chunks[c + 1] - chunks[c];
                    thriftCopier.CopyFrom(row_group_offset + chunks[c], toCopy);
                }
                else
                {
                    thriftCopier.CopyFrom(dummy_cc_bytes.data(), dummy_cc_bytes.size());
                }
            }

            index_src = row_group_offset + chunks[dataHeader.columns];
            toCopy = row_groups_offsets[1 + idx + 1] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;
        }
        else
        {
            toCopy = row_groups_offsets[1 + idx + 1] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;
        }
    }

    index_src = row_groups_offsets[1 + dataHeader.row_groups];

    if (columns.size() > 0 && !preserve_indices)
    {
        //> 7: optional list<ColumnOrder> column_orders;
        if (column_orders_offsets[0] != 0)
        {
            auto column_orders_list = &column_orders_offsets[0];
            toCopy = column_orders_list[0] - index_src;
            thriftCopier.CopyFrom(index_src, toCopy);
            index_src += toCopy;

            thriftCopier.WriteListBegin(::apache::thrift::protocol::T_STRUCT, columns.size());
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
    toCopy = metadata_src_length - index_src;
    thriftCopier.CopyFrom(index_src, toCopy);

    uint32_t length = thriftCopier.GetDataSize();

    if (dataHeader.encrypted_metadata_length > 0)
    {
        // Inject encryption_algorithm and footer_signing_key_metadata into the
        // filtered Thrift so FileMetaData::Make can initialize column decryptors.
        auto filtered_metadata = DeserializeFileMetadata(thriftCopier.GetData(), length);

        auto crypto_meta_ptr = &column_names_start[dataHeader.column_names_length +
                                                    dataHeader.encrypted_metadata_length];
        uint32_t crypto_len = dataHeader.crypto_metadata_length;
        auto file_crypto_metadata = parquet::FileCryptoMetaData::Make(
            crypto_meta_ptr, &crypto_len);
        auto enc_algo_full = file_crypto_metadata->encryption_algorithm();

        filtered_metadata.__isset.encryption_algorithm = true;
        if (enc_algo_full.algorithm == parquet::ParquetCipher::AES_GCM_V1)
        {
            filtered_metadata.encryption_algorithm.__isset.AES_GCM_V1 = true;
            filtered_metadata.encryption_algorithm.AES_GCM_V1.__isset.aad_prefix = !enc_algo_full.aad.aad_prefix.empty();
            filtered_metadata.encryption_algorithm.AES_GCM_V1.aad_prefix = enc_algo_full.aad.aad_prefix;
            filtered_metadata.encryption_algorithm.AES_GCM_V1.__isset.aad_file_unique = !enc_algo_full.aad.aad_file_unique.empty();
            filtered_metadata.encryption_algorithm.AES_GCM_V1.aad_file_unique = enc_algo_full.aad.aad_file_unique;
            filtered_metadata.encryption_algorithm.AES_GCM_V1.__isset.supply_aad_prefix = enc_algo_full.aad.supply_aad_prefix;
            filtered_metadata.encryption_algorithm.AES_GCM_V1.supply_aad_prefix = enc_algo_full.aad.supply_aad_prefix;
        }
        else
        {
            filtered_metadata.encryption_algorithm.__isset.AES_GCM_CTR_V1 = true;
            filtered_metadata.encryption_algorithm.AES_GCM_CTR_V1.__isset.aad_prefix = !enc_algo_full.aad.aad_prefix.empty();
            filtered_metadata.encryption_algorithm.AES_GCM_CTR_V1.aad_prefix = enc_algo_full.aad.aad_prefix;
            filtered_metadata.encryption_algorithm.AES_GCM_CTR_V1.__isset.aad_file_unique = !enc_algo_full.aad.aad_file_unique.empty();
            filtered_metadata.encryption_algorithm.AES_GCM_CTR_V1.aad_file_unique = enc_algo_full.aad.aad_file_unique;
            filtered_metadata.encryption_algorithm.AES_GCM_CTR_V1.__isset.supply_aad_prefix = enc_algo_full.aad.supply_aad_prefix;
            filtered_metadata.encryption_algorithm.AES_GCM_CTR_V1.supply_aad_prefix = enc_algo_full.aad.supply_aad_prefix;
        }
        filtered_metadata.__isset.footer_signing_key_metadata = true;
        filtered_metadata.footer_signing_key_metadata = file_crypto_metadata->key_metadata();

        auto tmem = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        auto tprot = std::make_shared<apache::thrift::protocol::TCompactProtocol>(tmem);
        filtered_metadata.write(tprot.get());
        uint8_t *ptr; uint32_t len;
        tmem->getBuffer(&ptr, &len);

        // Build PAR1 container with injected encryption fields.
        // ParquetFileReader::Open on a plaintext footer with encryption_algorithm set
        // will create the InternalFileDecryptor for column decryption.
        parquet::ReaderProperties reader_props;
        auto dec_props_builder = parquet::FileDecryptionProperties::Builder();
        dec_props_builder.key_retriever(decryption_properties->key_retriever());
        dec_props_builder.disable_footer_signature_verification();
        dec_props_builder.plaintext_files_allowed();
        reader_props.file_decryption_properties(dec_props_builder.build());

        size_t container_size = len + 4 + 4;
        std::shared_ptr<arrow::Buffer> container_buf;
        PARQUET_ASSIGN_OR_THROW(container_buf, arrow::AllocateBuffer(container_size));
        auto *cdst = const_cast<uint8_t *>(container_buf->data());
        memcpy(cdst, ptr, len);
        uint32_t le_len = len;
        memcpy(cdst + len, &le_len, 4);
        memcpy(cdst + len + 4, parquet::kParquetMagic, 4);

        auto source = std::make_shared<arrow::io::BufferReader>(container_buf);
        auto reader = parquet::ParquetFileReader::Open(source, reader_props);
        return reader->metadata();
    }

    parquet::ReaderProperties reader_props;
    if (decryption_properties)
    {
        auto dec_props_builder = parquet::FileDecryptionProperties::Builder();
        dec_props_builder.key_retriever(decryption_properties->key_retriever());
        dec_props_builder.disable_footer_signature_verification();
        dec_props_builder.plaintext_files_allowed();
        reader_props.file_decryption_properties(dec_props_builder.build());

        uint32_t metadata_len = length;
        size_t container_size = metadata_len + 4 + 4;
        std::shared_ptr<arrow::Buffer> container_buf;
        PARQUET_ASSIGN_OR_THROW(container_buf, arrow::AllocateBuffer(container_size));
        auto *dst = const_cast<uint8_t *>(container_buf->data());
        memcpy(dst, thriftCopier.GetData(), metadata_len);
        uint32_t le_len = metadata_len;
        memcpy(dst + metadata_len, &le_len, 4);
        memcpy(dst + metadata_len + 4, parquet::kParquetMagic, 4);

        auto source = std::make_shared<arrow::io::BufferReader>(container_buf);
        auto reader = parquet::ParquetFileReader::Open(source, reader_props);
        return reader->metadata();
    }

    return parquet::FileMetaData::Make(thriftCopier.GetData(), &length, reader_props);
}

const DataHeaderV3 *ReadAndParseHeader(const unsigned char *data, size_t data_length,
                                        size_t &header_size, DataHeaderV3 &pj2_buf)
{
    if (data_length < sizeof(DataHeaderV2))
    {
        throw std::logic_error("Index data is too small, length=" + std::to_string(data_length));
    }

    bool is_pj2 = (memcmp(data, HEADER_PJ2, HEADER_LENGTH) == 0);
    bool is_pj3 = (memcmp(data, HEADER_PJ3, HEADER_LENGTH) == 0);
    if (!is_pj2 && !is_pj3)
    {
        throw std::logic_error("Index file has unexpected format!");
    }

    if (is_pj2)
    {
        header_size = sizeof(DataHeaderV2);
        const DataHeaderV2 *pj2 = reinterpret_cast<const DataHeaderV2 *>(data);
        pj2_buf = {};
        memcpy(pj2_buf.header, pj2->header, HEADER_LENGTH);
        pj2_buf.row_groups = pj2->row_groups;
        pj2_buf.columns = pj2->columns;
        pj2_buf.column_names_length = pj2->column_names_length;
        pj2_buf.metadata_length = pj2->metadata_length;
        pj2_buf.encrypted_metadata_length = 0;
        pj2_buf.crypto_metadata_length = 0;
        return &pj2_buf;
    }

    header_size = sizeof(DataHeaderV3);
    if (data_length < sizeof(DataHeaderV3))
    {
        throw std::logic_error("Index data is too small for V3 header, length=" + std::to_string(data_length));
    }
    return reinterpret_cast<const DataHeaderV3 *>(data);
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const char *index_file_path,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names,
                                                    bool schema_only,
                                                    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties,
                                                    bool preserve_indices)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(std::string(index_file_path)));

    int64_t file_size;
    PARQUET_ASSIGN_OR_THROW(file_size, infile->GetSize());

    std::shared_ptr<arrow::Buffer> all_data;
    PARQUET_ASSIGN_OR_THROW(all_data, infile->Read(file_size));

    size_t header_size = 0;
    DataHeaderV3 pj2_buf;
    const DataHeaderV3 *dataHeader;
    try
    {
        dataHeader = ReadAndParseHeader(all_data->data(), all_data->size(), header_size, pj2_buf);
    }
    catch (const std::logic_error &)
    {
        auto msg = std::string("File '") + index_file_path + "' has unexpected format!";
        throw std::logic_error(msg);
    }

    auto body_size = dataHeader->get_body_size();
    size_t expected = header_size + body_size;
    if (static_cast<size_t>(all_data->size()) != expected)
    {
        auto msg = std::string("File '") + index_file_path + "' has unexpected size, expected=" +
                   std::to_string(expected) + ", actual=" + std::to_string(all_data->size());
        throw std::logic_error(msg);
    }

    return ReadMetadata(*dataHeader, all_data->data() + header_size, body_size,
                        row_groups, column_indices, column_names, schema_only, decryption_properties, preserve_indices);
}

std::shared_ptr<parquet::FileMetaData> ReadMetadata(const unsigned char *index_data,
                                                    size_t index_data_length,
                                                    const std::vector<uint32_t> &row_groups,
                                                    const std::vector<uint32_t> &column_indices,
                                                    const std::vector<std::string> &column_names,
                                                    bool schema_only,
                                                    std::shared_ptr<parquet::FileDecryptionProperties> decryption_properties,
                                                    bool preserve_indices)
{
    size_t header_size = 0;
    DataHeaderV3 pj2_buf;
    auto dataHeader = ReadAndParseHeader(index_data, index_data_length, header_size, pj2_buf);

    size_t expected_length = header_size + dataHeader->get_body_size();
    if (index_data_length != expected_length)
    {
        auto msg = std::string("Index data has unexpected length, length=") + std::to_string(index_data_length) + ", expected=" + std::to_string(expected_length);
        throw std::logic_error(msg);
    }

    return ReadMetadata(*dataHeader, &index_data[header_size], index_data_length - header_size,
                        row_groups, column_indices, column_names, schema_only, decryption_properties, preserve_indices);
}