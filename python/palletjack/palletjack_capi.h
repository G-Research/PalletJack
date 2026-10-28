#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "parquet/metadata.h"

// C API for other extension modules, exported as the PyCapsule palletjack.palletjack_capi (created in
// palletjack_cython and re-exported by palletjack/__init__.py). The capsule name is the attribute path, so C code
// can get the table with PyCapsule_Import(PALLETJACK_CAPI_CAPSULE_NAME, 0). A caller holding the table can call
// the functions declared in palletjack.h without the GIL and without calling back into Python.
//
// A caller must check capi->version >= the PALLETJACK_CAPI_VERSION that added the newest member it uses.
// The function types use C++ standard library and Arrow types, so the caller must be built against the same Arrow
// major and minor version, with a compatible compiler and C++ standard library. The functions throw std::exception
// on error, the caller must catch it.
//
// The table only grows. Members are never changed, reordered or removed, so a caller built against an older
// header keeps working. A new parameter is added as a new member with the next suffix (V2, V3, ...), and the older
// member keeps its signature and behaviour. A breaking change needs a new capsule name.
#define PALLETJACK_CAPI_CAPSULE_NAME "palletjack.palletjack_capi"

// Bumped when members are appended to PalletJackCAPI.
#define PALLETJACK_CAPI_VERSION 1

// Types of the functions in palletjack.h, one per overload.
using PalletJackGenerateMetadataIndexToBufferV1Func = std::shared_ptr<arrow::Buffer> (*)(const char *parquet_path);

using PalletJackGenerateMetadataIndexToFileV1Func = void (*)(const char *parquet_path, const char *index_file_path);

using PalletJackReadMetadataFromBufferV1Func =
    std::shared_ptr<parquet::FileMetaData> (*)(const unsigned char *index_data,
                                               size_t index_data_length,
                                               const std::vector<uint32_t> &row_groups,
                                               const std::vector<uint32_t> &column_indices,
                                               const std::vector<std::string> &column_names,
                                               bool schema_only);

using PalletJackReadMetadataFromFileV1Func =
    std::shared_ptr<parquet::FileMetaData> (*)(const char *index_file_path,
                                               const std::vector<uint32_t> &row_groups,
                                               const std::vector<uint32_t> &column_indices,
                                               const std::vector<std::string> &column_names,
                                               bool schema_only);

struct PalletJackCAPI
{
    int version; // PALLETJACK_CAPI_VERSION palletjack was built with
    // Added in version 1
    PalletJackGenerateMetadataIndexToBufferV1Func GenerateMetadataIndexToBufferV1;
    PalletJackGenerateMetadataIndexToFileV1Func GenerateMetadataIndexToFileV1;
    PalletJackReadMetadataFromBufferV1Func ReadMetadataFromBufferV1;
    PalletJackReadMetadataFromFileV1Func ReadMetadataFromFileV1;
};

// Implemented in palletjack.cc, returns the table the capsule points to.
const PalletJackCAPI *GetCAPI();
