# ADR-0001: Parquet Modular Encryption Support in PalletJack

## Status

Accepted

## Date

2026-05-25

## Context

PalletJack generates a metadata index file that stores byte offsets into the
serialized Thrift footer of a Parquet file. These offsets allow selective
extraction of metadata for specific row groups and columns without
re-parsing the entire footer.

Parquet Modular Encryption (PME) introduces two modes that affect footer
metadata:

1. **Plaintext footer with encrypted column metadata.** The file ends with
   `PAR1`. The `FileMetaData` Thrift structure is readable without keys.
   Individual `ColumnChunk` entries contain `encrypted_column_metadata` (an
   AES-encrypted blob) instead of a plaintext `meta_data` field, plus a
   `crypto_metadata` field describing the encryption key type. The
   `FileMetaData` also carries `encryption_algorithm` and
   `footer_signing_key_metadata` fields.

2. **Encrypted footer.** The file ends with `PARE`. The footer region
   contains a plaintext `FileCryptoMetaData` Thrift structure (algorithm and
   key metadata) followed by the AES-encrypted `FileMetaData` bytes.

PalletJack currently rejects both modes.

## Decision

PalletJack will support both encryption modes. Encrypted data stored in the
original Parquet footer will remain encrypted in the PalletJack index file.
The byte offset arrays in the index file will remain unencrypted.

### Mode 1: Plaintext footer with encrypted column metadata

**Index generation** requires no decryption keys. The Thrift `FileMetaData`
is parseable as-is. The `encrypted_column_metadata` field is a standard
Thrift binary field with known byte boundaries, and the existing
`column_chunks_offsets` already encompass it. The encryption-related fields
(`encryption_algorithm`, `footer_signing_key_metadata`) sit after
`column_orders` in the Thrift and are already preserved by the existing
"copy leftovers" logic in `ThriftCopier`.

**Reading** requires decryption keys. After `ThriftCopier` produces the
spliced Thrift bytes for the requested columns and row groups, PalletJack
must:

- Accept `FileDecryptionProperties` from the caller.
- Construct an `InternalFileDecryptor` using the `encryption_algorithm` and
  `footer_signing_key_metadata` from the Thrift blob.
- Pass it to `FileMetaData::Make`, which already accepts an optional
  `InternalFileDecryptor`. Arrow then decrypts `encrypted_column_metadata`
  internally when creating `ColumnChunkMetaData` objects.
- Call `set_file_decryptor` on the returned `FileMetaData`, matching
  Arrow's own `ParseMetaDataOfEncryptedFileWithPlaintextFooter` behavior.

### Mode 2: Encrypted footer

**Index generation** requires the footer key. The flow:

1. Detect `PARE` magic bytes at the end of the Parquet file.
2. Accept `FileDecryptionProperties` containing the footer key.
3. Parse the plaintext `FileCryptoMetaData` to obtain the algorithm and key
   metadata.
4. Construct `InternalFileDecryptor`, decrypt the footer bytes into
   plaintext Thrift.
5. Compute byte offsets from the decrypted Thrift (same logic as today).
6. Write to the index file: the unencrypted offset arrays, the original
   encrypted footer bytes (as read from the Parquet file), and the
   `FileCryptoMetaData` blob.

The decrypted Thrift is used only transiently to compute offsets. It is
never written to disk.

**Reading** requires the footer key. The flow:

1. Detect `crypto_metadata_length > 0` in the index header.
2. Require `FileDecryptionProperties` from the caller.
3. Parse the stored `FileCryptoMetaData`.
4. Construct `InternalFileDecryptor`, decrypt the stored encrypted footer.
5. Apply `ThriftCopier` splicing on the decrypted bytes using the stored
   offsets.
6. Pass the spliced result and `InternalFileDecryptor` to
   `FileMetaData::Make`.

### Index file format

Add a `crypto_metadata_length` field (uint32) to `DataHeader`. When zero,
the file is non-encrypted or uses plaintext footer mode (no format change
needed for Mode 1). When non-zero, the index contains the
`FileCryptoMetaData` blob appended after the existing metadata section.

Bump the format magic from `PJ_2` to `PJ_3`. Retain read support for
`PJ_2` files.

### Index file layout (PJ_3, encrypted footer)

```
+----------------------------+
| DataHeader                 |
|   'PJ_3'                   |
|   row_groups               |
|   columns                  |
|   column_names_length      |
|   metadata_length          |
|   crypto_metadata_length   |
+----------------------------+
| Offset arrays (uint32[])   |
|   num_rows_offsets          |
|   row_numbers              |
|   schema_offsets            |
|   schema_num_children       |
|   row_groups_offsets        |
|   column_orders_offsets     |
|   column_chunks_offsets     |
+----------------------------+
| Column names (NUL-term)    |
+----------------------------+
| Encrypted metadata (bytes) |
+----------------------------+
| FileCryptoMetaData (bytes) |
+----------------------------+
```

The offsets reference byte positions within the decrypted form of the
encrypted metadata section.

## Changes by file

| File | Change |
|---|---|
| `palletjack.h` | Add optional `FileDecryptionProperties` parameter to `GenerateMetadataIndex` and `ReadMetadata` |
| `palletjack.cc` - `DataHeader` | Add `crypto_metadata_length` field, define `PJ_3` magic, keep `PJ_2` read support |
| `palletjack.cc` - `GenerateMetadataIndex` | Detect `PARE` magic, decrypt footer with caller-provided key to compute offsets, store encrypted footer + `FileCryptoMetaData` in index. For plaintext footer mode, proceed as today. |
| `palletjack.cc` - `ReadMetadata` | Construct `InternalFileDecryptor` from stored crypto metadata and caller's `FileDecryptionProperties`. For encrypted footer: decrypt before splicing. For plaintext footer with encryption: pass decryptor to `FileMetaData::Make`. |
| `palletjack_cython.pyx` / `.pxd` | Expose `FileDecryptionProperties` parameter |
| `__init__.py` | Accept `pyarrow.parquet.FileDecryptionProperties` in public API |
| `parquet_types_palletjack.h/.cpp` | No changes. The custom Thrift struct already tracks offsets that encompass encryption fields within `ColumnChunk` boundaries. |
| Tests | Convert existing encrypted tests from "expect error" to "expect success". Add tests for selective column and row group reads under both encryption modes. |

## Consequences

- Users of encrypted Parquet files can generate and use PalletJack metadata
  indices.
- The index file never contains decrypted metadata. The security model of
  the original Parquet file is preserved: anyone who obtains the index file
  still needs the correct keys to read the metadata.
- Index generation for encrypted-footer files requires the footer key. This
  is unavoidable because offset computation requires parsing the Thrift
  structure.
- The `PJ_3` format is backward-incompatible for encrypted-footer indices.
  Non-encrypted and plaintext-footer indices written as `PJ_3` remain
  structurally identical to `PJ_2` (with `crypto_metadata_length = 0`),
  but older readers will reject the magic bytes.
- Arrow's `InternalFileDecryptor` and `FileDecryptionProperties` become
  build-time dependencies. These are part of the Arrow Parquet library when
  built with encryption support (`PARQUET_REQUIRE_ENCRYPTION=ON`).
