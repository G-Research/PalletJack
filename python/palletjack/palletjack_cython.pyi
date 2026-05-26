from typing import Optional, Sequence, overload

import pyarrow as pa
import pyarrow.parquet as pq

@overload
def generate_metadata_index(
    parquet_path: str,
    index_file_path: str,
    decryption_properties: Optional[pq.FileDecryptionProperties] = None,
) -> None: ...
@overload
def generate_metadata_index(
    parquet_path: str,
    decryption_properties: Optional[pq.FileDecryptionProperties] = None,
) -> bytearray:
    """Generate a metadata index for a Parquet file.

    Args:
        parquet_path: Path to the source Parquet file.
        index_file_path: If provided, the index is written to this path and
            ``None`` is returned.  If omitted, the index is returned as a
            ``bytearray``.
        decryption_properties: Decryption properties for encrypted Parquet
            files. Required for files with encrypted footers (PARE magic).
            Not needed for plaintext-footer files with encrypted column
            metadata.

    Returns:
        The serialized index when *index_file_path* is ``None``, otherwise
        ``None``.
    """
    ...

def read_metadata(
    index_file_path: Optional[str] = None,
    row_groups: Sequence[int] = [],
    column_indices: Sequence[int] = [],
    column_names: Sequence[str] = [],
    index_data: Optional[bytes] = None,
    decryption_properties: Optional[pq.FileDecryptionProperties] = None,
) -> pq.FileMetaData:
    """Read Parquet metadata from a previously generated index.

    Supply either *index_file_path* or *index_data*, not both.
    *column_indices* and *column_names* are mutually exclusive.

    Args:
        index_file_path: Path to the index file on disk.
        row_groups: Subset of row-group indices to read.
        column_indices: Subset of column indices to read.
        column_names: Subset of column names to read.
        index_data: In-memory index bytes (e.g. from
            :func:`generate_metadata_index`).
        decryption_properties: Decryption properties for indexes generated
            from encrypted Parquet files. Currently unused (reserved for
            future encrypted-footer index support).

    Returns:
        A :class:`pyarrow.parquet.FileMetaData` instance containing only the
        requested subset of metadata.
    """
    ...

def read_schema(
    index_file_path: Optional[str] = None,
    column_indices: Sequence[int] = [],
    column_names: Sequence[str] = [],
    index_data: Optional[bytes] = None,
    decryption_properties: Optional[pq.FileDecryptionProperties] = None,
) -> pa.Schema:
    """Read the Arrow schema from a previously generated index.

    Skips row-group decoding entirely.

    Supply either *index_file_path* or *index_data*, not both.
    *column_indices* and *column_names* are mutually exclusive.

    Args:
        index_file_path: Path to the index file on disk.
        column_indices: Subset of column indices to read.
        column_names: Subset of column names to read.
        index_data: In-memory index bytes (e.g. from
            :func:`generate_metadata_index`).
        decryption_properties: Decryption properties for indexes generated
            from encrypted Parquet files. Currently unused (reserved for
            future encrypted-footer index support).

    Returns:
        A :class:`pyarrow.Schema` instance.
    """
    ...
