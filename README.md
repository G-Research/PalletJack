# PalletJack
PalletJack was created as a workaround for apache/arrow#38149. The standard parquet reader is not efficient for files with numerous columns and row groups, as it requires parsing the entire metadata section each time the file is opened. The size of this metadata section is proportional to the number of columns and row groups in the file.

PalletJack reduces the amount of metadata bytes that need to be read and decoded by storing metadata in a different format. This approach enables reading only the essential subset of metadata as required.

## Features

- Storing parquet metadata in an indexed format
- Reading parquet metadata for a subset of row groups and columns
