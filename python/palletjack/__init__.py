# Importing pyarrow is necessary to load the runtime libraries
import pyarrow
import pyarrow.parquet

from .package_metadata import (
    __version__,
    __dependencies__
)

try:
    from .palletjack_cython import *
except ImportError as e:
    if any(x in str(e) for x in ['arrow', 'parquet']):
        pyarrow_req = next((r for r in __dependencies__ if r.startswith('pyarrow')), '')
        raise ImportError(f"This version of {__package__}={__version__} is built against {pyarrow_req}, please ensure you have it installed. Current pyarrow version is {pyarrow.__version__}. ({str(e)})") from None
    else:
        raise
