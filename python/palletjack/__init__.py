# Importing pyarrow is necessary to load the runtime libraries
import pyarrow
import pyarrow.parquet
from importlib.metadata import version, requires
__version__ = version(__package__)  # Uses package metadata
dependencies = requires(__package__)
pyarrow_req = next((r for r in dependencies if r.startswith('pyarrow')), '')

try:
    from .palletjack_cython import *
except ImportError as e:
    if any(x in str(e) for x in ['arrow', 'parquet']):
        raise ImportError(f"This version of {__package__}={__version__} is built against {pyarrow_req}, please ensure you have it installed. Current pyarrow version is {pyarrow.__version__}. ({str(e)})") from None
    else:
        raise
