import logging
import pandas as pd

from .config import TN_DUMP, REDIRECT_DUMP, VALID_GND_DUMP


LOG = logging.getLogger(__name__)


def load_latest_tn_dump() -> pd.DataFrame:
    df = pd.read_csv(
        TN_DUMP,
        names=[ 'Tn' ],
        dtype={ 'Tn' : str }
    )

    return df


def load_redirect_dump() -> pd.DataFrame:
    df = pd.read_csv(
        REDIRECT_DUMP,
        sep='\t',
        names=[ 'redirect' ],
        dtype={ 'redirect' : str },
        usecols=[0]
    )

    return df


def load_latest_valid_identifier_dump() -> pd.DataFrame:
    df = pd.read_csv(
        VALID_GND_DUMP,
        names=[ 'GND' ],
        dtype={ 'GND' : str }
    )

    return df
