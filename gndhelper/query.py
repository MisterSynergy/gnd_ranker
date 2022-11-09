from io import StringIO
import logging
from time import sleep

import pandas as pd
import requests

from .config import USER_AGENT, WDQS_ENDPOINT, WDQS_SLICE_LIMIT
from .config import WD, WIKIBASE, S
from .config import PID_GND, PID_DEPRECATION, QID_REDIRECT, QID_UNDIFFERENTIATED, QID_WITHDRAWN

LOG = logging.getLogger(__name__)


def query_wdqs(query:str) -> str:
    response = requests.post(
        url=WDQS_ENDPOINT,
        data={
            'query' : query
        },
        headers={
            'User-Agent': USER_AGENT,
            'Accept' : 'text/csv'
        }
    )

    return response.text


def query_current_gnd_values_in_wikidata() -> pd.DataFrame:
    query = """
    SELECT ?item ?s ?gnd ?rank WHERE {{
        SERVICE bd:slice {{
            ?item p:{prop} ?s .
            bd:serviceParam bd:slice.offset {offset} .
            bd:serviceParam bd:slice.limit {limit} .
        }}
        ?s wikibase:rank ?rank; ps:{prop} ?gnd .
    }}
    """
    columns = {
        'item' : str,
        's' : str,
        'gnd' : str,
        'rank' : str
    }
    dfs:list[pd.DataFrame] = []

    offset = 0

    while True:
        chunk = query_wdqs(
            query.format(
                prop=PID_GND,
                offset=offset,
                limit=WDQS_SLICE_LIMIT
            )
        )

        try:
            df_slice = pd.read_csv(
                StringIO(chunk),
                header=0,
                names=list(columns.keys()),
                dtype=columns
            )
        except pd.errors.ParserError as exception:
            # exhausted server yields an error message
            break
        else:
            df_slice['item'] = df_slice['item'].str.slice(start=len(WD))
            df_slice['s'] = df_slice['s'].str.slice(start=len(S))
            df_slice['rank'] = df_slice['rank'].str.slice(start=len(WIKIBASE))

        dfs.append(df_slice)
        offset += WDQS_SLICE_LIMIT

    df = pd.concat(dfs, ignore_index=True)

    duplicated_entries = df[['item', 's']].value_counts()
    if duplicated_entries[duplicated_entries>1].shape[0] > 0:  # wait a moment and then redo if duplicates are found
        sleep(10)
        return query_current_gnd_values_in_wikidata()

    return df


def query_current_incorrect_gnd_values_in_wikidata() -> pd.DataFrame:
    query = f"""
    SELECT ?item ?gnd ?rank WHERE {{
        ?item p:{PID_GND} [ ps:{PID_GND} ?gnd; wikibase:rank ?rank; pq:{PID_DEPRECATION} wd:{QID_WITHDRAWN} ]
    }}
    """
    columns = {
        'item' : str,
        'gnd' : str,
        'rank' : str
    }

    df = pd.read_csv(
        StringIO(query_wdqs(query)),
        header=0,
        names=list(columns.keys()),
        dtype=columns
    )

    df['item'] = df['item'].str.slice(start=len(WD))
    df['rank'] = df['rank'].str.slice(start=len(WIKIBASE))

    return df


def query_current_gnds_tagged_as_redirects_in_wikidata() -> pd.DataFrame:
    query = f"""
    SELECT ?item ?gnd ?rank WHERE {{
        ?item p:{PID_GND} [ ps:{PID_GND} ?gnd; wikibase:rank ?rank; pq:{PID_DEPRECATION} wd:{QID_REDIRECT} ]
    }}
    """
    columns = {
        'item' : str,
        'gnd' : str,
        'rank' : str
    }

    df = pd.read_csv(
        StringIO(query_wdqs(query)),
        header=0,
        names=list(columns.keys()),
        dtype=columns
    )

    df['item'] = df['item'].str.slice(start=len(WD))
    df['rank'] = df['rank'].str.slice(start=len(WIKIBASE))

    return df


def query_current_gnds_tagged_as_undifferentiated_in_wikidata() -> pd.DataFrame:
    query = f"""
    SELECT ?item ?gnd ?rank WHERE {{
        ?item p:{PID_GND} [ ps:{PID_GND} ?gnd; wikibase:rank ?rank; pq:{PID_DEPRECATION} wd:{QID_UNDIFFERENTIATED} ]
    }}
    """
    columns = {
        'item' : str,
        'gnd_tn' : str,
        'rank' : str
    }

    df = pd.read_csv(
        StringIO(query_wdqs(query)),
        header=0,
        names=list(columns.keys()),
        dtype=columns
    )

    df['item'] = df['item'].str.slice(start=len(WD))
    df['rank'] = df['rank'].str.slice(start=len(WIKIBASE))

    return df


def query_odd_ranking() -> pd.DataFrame:
    query = f"""
    SELECT ?item WHERE {{
      ?item p:{PID_GND}/wikibase:rank wikibase:PreferredRank .
      MINUS {{ ?item p:{PID_GND}/wikibase:rank wikibase:NormalRank }} .
    }}
    """
    columns = {
        'item' : str
    }

    df = pd.read_csv(
        StringIO(query_wdqs(query)),
        header=0,
        names=list(columns.keys()),
        dtype=columns
    )

    df['item'] = df['item'].str.slice(start=len(WD))

    return df