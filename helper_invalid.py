from time import sleep

import pandas as pd
import requests

from gndhelper.config import GND_SLEEP, USER_AGENT
from gndhelper.load import load_latest_valid_identifier_dump, load_redirect_dump
from gndhelper.query import query_current_gnd_values_in_wikidata
from gndhelper.verify import is_valid_gnd_identifier


def is_valid_dnb_identifier(gnd:str) -> bool:
    sleep(GND_SLEEP)

    response = requests.head(
        url=f'https://d-nb.info/{gnd}',
        allow_redirects=True,
        headers={ 'User-Agent' : USER_AGENT }
    )

    return (response.status_code != 404)


def main() -> None:
    current_gnd_values_in_wikidata = query_current_gnd_values_in_wikidata()
    gnds_from_dump = load_latest_valid_identifier_dump()
    gnd_redirects = load_redirect_dump()

    df = current_gnd_values_in_wikidata.merge(
        gnds_from_dump,
        how='left',
        left_on='gnd',
        right_on='GND',
        indicator=True
    ).merge(
        gnd_redirects,
        how='left',
        left_on='gnd',
        right_on='redirect'
    )

    not_deprecated_filter = (df['rank']!='DeprecatedRank') \
                          & (df['_merge']=='left_only') \
                          & (df['redirect'].isna())

    print(f"""Found:
* {current_gnd_values_in_wikidata.shape[0]} GNDs in Wikidata
* {gnds_from_dump.shape[0]} GNDs in dump
* {gnd_redirects.shape[0]} GND redirects
* {df.loc[not_deprecated_filter].shape[0]} potentially invalid GNDs in Wikidata
""")

    with open('./output/invalid_issue.tsv', mode='w', encoding='utf8') as file_handle, open('./output/invalid_issue_wikitable.txt', mode='w', encoding='utf8') as wt_handle:
        wt_handle.write('{| class="wikitable"\n|-\n! hist !! item !! invalid GND !! check DNB\n')
        for i, row in enumerate(df.loc[not_deprecated_filter].itertuples(), start=1):
            if is_valid_gnd_identifier(row.gnd) is True:
                continue

            valid_dnb = is_valid_dnb_identifier(row.gnd)

            if valid_dnb is True:
                dnb = f'[https://d-nb.info/{row.gnd} {row.gnd}]'
            else:
                dnb = ''

            print(i, row.item, row.gnd, row.rank, valid_dnb)
            file_handle.write(f'{row.item}\t{row.s}\t{row.gnd}\t{row.rank}\t{valid_dnb}\n')
            wt_handle.write(f'|-\n| [https://www.wikidata.org/w/index.php?title={row.item}&action=history hist] || {{{{Q|{row.item}}}}} || [https://d-nb.info/gnd/{row.gnd} {row.gnd}] || {dnb}\n')
            sleep(GND_SLEEP)

        wt_handle.write('|}')

if __name__=='__main__':
    main()
