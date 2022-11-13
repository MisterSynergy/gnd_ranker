from time import strftime

import pandas as pd

from gndhelper.load import load_latest_valid_identifier_dump, load_redirect_dump
from gndhelper.query import query_current_gnd_values_in_wikidata
from gndhelper.verify import is_valid_gnd_identifier


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

    with open('./output/invalid_issue.tsv', mode='w', encoding='utf8') as file_handle:
        for i, row in enumerate(df.loc[not_deprecated_filter].itertuples(), start=1):
            if i%100==0:
                print(f'{strftime("%H:%M:%S")}: ({i}/{df.loc[not_deprecated_filter].shape[0]})')

            gnd_is_valid = is_valid_gnd_identifier(row.gnd)

            if gnd_is_valid is True:
                continue

            print(row.item, row.s, row.gnd, row.rank, gnd_is_valid)
            file_handle.write(f'{row.item}\t{row.s}\t{row.gnd}\t{row.rank}\t{gnd_is_valid}\n')


if __name__=='__main__':
    main()
