from time import strftime

import pandas as pd

from gndhelper.load import load_redirect_dump
from gndhelper.query import query_current_gnd_values_in_wikidata
from gndhelper.verify import get_redirect_target


def main() -> None:
    current_gnd_values_in_wikidata = query_current_gnd_values_in_wikidata()
    gnd_redirects = load_redirect_dump()

    df = current_gnd_values_in_wikidata.merge(
        right=gnd_redirects,
        how='inner',
        left_on='gnd',
        right_on='redirect'
    )

    print(f"""Found:
* {current_gnd_values_in_wikidata.shape[0]} GNDs in Wikidata
* {gnd_redirects.shape[0]} redirects in GND
* {df.shape[0]} redirect GNDs in Wikidata
""")

    with open('./output/redirect_issue.tsv', mode='w', encoding='utf8') as file_handle:
        for i, row in enumerate(df.itertuples(), start=1):
            if i%100==0:
                print(f'{strftime("%H:%M:%S")}: ({i}/{df.shape[0]})')

            try:
                target = get_redirect_target(row.redirect)
            except RuntimeWarning:  # not a redirect then, can be ignored
                continue

            if target==row.target:  # URL redirect target same as in dump
                continue

            # fields in row: item s gnd rank redirect=gnd target
            print(row.item, row.s, row.gnd, row.rank, row.target, target)
            file_handle.write(f'{row.item}\t{row.s}\t{row.gnd}\t{row.rank}\t{row.target}\t{target}\n')


if __name__=='__main__':
    main()
