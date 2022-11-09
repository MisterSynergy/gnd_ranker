import logging

import pandas as pd


from .config import RESULTS_FOLDER
from .load import load_latest_tn_dump, load_redirect_dump, load_latest_valid_identifier_dump
from .query import query_current_gnd_values_in_wikidata, query_current_gnds_tagged_as_undifferentiated_in_wikidata, \
    query_current_gnds_tagged_as_redirects_in_wikidata, query_current_incorrect_gnd_values_in_wikidata, query_odd_ranking


LOG = logging.getLogger(__name__)


def query_items_with_untagged_redirects() -> pd.DataFrame:
    current_gnd_values_in_wikidata = query_current_gnd_values_in_wikidata()
    LOG.info(f'Found {current_gnd_values_in_wikidata.shape[0]} GND identifiers in Wikidata')

    gnd_redirects = load_redirect_dump()
    LOG.info(f'Found {gnd_redirects.shape[0]} redirects in dump')

    gnds_taggeds_as_redirects_in_wikidata = query_current_gnds_tagged_as_redirects_in_wikidata()
    LOG.info(f'Found {gnds_taggeds_as_redirects_in_wikidata.shape[0]} GND identifiers tagged as redirect in Wikidata')

    df = current_gnd_values_in_wikidata.merge(
        right=gnd_redirects,
        how='inner',
        left_on='gnd',
        right_on='redirect'
    ).merge(
        right=gnds_taggeds_as_redirects_in_wikidata,
        how='left',
        left_on='gnd',
        right_on='gnd',
        suffixes=(None, '_cur')
    )

    filt = df['item_cur'].isna() | (df['rank_cur']!='DeprecatedRank')
    LOG.info(f'Found {df.loc[filt].shape[0]} redirecting GND identifiers in Wikidata')

    df.loc[filt].to_csv(
        f'{RESULTS_FOLDER}untagged_redirects.tsv',
        sep='\t'
    )

    return df.loc[filt]


def query_items_with_untagged_tns() -> pd.DataFrame:
    current_gnd_values_in_wikidata = query_current_gnd_values_in_wikidata()
    LOG.info(f'Found {current_gnd_values_in_wikidata.shape[0]} GND identifiers in Wikidata')

    current_tn_values_in_wikidata = query_current_gnds_tagged_as_undifferentiated_in_wikidata()
    LOG.info(f'Found {current_tn_values_in_wikidata.shape[0]} GND identifiers in Wikidata that are marked as undifferentiated')

    tns_from_dump = load_latest_tn_dump()   
    LOG.info(f'Found {tns_from_dump.shape[0]} Tn identifiers in latest dump')

    potential_tns_in_wd = current_gnd_values_in_wikidata.merge(
        right=tns_from_dump,
        how='inner',
        left_on='gnd',
        right_on='Tn'
    ).merge(
        right=current_tn_values_in_wikidata,
        how='left',
        left_on='gnd',
        right_on='gnd_tn',
        suffixes=(None, '_y')
    )

    filt = (potential_tns_in_wd['gnd_tn'].isna())

    LOG.info(f'Found {potential_tns_in_wd.loc[filt].shape[0]} potentially untagged Tn identifiers in Wikidata')
    potential_tns_in_wd.loc[filt].to_csv(
        f'{RESULTS_FOLDER}untagged_tns.tsv',
        sep='\t'
    )

    return potential_tns_in_wd.loc[filt]


# be aware of dump delay
# untagged redirects and Tns might also show up here; run those tasks first to clear the backlog
def query_items_with_untagged_invalid_identifiers() -> pd.DataFrame:
    current_gnd_values_in_wikidata = query_current_gnd_values_in_wikidata()
    LOG.info(f'Found {current_gnd_values_in_wikidata.shape[0]} GND identifiers in Wikidata')

    gnds_from_dump = load_latest_valid_identifier_dump()
    LOG.info(f'Found {gnds_from_dump.shape[0]} valid GND identifiers in latest dump')

    gnd_redirects = load_redirect_dump()
    LOG.info(f'Found {gnd_redirects.shape[0]} redirects in latest dump')

    potential_invalid_identifiers_in_wd = current_gnd_values_in_wikidata.merge(
        gnds_from_dump,
        how='left',
        left_on='gnd',
        right_on='GND',
        indicator=True
    )
    potential_invalid_identifiers_in_wd.drop(columns=['GND'], inplace=True)

    potential_invalid_identifiers_in_wd = potential_invalid_identifiers_in_wd.merge(
        gnd_redirects,
        how='left',
        left_on='gnd',
        right_on='redirect'
    )

    not_deprecated_filter = (potential_invalid_identifiers_in_wd['rank']!='DeprecatedRank') \
                          & (potential_invalid_identifiers_in_wd['_merge']=='left_only') \
                          & (potential_invalid_identifiers_in_wd['redirect'].isna())

    LOG.info(f'Found {potential_invalid_identifiers_in_wd.loc[not_deprecated_filter].shape[0]} potentially invalid identifiers in Wikidata.')
    potential_invalid_identifiers_in_wd.loc[not_deprecated_filter].to_csv(
        f'{RESULTS_FOLDER}untagged_invalid.tsv',
        sep='\t'
    )

    return potential_invalid_identifiers_in_wd.loc[not_deprecated_filter]


# be aware of dump delay
def query_items_with_incorrect_redirect_tags() -> pd.DataFrame:
    gnds_taggeds_as_redirects_in_wikidata = query_current_gnds_tagged_as_redirects_in_wikidata()
    LOG.info(f'Found {gnds_taggeds_as_redirects_in_wikidata.shape[0]} GND identifiers tagged as redirect in Wikidata')

    gnd_redirects = load_redirect_dump()
    LOG.info(f'Found {gnd_redirects.shape[0]} redirects in dump')

    incorrect_redirect_tags = gnds_taggeds_as_redirects_in_wikidata.merge(
        gnd_redirects,
        how='left',
        left_on='gnd',
        right_on='redirect'
    )
    filt = incorrect_redirect_tags['redirect'].isna()

    LOG.info(f'Found {incorrect_redirect_tags.loc[filt].shape[0]} GND identifiers that are potentially invalidly tagged as redirects')
    incorrect_redirect_tags.loc[filt].to_csv(
        f'{RESULTS_FOLDER}incorrect_redirects.tsv',
        sep='\t'
    )

    return incorrect_redirect_tags.loc[filt]


def query_items_with_incorrect_tn_tags() -> pd.DataFrame:
    gnds_taggeds_as_tn_in_wikidata = query_current_gnds_tagged_as_undifferentiated_in_wikidata()
    LOG.info(f'Found {gnds_taggeds_as_tn_in_wikidata.shape[0]} GND identifiers tagged as Tn in Wikidata')

    gnd_tns = load_latest_tn_dump()
    LOG.info(f'Found {gnd_tns.shape[0]} Tns in dump')

    incorrect_tn_tags = gnds_taggeds_as_tn_in_wikidata.merge(
        gnd_tns,
        how='left',
        left_on='gnd_tn',
        right_on='Tn'
    )
    filt = incorrect_tn_tags['Tn'].isna()

    LOG.info(f'Found {incorrect_tn_tags.loc[filt].shape[0]} GND identifiers that are potentially invalidly tagged as Tn')
    incorrect_tn_tags.loc[filt].to_csv(
        f'{RESULTS_FOLDER}incorrect_tns.tsv',
        sep='\t'
    )

    return incorrect_tn_tags.loc[filt]


# be aware of dump delay
def query_incorrectly_tagged_as_incorrect() -> pd.DataFrame:
    current_incorrect_gnd_values_in_wikidata = query_current_incorrect_gnd_values_in_wikidata()
    LOG.info(f'Found {current_incorrect_gnd_values_in_wikidata.shape[0]} GND identifiers in Wikidata marked as incorrect')

    valid_identifier_dump = load_latest_valid_identifier_dump()   
    LOG.info(f'Found {valid_identifier_dump.shape[0]} valid identifiers in latest dump')

    df = current_incorrect_gnd_values_in_wikidata.merge(
        valid_identifier_dump,
        how='left',
        left_on='gnd',
        right_on='GND'
    )
    filt = df['GND'].notna()

    LOG.info(f'Found {df.loc[filt].shape[0]} identifiers that are marked as incorrect, but should potentially not')
    df.loc[filt].to_csv(
        f'{RESULTS_FOLDER}incorrect_invalid.tsv',
        sep='\t'
    )

    return df.loc[filt]


def query_items_with_odd_ranking_situation() -> pd.DataFrame:
    df = query_odd_ranking()

    LOG.info(f'Found {df.shape[0]} items with oddly ranked GND claims')
    df.to_csv(
        f'{RESULTS_FOLDER}odd_ranking.tsv',
        sep='\t'
    )

    return df