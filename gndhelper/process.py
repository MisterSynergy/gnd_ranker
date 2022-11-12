import logging

import pandas as pd

from .config import LIMIT, QID_REDIRECT, QID_UNDIFFERENTIATED, QID_WITHDRAWN
from .verify import get_redirect_target, is_valid_gnd_identifier, is_tn_identifier
from .bot import raise_to_normal_rank, set_to_deprecated_rank, add_deprecation_qualifier, \
    add_redirect_target, normalize_ranking, remove_deprecation_qualifier


LOG = logging.getLogger(__name__)


def process_items_with_untagged_redirects(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):
        try:
            target = get_redirect_target(row.redirect)
        except RuntimeWarning:  # not a redirect then
            continue

        if target == row.redirect:
            continue

        LOG.info(f'Found {row.item} has GND "{row.redirect}" with rank {row.rank}; redirect target: {target}')

        set_to_deprecated_rank(
            row.item,
            row.redirect
        )
        add_deprecation_qualifier(
            row.item,
            row.redirect,
            QID_REDIRECT,
            f'GND identifier {row.redirect} redirects to "{target}"'
        )

        if target != row.target:  # redirect target in dump different from URL redirect target; ignore
            LOG.info(f'Redirect target different in dump ({row.target}) and from URL ({target}); do not add redirect target')
            continue

        add_redirect_target(
            row.item,
            target
        )

        if LIMIT is not None and i >= LIMIT:
            break


def process_items_with_untagged_tns(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):
        is_tn = is_tn_identifier(row.gnd)

        LOG.info(f'Found: {row.item} has GND "{row.gnd}" with rank {row.rank}; GND is Tn: {is_tn}')

        if is_tn is not True:
            continue

        set_to_deprecated_rank(
            row.item,
            row.gnd
        )
        add_deprecation_qualifier(
            row.item,
            row.gnd,
            QID_UNDIFFERENTIATED,
            'value has Tn type and was removed from GND'
        )

        if LIMIT is not None and i >= LIMIT:
            break


def process_items_with_untagged_invalid_identifiers(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):
        gnd_is_valid = is_valid_gnd_identifier(row.gnd)

        LOG.info(f'Found: {row.item} has GND "{row.gnd}" with rank {row.rank}; GND is valid: {gnd_is_valid}')

        if gnd_is_valid is not False:
            continue

        set_to_deprecated_rank(
            row.item,
            row.gnd
        )
        add_deprecation_qualifier(
            row.item,
            row.gnd,
            QID_WITHDRAWN,
            'URL for identifier does not resolve (HTTP status code 404)'
        )

        if LIMIT is not None and i >= LIMIT:
            break


def process_items_with_identifiers_incorrectly_tagged_as_redirect(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):
        try:
            _ = get_redirect_target(row.gnd)
        except RuntimeWarning:  # not a redirect then
            LOG.info(f'Found {row.item} has GND "{row.gnd}" with rank {row.rank}')

            raise_to_normal_rank(
                row.item,
                row.gnd
            )
            remove_deprecation_qualifier(
                row.item,
                row.gnd,
                QID_REDIRECT,
                'remove deprecation reason, since the URL does not redirect'
            )
        else:
            LOG.info(f'Found {row.item} has GND "{row.gnd}" with rank {row.rank}: GND is a redirect')

        if LIMIT is not None and i >= LIMIT:
            break


def process_items_with_identifiers_incorrectly_tagged_as_tn(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):
        is_tn = is_tn_identifier(row.gnd_tn)

        LOG.info(f'Found: {row.item} has GND "{row.gnd_tn}" with rank {row.rank}; GND is Tn: {is_tn}')

        if is_tn is True:
            continue

        raise_to_normal_rank(
            row.item,
            row.gnd_tn
        )
        remove_deprecation_qualifier(
            row.item,
            row.gnd_tn,
            QID_UNDIFFERENTIATED,
            'remove deprecation reason, since the identifier is not of undifferentiated/Tn type'
        )

        if LIMIT is not None and i >= LIMIT:
            break


def process_items_with_identifiers_incorrectly_tagged_as_invalid(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):

        gnd_is_valid = is_valid_gnd_identifier(row.gnd)

        LOG.info(f'Found: {row.item} has GND "{row.gnd}" with rank {row.rank}; GND is valid: {gnd_is_valid}')

        if gnd_is_valid is False:
            continue

        raise_to_normal_rank(
            row.item,
            row.gnd
        )
        remove_deprecation_qualifier(
            row.item,
            row.gnd,
            QID_WITHDRAWN,
            'remove deprecation reason, since the URL for the identifier does resolve with a non-404 status code'
        )

        if LIMIT is not None and i >= LIMIT:
            break


def process_items_with_odd_ranking_situation(df:pd.DataFrame) -> None:
    for i, row in enumerate(df.itertuples(), start=1):
        normalize_ranking(row.item)

        if LIMIT is not None and i >= LIMIT:
            break