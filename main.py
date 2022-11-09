import logging
import logging.config

logging.config.fileConfig('logging.conf')

from gndhelper import *


def main() -> None:
    if UNTAGGED_TN is True:
        process_items_with_untagged_tns(query_items_with_untagged_tns())

    if UNTAGGED_REDIRECTS is True:
        process_items_with_untagged_redirects(query_items_with_untagged_redirects())

    if UNTAGGED_INVALID is True:
        process_items_with_untagged_invalid_identifiers(query_items_with_untagged_invalid_identifiers())

    if INCORRECTLY_TN is True:
        process_items_with_identifiers_incorrectly_tagged_as_tn(query_items_with_incorrect_tn_tags())

    if INCORRECTLY_REDIRECT is True:
        process_items_with_identifiers_incorrectly_tagged_as_redirect(query_items_with_incorrect_redirect_tags())

    if INCORRECTLY_INVALID is True:
        process_items_with_identifiers_incorrectly_tagged_as_invalid(query_incorrectly_tagged_as_incorrect())

    if NORMALIZE_RANKING is True:
        process_items_with_odd_ranking_situation(query_items_with_odd_ranking_situation())


if __name__=='__main__':
    main()
