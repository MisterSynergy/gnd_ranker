from .config import UNTAGGED_TN, UNTAGGED_REDIRECTS, UNTAGGED_INVALID, INCORRECTLY_TN, \
    INCORRECTLY_REDIRECT, INCORRECTLY_INVALID, NORMALIZE_RANKING
from .gather import query_items_with_untagged_redirects, query_items_with_untagged_tns, \
    query_items_with_untagged_invalid_identifiers, query_items_with_incorrect_redirect_tags, \
    query_items_with_incorrect_tn_tags, query_incorrectly_tagged_as_incorrect, \
    query_items_with_odd_ranking_situation
from .process import process_items_with_untagged_redirects, process_items_with_untagged_tns, \
    process_items_with_odd_ranking_situation, process_items_with_untagged_invalid_identifiers, \
    process_items_with_identifiers_incorrectly_tagged_as_redirect, process_items_with_identifiers_incorrectly_tagged_as_tn, \
    process_items_with_identifiers_incorrectly_tagged_as_invalid
