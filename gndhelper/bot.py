import logging

import pywikibot as pwb

from .config import REPO, SIMULATE
from .config import PID_GND, PID_DEPRECATION, RANK_PREFERRED, RANK_NORMAL, RANK_DEPRECATED

LOG = logging.getLogger(__name__)


def raise_to_normal_rank(qid:str, gnd:str) -> None:
    item = pwb.ItemPage(REPO, qid)
    item.get()

    if not item.claims or PID_GND not in item.claims:
        return

    for claim in item.claims[PID_GND]:
        if claim.getTarget() != gnd:
            continue

        if claim.getRank() != RANK_DEPRECATED:
            continue

        if SIMULATE is not True:
            claim.changeRank(RANK_NORMAL)

        LOG.info(f'Raised rank to "{RANK_NORMAL}" in {qid}, value {gnd}')


def set_to_deprecated_rank(qid:str, gnd_to_deprecate:str) -> None:
    item = pwb.ItemPage(REPO, qid)
    item.get()

    if not item.claims or PID_GND not in item.claims:
        return

    for claim in item.claims[PID_GND]:
        if claim.getTarget() != gnd_to_deprecate:
            continue

        if claim.getRank() == RANK_DEPRECATED:
            continue

        if SIMULATE is not True:
            claim.changeRank(RANK_DEPRECATED)

        LOG.info(f'Set "{RANK_DEPRECATED}" rank in {qid}, value {gnd_to_deprecate}')


def add_deprecation_qualifier(qid:str, gnd_to_deprecate:str, deprecation_value:str, \
                              summary:str) -> None:
    item = pwb.ItemPage(REPO, qid)
    item.get()

    if not item.claims or PID_GND not in item.claims:
        return

    for claim in item.claims[PID_GND]:
        if claim.getTarget() != gnd_to_deprecate:
            continue

        qualifier_target = pwb.ItemPage(REPO, deprecation_value)

        if claim.has_qualifier(PID_DEPRECATION, qualifier_target):
            continue

        deprecation_qualifier_claim = pwb.Claim(REPO, PID_DEPRECATION)
        deprecation_qualifier_claim.setTarget(value=qualifier_target)

        if SIMULATE is not True:
            claim.addQualifier(
                deprecation_qualifier_claim,
                summary=summary
            )

        LOG.info(f'Added deprecation qualifier {deprecation_value} in {qid}, value {gnd_to_deprecate}')


def remove_deprecation_qualifier(qid:str, gnd:str, deprecation_value:str, \
                                 summary:str) -> None:
    item = pwb.ItemPage(REPO, qid)
    item.get()

    if not item.claims or PID_GND not in item.claims:
        return

    for claim in item.claims[PID_GND]:
        if claim.getTarget() != gnd:
            continue

        for pid in claim.qualifiers:
            if pid != PID_DEPRECATION:
                continue

            for qualifier in claim.qualifiers.get(pid):
                if qualifier.getTarget().title() != deprecation_value:
                    continue

                if SIMULATE is not True:
                    claim.removeQualifier(
                        qualifier,
                        summary=summary
                    )

                LOG.info(f'Removed deprecation qualifier {deprecation_value} in {qid}, value {gnd}')


def add_redirect_target(qid:str, target:str) -> None:
    item = pwb.ItemPage(REPO, qid)
    item.get()

    for claim in item.claims[PID_GND]:
        if claim.getTarget() == target:
            return  # do not add a value that is already there

    new_claim = pwb.Claim(REPO, PID_GND)
    new_claim.setTarget(target)

    if SIMULATE is not True:
        item.addClaim(
            new_claim,
            summary='add missing GND redirect target'
        )

    LOG.info(f'Added missing redirect target GND to {qid}')


# lower rank from preferred to normal, but only if there are preferred 
# but no normal rank claims
def normalize_ranking(qid:str) -> None:
    item = pwb.ItemPage(REPO, qid)
    item.get()

    ranking_counts = {
        RANK_DEPRECATED : 0,
        RANK_NORMAL : 0,
        RANK_PREFERRED : 0
    }
    changed_claims = 0

    for claim in item.claims[PID_GND]:
        rank = claim.getRank()
        if rank not in ranking_counts:
            LOG.debug(claim)
            continue
        ranking_counts[rank] += 1

    if ranking_counts[RANK_PREFERRED] == 0:
        return
    if ranking_counts[RANK_NORMAL] > 0:
        return

    for claim in item.claims[PID_GND]:
        if claim.getRank() != RANK_PREFERRED:
            continue

        changed_claims += 1

        if SIMULATE is not True:
            claim.changeRank(RANK_NORMAL)

    LOG.info(f'Changed ranking of {changed_claims:d} statements in {qid}')
