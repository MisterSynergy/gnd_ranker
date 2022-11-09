import logging
from time import sleep

from rdflib import Graph
from rdflib.plugins.parsers.notation3 import BadSyntax
import requests

from .config import USER_AGENT, GND_RDF_URL_PATTERN, GND_RDF_FILE, GND_SLEEP


LOG = logging.getLogger(__name__)


def is_valid_gnd_identifier(gnd:str) -> bool:
    sleep(GND_SLEEP)

    response = requests.head(
        url=GND_RDF_URL_PATTERN.format(gnd=gnd),
        headers={ 'User-Agent' : USER_AGENT }
    )

    return (response.status_code != 404)


def is_tn_identifier(gnd:str) -> bool:
    sleep(GND_SLEEP)

    response = requests.get(
        url=GND_RDF_FILE.format(gnd=gnd),
        headers={ 'User-Agent' : USER_AGENT }
    )

    g = Graph()
    try:
        g.parse(
            data=response.text,
            format='ttl'
        )
    except BadSyntax as exception:
        raise RuntimeWarning(f'Cannot parse RDF serialization of {gnd} as graph;' \
                             f' status_code: {response.status_code}') from exception

    if len(g) == 0:
        return False

    query = """
    SELECT DISTINCT ?entity WHERE {
        ?entity rdf:type gndo:UndifferentiatedPerson .
    }
    """

    query_result = g.query(query)

    for row in query_result:
        entity = row[0]
        if entity.toPython() == GND_RDF_URL_PATTERN.format(gnd=gnd):
            return True

    return False


def get_redirect_target(redirect_gnd:str) -> str:
    sleep(GND_SLEEP)

    response = requests.head(
        url=GND_RDF_URL_PATTERN.format(gnd=redirect_gnd),
        headers={ 'User-Agent' : USER_AGENT }
    )

    if response.status_code != 301 or 'Location' not in response.headers:
        raise RuntimeWarning(f'{redirect_gnd} is likely not a redirect')

    return response.headers['Location'][5:]
