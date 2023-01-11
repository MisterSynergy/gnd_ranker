import pywikibot as pwb
from requests.utils import default_headers

# which jobs to run
UNTAGGED_TN = True
UNTAGGED_REDIRECTS = True
UNTAGGED_INVALID = True
INCORRECTLY_TN = True
INCORRECTLY_REDIRECT = True
INCORRECTLY_INVALID = True
NORMALIZE_RANKING = True

# constants
WD = 'http://www.wikidata.org/entity/'
WIKIBASE = 'http://wikiba.se/ontology#'
S = 'http://www.wikidata.org/entity/statement/'

PID_GND = 'P227'
PID_DEPRECATION = 'P2241'
QID_REDIRECT = 'Q45403344'
QID_UNDIFFERENTIATED = 'Q68648103'
QID_WITHDRAWN = 'Q21441764'
RANK_PREFERRED = 'preferred'
RANK_NORMAL = 'normal'
RANK_DEPRECATED = 'deprecated'

# query config
USER_AGENT = f'{default_headers()["User-Agent"]} (Wikidata bot by' \
              ' User:MisterSynergy; mailto:mister.synergy@yahoo.com)'
GND_RDF_URL_PATTERN = 'https://d-nb.info/gnd/{gnd}'
GND_RDF_FILE = 'https://d-nb.info/gnd/{gnd}/about/lds'
WDQS_ENDPOINT = 'https://query.wikidata.org/sparql'
WDQS_SLICE_LIMIT = 500000  # int
GND_SLEEP = 2  # int, seconds

# log dataframes
RESULTS_FOLDER = './output/'

# dump locations
#TN_DUMP should be static since no new ones are being issued
TN_DUMP = 'https://persondata.toolforge.org/data/Tns.txt.gz'

#VALID_GND_DUMP is managed by User:Wurgl; it is a list of valid GNDs extracted from a GND dump
VALID_GND_DUMP = 'https://persondata.toolforge.org/data/GNDs.txt.gz'

#REDIRECT_DUMP is created locally with "gnd dump parser" by User:MisterSynergy
REDIRECT_DUMP = './assets/redirects-latest.tsv'

# bot config
SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()
SIMULATE = False  # do not make any edits when True

# botjobs config
LIMIT = None  # int or None; items to process
