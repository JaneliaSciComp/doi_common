from operator import attrgetter
import jrc_common.jrc_common as JRC
from doi_common.doi_common import get_single_author_details, single_orcid_lookup

DB = {}
dbconfig = JRC.get_config("databases")
dbo = attrgetter("dis.prod.read")(dbconfig)
DB['dis'] = JRC.connect_database(dbo)
COLL_ORCID = DB['dis'].orcid


def test_single_orcid_lookup():
    rec = single_orcid_lookup('0000-0001-8374-6008', COLL_ORCID)
    assert isinstance(rec, dict)
    assert rec['family'] == ['Svirskas']


def test_get_single_author_details():
    rec = single_orcid_lookup('0000-0001-8374-6008', COLL_ORCID)
    details = get_single_author_details(rec, COLL_ORCID)
    assert isinstance(details, dict)
    assert details['in_database']
