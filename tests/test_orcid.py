from operator import attrgetter
import os
import jrc_common.jrc_common as JRC
from doi_common.doi_common import get_affiliations, get_author_details, get_doi_record, \
    get_name_combinations, get_project_map, get_single_author_details, is_janelia_author, \
    single_orcid_lookup

os.environ['CONFIG_SERVER_URL'] = 'https://config.int.janelia.org/'
DB = {}
dbconfig = JRC.get_config("databases")
dbo = attrgetter("dis.prod.read")(dbconfig)
DB['dis'] = JRC.connect_database(dbo)
COLL_DOIS = DB['dis'].dois
COLL_ORCID = DB['dis'].orcid
COLL_PM = DB['dis'].project_map


def test_get_affiliations():
    person_rec = JRC.call_people_by_id('RUBING@HHMI.ORG')
    orcid_rec = single_orcid_lookup('0000-0001-8762-8703', COLL_ORCID)
    del orcid_rec['affiliations']
    get_affiliations(person_rec, orcid_rec)
    assert 'Gerry Rubin Lab' in orcid_rec['affiliations']


def test_get_author_details():
    rec = get_doi_record('10.1016/j.cell.2006.04.005', COLL_DOIS)
    auth_list = get_author_details(rec)
    assert isinstance(auth_list, list)
    assert isinstance(auth_list[0], dict)
    assert auth_list[0]['is_first']


def test_get_name_combinations():
    person_rec = JRC.call_people_by_id('RUBING@HHMI.ORG')
    orcid_rec = single_orcid_lookup('0000-0001-8762-8703', COLL_ORCID)
    orcid_rec['family'] = ['Rubin']
    orcid_rec['given'] = ['Gerald']
    get_name_combinations(person_rec, orcid_rec)
    assert 'Gerry' in orcid_rec['given']


def test_get_single_author_details():
    rec = single_orcid_lookup('0000-0001-8762-8703', COLL_ORCID)
    details = get_single_author_details(rec, COLL_ORCID)
    assert isinstance(details, dict)
    assert details['in_database']


def test_is_janelia_author():
    pmap = get_project_map(COLL_PM)
    rec = get_doi_record('10.1016/j.cell.2006.04.005', COLL_DOIS)
    assert is_janelia_author(rec['author'][0], COLL_ORCID, pmap)


def test_single_orcid_lookup():
    rec = single_orcid_lookup('0000-0001-8762-8703', COLL_ORCID)
    assert isinstance(rec, dict)
    assert rec['family'] == ['Rubin']