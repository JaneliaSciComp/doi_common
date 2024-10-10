from operator import attrgetter
import jrc_common.jrc_common as JRC
from doi_common.doi_common import get_author_details, get_author_list, \
     get_doi_record, get_journal, get_publishing_date, get_title, \
     is_datacite, is_preprint, short_citation

DB = {}
dbconfig = JRC.get_config("databases")
dbo = attrgetter("dis.prod.read")(dbconfig)
DB['dis'] = JRC.connect_database(dbo)
COLL_DOIS = DB['dis'].dois
COLL_ORCID = DB['dis'].orcid


def test_get_doi_record():
    assert not get_doi_record('not a doi', COLL_DOIS)
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    assert rec
    assert isinstance(rec, dict)
    assert rec['title'] == ['A split-GAL4 driver line resource for Drosophila CNS cell types']


def test_get_author_details():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    auth_list = get_author_details(rec)
    assert isinstance(auth_list, list)
    assert auth_list[0]['is_first']
    assert auth_list[0]['paper_orcid'] == '0000-0003-0369-9788'
    assert auth_list[0]['family'] == 'Meissner'
    assert 'Janelia Research Campus, Howard Hughes Medical Institute' \
        in auth_list[0]['affiliations']
    auth_list = get_author_details(rec, COLL_ORCID)
    assert isinstance(auth_list, list)
    assert auth_list[0]['userIdO365'] == 'MEISSNERG@hhmi.org'


def test_get_author_list():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    auth_list = get_author_list(rec)
    assert isinstance(auth_list, str)
    assert auth_list.startswith('Meissner, GW')
    auth_list = get_author_list(rec, style='flylight')
    assert auth_list.startswith('Meissner, G. W.')
    auth_list = get_author_list(rec, returntype='list')
    assert isinstance(auth_list, list)
    assert auth_list[0] == 'Meissner, GW'


def test_get_journal():
    rec = get_doi_record('10.1002/cne.22542', COLL_DOIS)
    assert get_journal(rec) == 'J of Comparative Neurology. 2011; 519: 661-689'


def test_get_publishing_date():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    pdate = get_publishing_date(rec)
    assert pdate == '2024-07-30'


def test_get_title():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    title = get_title(rec)
    assert title == 'A split-GAL4 driver line resource for Drosophila CNS cell types'


def test_is_datacite():
    assert is_datacite('10.25378/janelia.23816295.v1')
    assert not is_datacite('10.7554/elife.98405')


def test_is_preprint():
    rec = get_doi_record('10.1101/2022.07.20.500311', COLL_DOIS)
    assert is_preprint(rec)
    rec = get_doi_record('10.1186/s12859-024-05732-7', COLL_DOIS)
    assert not is_preprint(rec)


def test_short_citation():
    assert short_citation('10.7554/elife.98405') == 'Meissner et al. 2024'
