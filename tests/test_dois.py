import os
import re
from operator import attrgetter
import jrc_common.jrc_common as JRC
from doi_common.doi_common import get_author_details, get_author_list, \
     get_doi_record, get_journal, get_publishing_date, get_title, \
     acting_user, doi_api_url, get_abstract, \
     is_datacite, is_journal, is_preprint, is_version, short_citation

DB = {}
dbconfig = JRC.get_config("databases")
dbo = attrgetter("dis.prod.read")(dbconfig)
DB['dis'] = JRC.connect_database(dbo)
COLL_DOIS = DB['dis'].dois
COLL_ORCID = DB['dis'].orcid


def test_get_author_details():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    auth_list = get_author_details(rec)
    assert isinstance(auth_list, list)
    assert auth_list[0]['is_first']
    assert auth_list[0]['paper_orcid'] == '0000-0003-0369-9788'
    assert auth_list[0]['family'] == 'Meissner'
    # Affiliation strings are reworded by the publisher; match the institution,
    # not the full address.
    assert any('Janelia' in aff for aff in auth_list[0]['affiliations'])
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


def test_get_doi_record():
    assert not get_doi_record('not a doi', COLL_DOIS)
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    assert rec
    assert isinstance(rec, dict)
    # The title was revised from "CNS cell types" to "neuron types"; assert the
    # shape and the stable part of the string.
    assert isinstance(rec['title'], list) and rec['title']
    assert rec['title'][0].startswith('A split-GAL4 driver line resource for Drosophila')


def test_get_journal():
    rec = get_doi_record('10.1002/cne.22542', COLL_DOIS)
    # The journal name expanded from "J of" to "Journal of"; assert the parts
    # get_journal() assembles rather than the publisher's current abbreviation.
    journal = get_journal(rec)
    assert 'Comparative Neurology' in journal
    assert journal.endswith('2011; 519: 661-689')


def test_get_publishing_date():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    pdate = get_publishing_date(rec)
    # A new version moved this date; assert the format, which is what the
    # function guarantees.
    assert re.fullmatch(r'\d{4}-\d{2}-\d{2}', pdate)


def test_get_title():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    title = get_title(rec)
    assert isinstance(title, str)
    assert title.startswith('A split-GAL4 driver line resource for Drosophila')


def test_is_datacite():
    assert is_datacite('10.25378/janelia.23816295.v1')
    assert not is_datacite('10.7554/elife.98405')


def test_is_preprint():
    rec = get_doi_record('10.1101/2022.07.20.500311', COLL_DOIS)
    assert is_preprint(rec)
    rec = get_doi_record('10.1186/s12859-024-05732-7', COLL_DOIS)
    assert not is_preprint(rec)
    # Crossref types some SSRN postings as journal-article with no subtype, so
    # the prefix decides. Both typings must read as a preprint.
    rec = get_doi_record('10.2139/ssrn.3330557', COLL_DOIS)
    assert rec and rec.get('type') == 'journal-article'
    assert is_preprint(rec)
    rec = get_doi_record('10.2139/ssrn.3232155', COLL_DOIS)
    assert is_preprint(rec)
    # 10.1101 is Cold Spring Harbor Laboratory Press, which registers bioRxiv
    # and its own journals; a CSH Protocols article is not a preprint.
    rec = get_doi_record('10.1101/pdb.top78', COLL_DOIS)
    assert rec and not is_preprint(rec)


def test_short_citation():
    # The year follows the record's publishing date, which a new version moved.
    assert re.fullmatch(r'Meissner et al\. \d{4}', short_citation('10.7554/elife.98405'))


def test_get_title_needs_the_upper_case_doi_key():
    """get_title() decides Crossref vs DataCite by testing for the upper-case
    "DOI" key, so a projection that omits it sends every Crossref record down
    the DataCite branch and returns the literal string "No title". This has
    caused two separate display bugs, so the contract is pinned here."""
    full = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    assert 'DOI' in full
    assert get_title(full) != 'No title'
    # the same record without that one key
    stripped = {k: v for k, v in full.items() if k != 'DOI'}
    assert get_title(stripped) == 'No title'
    # DataCite records have no DOI key and must still resolve
    dc = get_doi_record('10.25378/janelia.23816295.v1', COLL_DOIS)
    assert 'DOI' not in dc
    assert get_title(dc) != 'No title'


def test_is_journal():
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    assert is_journal(rec)
    rec = get_doi_record('10.1101/2022.07.20.500311', COLL_DOIS)
    assert not is_journal(rec)


def test_journal_and_preprint_are_exclusive():
    """Crossref types some SSRN postings as journal-article with no subtype, so
    without a guard a record satisfies both predicates and gets counted twice."""
    for doi in ('10.2139/ssrn.3330557', '10.7554/elife.98405',
                '10.1101/2022.07.20.500311', '10.1101/pdb.top78'):
        rec = get_doi_record(doi, COLL_DOIS)
        assert rec, doi
        assert not (is_journal(rec) and is_preprint(rec)), doi


def test_is_version():
    rec = get_doi_record('10.25378/janelia.12106749.v2', COLL_DOIS)
    assert is_version(rec)
    rec = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    assert not is_version(rec)


def test_get_abstract_needs_the_upper_case_doi_key():
    """get_abstract() branches on the upper-case "DOI" key exactly as get_title()
    does, so a projection omitting it skips the Crossref path entirely and
    returns None. Same trap, second function - pinned so it is not rediscovered."""
    full = get_doi_record('10.7554/elife.98405', COLL_DOIS)
    assert 'DOI' in full and 'abstract' in full        # no network path taken
    assert get_abstract(full)
    stripped = {k: v for k, v in full.items() if k != 'DOI'}
    assert get_abstract(stripped) is None
    # DataCite records carry the abstract under descriptions instead
    dc = get_doi_record('10.25378/janelia.23816295.v1', COLL_DOIS)
    assert 'DOI' not in dc and 'descriptions' in dc
    assert get_abstract(dc)


def test_doi_api_url():
    doi = '10.7554/elife.98405'
    assert doi_api_url(doi, source='openalex').endswith(doi)
    assert doi_api_url(doi, source='unpaywall').startswith('https://api.unpaywall.org/v2/')
    # elife takes the article number out of the DOI
    assert doi_api_url(doi, source='elife').endswith('/98405')
    # arxiv strips the DOI prefix, case-insensitively
    assert doi_api_url('10.48550/arXiv.2301.02661',
                       source='arxiv').endswith('id_list=2301.02661')
    # elsevier switches the accept header on content type
    assert 'application/json' in doi_api_url(doi, source='elsevier')
    assert 'text/xml' in doi_api_url(doi, source='elsevier', content='xml')
    # an unknown source yields nothing rather than a malformed URL
    assert doi_api_url(doi, source='not-a-source') is None


def test_acting_user():
    """An unattended run records no user. Detection is by environment, so the
    variables are set and cleared around the assertions rather than depending on
    how the suite happens to be launched."""
    saved = {k: os.environ.pop(k, None)
             for k in ('JENKINS_URL', 'BUILD_NUMBER', 'BUILD_TAG', 'CI')}
    try:
        assert acting_user()                     # interactive: a login name
        for var in ('JENKINS_URL', 'BUILD_NUMBER', 'BUILD_TAG', 'CI'):
            os.environ[var] = 'set'
            assert acting_user() is None, var
            del os.environ[var]
    finally:
        for key, val in saved.items():
            if val is not None:
                os.environ[key] = val
