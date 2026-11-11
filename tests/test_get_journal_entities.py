''' get_journal: names are HTML-unescaped and their whitespace is collapsed.

    Crossref sends some container titles with the entity still in them, and both
    update_dois.py (dois) and update_ack_records.py (external_dois) store
    get_journal(rec, name_only=True) as jrc_journal, so 79 + 31 stored journal names
    read "Genes &amp; Development". Hermetic: the records are built here, with no
    database or network.
'''

from doi_common.doi_common import clean_journal_name, get_journal


def _crossref(**fields):
    rec = {'DOI': '10.1/x', 'published': {'date-parts': [[2024, 5, 1]]}}
    rec.update(fields)
    return rec


def _datacite(**fields):
    rec = {'doi': '10.5281/zenodo.1', 'publicationYear': 2024,
           'dates': [{'date': '2024-05-01', 'dateType': 'Issued'}]}
    rec.update(fields)
    return rec


def test_a_crossref_container_title_is_unescaped():
    rec = _crossref(**{'container-title': ['Genes &amp; Development']})
    assert get_journal(rec, name_only=True) == 'Genes & Development'


def test_the_short_container_title_fallback_is_unescaped_too():
    rec = _crossref(**{'short-container-title': ['Genes &amp; Dev.']})
    assert get_journal(rec, name_only=True) == 'Genes & Dev.'


def test_the_full_form_carries_the_unescaped_name():
    rec = _crossref(**{'container-title': ['Arthritis &amp; Rheumatology'],
                       'volume': '76', 'page': '1-6'})
    assert get_journal(rec) == 'Arthritis & Rheumatology. 2024; 76: 1-6'


def test_a_datacite_publisher_is_unescaped():
    rec = _datacite(publisher='Cold Spring Harbor &amp; Co')
    assert get_journal(rec, name_only=True) == 'Cold Spring Harbor & Co'


def test_a_datacite_publisher_object_is_returned_unchanged():
    ''' Newer DataCite responses can carry publisher as an object. '''
    pub = {'name': 'Zenodo'}
    assert get_journal(_datacite(publisher=pub), name_only=True) == pub


def test_names_without_entities_are_unchanged():
    for name in ('Nature', 'Genes & Development', 'Zeitschrift für Naturforschung'):
        assert get_journal(_crossref(**{'container-title': [name]}), name_only=True) == name


def test_only_one_level_of_escaping_is_removed():
    ''' A doubly escaped name is left with one level, not guessed at. '''
    rec = _crossref(**{'container-title': ['A &amp;amp; B']})
    assert get_journal(rec, name_only=True) == 'A &amp; B'


def test_a_line_break_inside_the_name_becomes_a_space():
    ''' ACS sent "ACS Central\\nScience" for 10.1021/acscentsci.7b00247. '''
    rec = _crossref(**{'container-title': ['ACS Central\nScience']})
    assert get_journal(rec, name_only=True) == 'ACS Central Science'


def test_an_entity_and_a_line_break_together():
    ''' 10.1021/cbmi.6c00054 had both. '''
    rec = _crossref(**{'container-title': ['Chemical &amp; Biomedical\nImaging']})
    assert get_journal(rec, name_only=True) == 'Chemical & Biomedical Imaging'


def test_runs_of_whitespace_and_the_ends_are_cleaned():
    assert clean_journal_name('  Cell\t  Reports \r\n') == 'Cell Reports'
    assert clean_journal_name('Cell&#160;Reports') == 'Cell Reports'     # no-break space


def test_a_datacite_publisher_is_cleaned_too():
    assert get_journal(_datacite(publisher='Zenodo\n'), name_only=True) == 'Zenodo'


def test_clean_journal_name_passes_through_anything_but_a_string():
    for value in (None, {'name': 'Zenodo'}, ['Nature']):
        assert clean_journal_name(value) == value
