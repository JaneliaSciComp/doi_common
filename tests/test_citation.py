''' Tests for the formatted-citation helpers. Unlike the other files here these
    touch neither the database nor the network: _clean_citation is pure, and
    get_citation is exercised with requests.get replaced, so the registrar split
    can be asserted without asking either formatter for anything.
'''

from unittest.mock import patch
import types

from doi_common.doi_common import CITATION_STYLES, _clean_citation, get_bibtex, \
     get_citation


class TestCleanCitation:
    ''' The three artifacts the formatters emit. '''

    def test_strips_a_numbered_bibliography_marker(self):
        assert _clean_citation("1.Aaron J, Chew T-L. Title.") == "Aaron J, Chew T-L. Title."

    def test_strips_a_bracketed_marker(self):
        assert _clean_citation("[1]F. Bhinderwala et al., Title.") \
            == "F. Bhinderwala et al., Title."

    def test_leaves_a_citation_that_merely_starts_with_a_digit_alone(self):
        # "2026 in review." is text, not a bibliography number
        assert _clean_citation("2026 in review. Journal.") == "2026 in review. Journal."

    def test_removes_the_empty_editor_placeholder(self):
        # american-medical-association emits this for every Crossref DOI, even
        # one whose record has no editor key at all
        assert _clean_citation("Broad adoption. , ed. Journal of Microscopy. 2025;298.") \
            == "Broad adoption. Journal of Microscopy. 2025;298."

    def test_keeps_a_genuine_editor(self):
        assert ", ed." in _clean_citation("Smith J, ed. Handbook of Things. 2020.")

    def test_strips_html_tags(self):
        assert _clean_citation("<i>MouseLight Neuron AA0340</i> [Dataset].") \
            == "MouseLight Neuron AA0340 [Dataset]."

    def test_unescapes_entities(self):
        assert _clean_citation("Chandrashekar, J., &amp; Spruston Lab.") \
            == "Chandrashekar, J., & Spruston Lab."

    def test_collapses_whitespace_and_trims(self):
        assert _clean_citation("  Aaron J.\n\n  Journal.  ") == "Aaron J. Journal."


class TestGetCitation:
    ''' Which formatter is asked, and what a failure returns. '''

    @staticmethod
    def _call(doi, style='apa', datacite=False, status=200, text='1.A citation.',
              headers=None):
        resp = types.SimpleNamespace(
            status_code=status, text=text, encoding='ISO-8859-1',
            headers=headers if headers is not None else {'content-type': 'text/x-bibliography'})
        with patch('doi_common.doi_common.is_datacite', return_value=datacite), \
             patch('doi_common.doi_common.requests.get', return_value=resp) as get:
            out = get_citation(doi, style)
        return out, (get.call_args[0][0] if get.call_args else None), resp

    def test_a_crossref_doi_asks_crossref(self):
        _, url, _ = self._call('10.1111/jmi.13400')
        assert url == 'https://api.crossref.org/works/10.1111/jmi.13400/transform'

    def test_a_datacite_doi_asks_datacite(self):
        _, url, _ = self._call('10.25378/janelia.7613747', datacite=True)
        assert url == \
            'https://api.datacite.org/text/x-bibliography/10.25378/janelia.7613747'

    def test_the_style_is_sent_in_the_accept_header(self):
        resp = types.SimpleNamespace(status_code=200, text='x', encoding='utf-8',
                                     headers={'content-type': 'text/x-bibliography'})
        with patch('doi_common.doi_common.is_datacite', return_value=False), \
             patch('doi_common.doi_common.requests.get', return_value=resp) as get:
            get_citation('10.1/a', 'ama')
        assert get.call_args[1]['headers']['Accept'] == \
            'text/x-bibliography; style=american-medical-association'

    def test_the_result_is_cleaned(self):
        out, _, _ = self._call('10.1/a', text='1.A citation.')
        assert out == 'A citation.'

    def test_utf8_is_forced_when_no_charset_is_declared(self):
        # Crossref sends text/x-bibliography with no charset, so requests would
        # otherwise decode as ISO-8859-1 and mangle accented names
        _, _, resp = self._call('10.1/a')
        assert resp.encoding == 'utf-8'

    def test_a_declared_charset_is_respected(self):
        _, _, resp = self._call(
            '10.1/a', datacite=True,
            headers={'content-type': 'text/x-bibliography; charset=utf-8'})
        assert resp.encoding == 'ISO-8859-1'   # left as requests worked it out

    def test_an_unknown_style_returns_empty(self):
        assert get_citation('10.1/a', 'nosuchstyle') == ''

    def test_an_empty_doi_returns_empty(self):
        assert get_citation('', 'apa') == ''

    def test_a_non_200_returns_empty(self):
        out, _, _ = self._call('10.1/a', status=406)
        assert out == ''

    def test_a_network_failure_returns_empty(self):
        with patch('doi_common.doi_common.is_datacite', return_value=False), \
             patch('doi_common.doi_common.requests.get', side_effect=OSError('down')):
            assert get_citation('10.1/a', 'apa') == ''

    def test_every_offered_style_maps_to_a_slug(self):
        assert set(CITATION_STYLES) == {'apa', 'ama', 'nature', 'cell', 'chicago'}
        assert all(v and isinstance(v, str) for v in CITATION_STYLES.values())


class TestGetBibtex:
    ''' BibTeX now comes from whichever registrar owns the DOI. '''

    @staticmethod
    def _call(doi, datacite=False, status=200, text='@article{x}', headers=None):
        resp = types.SimpleNamespace(
            status_code=status, text=text, encoding='ISO-8859-1',
            headers=headers if headers is not None else {'content-type': 'application/x-bibtex'})
        with patch('doi_common.doi_common.is_datacite', return_value=datacite), \
             patch('doi_common.doi_common.requests.get', return_value=resp) as get:
            out = get_bibtex(doi)
        return out, (get.call_args[0][0] if get.call_args else None), resp

    def test_a_crossref_doi_uses_the_transform_endpoint(self):
        _, url, _ = self._call('10.1111/jmi.13400')
        assert url == \
            'https://api.crossref.org/works/10.1111/jmi.13400/transform/application/x-bibtex'

    def test_a_datacite_doi_uses_datacites_bibtex_route(self):
        _, url, _ = self._call('10.25378/janelia.7613747', datacite=True)
        assert url == \
            'https://api.datacite.org/application/x-bibtex/10.25378/janelia.7613747'

    def test_the_result_is_stripped_but_not_otherwise_touched(self):
        # BibTeX is a record, not prose: the citation cleanups must not run on it
        out, _, _ = self._call('10.1/a', text='  @misc{a, title = {1. <i>x</i> &amp; y}}  ')
        assert out == '@misc{a, title = {1. <i>x</i> &amp; y}}'

    def test_utf8_is_forced_when_no_charset_is_declared(self):
        _, _, resp = self._call('10.1/a')
        assert resp.encoding == 'utf-8'

    def test_an_empty_doi_returns_empty(self):
        assert get_bibtex('') == ''

    def test_a_non_200_returns_empty(self):
        out, _, _ = self._call('10.1/a', status=404)
        assert out == ''

    def test_a_network_failure_returns_empty(self):
        with patch('doi_common.doi_common.is_datacite', return_value=False), \
             patch('doi_common.doi_common.requests.get', side_effect=OSError('down')):
            assert get_bibtex('10.1/a') == ''
