''' Tests for authorship_gaps. Uses a fake collection rather than the database,
    so unlike the other files here these do not drift with the data.
'''

import pytest

from doi_common.doi_common import authorship_gaps


class FakeColl:
    ''' Returns the given docs from find(), ignoring the projection. '''
    def __init__(self, docs):
        self.docs = docs

    def find(self, _query=None, _projection=None):
        return list(self.docs)


def gaps(docs, relation='both'):
    return authorship_gaps(FakeColl(docs), relation)


class TestPreprintRelation:
    def test_partner_credits_someone_this_doi_does_not(self):
        out = gaps([{'doi': 'j/1', 'jrc_author': ['A'], 'jrc_preprint': ['p/1']},
                    {'doi': 'p/1', 'jrc_author': ['A', 'B']}])
        assert out == [{'doi': 'j/1', 'relation': 'preprint',
                        'missing': ['B'], 'partners': ['p/1']}]

    def test_no_gap_when_they_agree(self):
        assert gaps([{'doi': 'j/1', 'jrc_author': ['A'], 'jrc_preprint': ['p/1']},
                     {'doi': 'p/1', 'jrc_author': ['A']}]) == []

    def test_extra_authors_here_are_not_a_gap_for_this_doi(self):
        # this DOI credits more; the gap belongs to the partner, and is only
        # reported when the partner is the record being examined
        out = gaps([{'doi': 'j/1', 'jrc_author': ['A', 'B'], 'jrc_preprint': ['p/1']},
                    {'doi': 'p/1', 'jrc_author': ['A']}])
        assert out == []

    def test_a_scalar_relation_is_accepted(self):
        out = gaps([{'doi': 'j/1', 'jrc_author': [], 'jrc_preprint': 'p/1'},
                    {'doi': 'p/1', 'jrc_author': ['B']}])
        assert out[0]['missing'] == ['B']

    def test_an_unknown_partner_is_ignored(self):
        assert gaps([{'doi': 'j/1', 'jrc_author': [], 'jrc_preprint': ['nope/1']}]) == []

    def test_partners_are_merged_across_several_links(self):
        out = gaps([{'doi': 'j/1', 'jrc_author': [], 'jrc_preprint': ['p/1', 'p/2']},
                    {'doi': 'p/1', 'jrc_author': ['B']},
                    {'doi': 'p/2', 'jrc_author': ['C']}])
        assert out[0]['missing'] == ['B', 'C']
        assert out[0]['partners'] == ['p/1', 'p/2']


class TestVersionRelation:
    def test_a_version_missing_someone_its_sibling_credits(self):
        out = gaps([{'doi': 'd/1.v1', 'jrc_author': ['A']},
                    {'doi': 'd/1.v2', 'jrc_author': ['A', 'B']}])
        assert out == [{'doi': 'd/1.v1', 'relation': 'version',
                        'missing': ['B'], 'partners': ['d/1.v2']}]

    def test_the_stem_counts_as_a_member(self):
        out = gaps([{'doi': 'd/1', 'jrc_author': []},
                    {'doi': 'd/1.v1', 'jrc_author': ['B']}])
        assert [g['doi'] for g in out] == ['d/1']

    def test_a_lone_version_has_nothing_to_compare_with(self):
        assert gaps([{'doi': 'd/1.v1', 'jrc_author': ['A']}]) == []

    def test_similar_dois_are_not_grouped(self):
        # only a trailing .vN makes a version; a DOI merely containing "v" does not
        assert gaps([{'doi': 'd/1v2', 'jrc_author': ['A']},
                     {'doi': 'd/1v3', 'jrc_author': ['A', 'B']}]) == []

    def test_every_member_short_of_the_union_is_reported(self):
        out = gaps([{'doi': 'd/1.v1', 'jrc_author': ['A']},
                    {'doi': 'd/1.v2', 'jrc_author': ['B']}])
        assert {g['doi']: g['missing'] for g in out} == {'d/1.v1': ['B'], 'd/1.v2': ['A']}


class TestRelationFilter:
    DOCS = [{'doi': 'j/1', 'jrc_author': [], 'jrc_preprint': ['p/1']},
            {'doi': 'p/1', 'jrc_author': ['B']},
            {'doi': 'd/1.v1', 'jrc_author': []},
            {'doi': 'd/1.v2', 'jrc_author': ['C']}]

    def test_both_by_default(self):
        assert {g['relation'] for g in gaps(self.DOCS)} == {'preprint', 'version'}

    def test_preprint_only(self):
        assert {g['relation'] for g in gaps(self.DOCS, 'preprint')} == {'preprint'}

    def test_version_only(self):
        assert {g['relation'] for g in gaps(self.DOCS, 'version')} == {'version'}

    def test_an_unknown_relation_is_rejected(self):
        with pytest.raises(Exception):
            gaps(self.DOCS, 'nonsense')

    def test_results_are_ordered(self):
        out = gaps(self.DOCS)
        assert out == sorted(out, key=lambda g: (g['doi'], g['relation']))
