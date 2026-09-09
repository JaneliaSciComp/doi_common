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


class FakeOrcid:
    def __init__(self, docs):
        self.docs = docs

    def find(self, _query=None, _projection=None):
        return list(self.docs)


class FakeDois:
    def __init__(self, docs):
        self.docs = docs

    def find(self, _query=None, _projection=None):
        return [d for d in self.docs if d.get('jrc_author') is not None]


def mismatches(authors, roster, **kw):
    from doi_common.doi_common import name_mismatches
    dois = [{'doi': 'd/1', 'jrc_author': ['1'],
             'author': [{'given': g, 'family': f} for g, f in authors]}]
    people = [{'given': [g], 'family': [f], 'employeeId': e, **extra}
              for g, f, e, extra in roster]
    return name_mismatches(FakeDois(dois), FakeOrcid(people), **kw)


class TestNameMismatches:
    ROSTER = [('Joshua T.', 'Dudman', 'J0090', {}),
              ('Ann', 'M Hermundstad', '111', {}),
              ('Loren', 'Looger', '222', {'alumni': True})]

    def test_an_exact_name_is_not_reported(self):
        assert mismatches([('Joshua T.', 'Dudman')], self.ROSTER) == []

    def test_a_misspelling_is_reported_as_spelling(self):
        out = mismatches([('Joshua T.', 'Dudmann')], self.ROSTER)
        assert len(out) == 1
        assert out[0]['kind'] == 'spelling'
        assert out[0]['roster'] == 'Joshua T. Dudman'
        assert out[0]['employeeId'] == 'J0090'
        assert 95 < out[0]['score'] < 100

    def test_a_doubled_space_is_reported_as_punctuation(self):
        out = mismatches([('Ann ', ' M Hermundstad')], self.ROSTER)
        assert out and out[0]['kind'] == 'punctuation'
        assert out[0]['score'] == 100.0

    def test_case_and_accents_are_not_reported(self):
        # the author matcher's collation resolves these for itself
        assert mismatches([('JOSHUA T.', 'DUDMAN')], self.ROSTER) == []

    def test_alumni_are_flagged_not_hidden(self):
        out = mismatches([('Loren', 'Loogerr')], self.ROSTER)
        assert out and out[0]['alumni'] is True

    def test_an_unrelated_name_is_not_reported(self):
        assert mismatches([('Wolfgang', 'Amadeus')], self.ROSTER) == []

    def test_the_cutoff_is_honoured(self):
        loose = mismatches([('Joshua T.', 'Dudmenn')], self.ROSTER, cutoff=60)
        tight = mismatches([('Joshua T.', 'Dudmenn')], self.ROSTER, cutoff=99)
        assert loose and not tight

    def test_the_default_cutoff_rejects_a_one_letter_difference_in_a_short_name(self):
        # "Michelle Hu" vs "Michelle Du" scores about 91: two people, not a typo
        roster = [('Michelle', 'Du', '900', {})]
        assert mismatches([('Michelle', 'Hu')], roster) == []
        assert mismatches([('Michelle', 'Hu')], roster, cutoff=88) != []

    def test_the_default_cutoff_keeps_a_typo_in_a_longer_name(self):
        assert mismatches([('Joshua T.', 'Dudmann')], self.ROSTER) != []

    def test_results_are_ordered_by_kind_then_score(self):
        out = mismatches([('Joshua T.', 'Dudmann'), ('Ann ', ' M Hermundstad')], self.ROSTER)
        assert [r['kind'] for r in out] == ['punctuation', 'spelling']
