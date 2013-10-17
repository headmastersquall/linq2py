#!/usr/bin/env python

import context
import unittest
from array import array
from linq2py import From


class FromTestCase(unittest.TestCase):
    """
    Test case for the From class.
    """

    def setUp(self):
        self.items = range(1, 11)

    def tearDown(self):
        pass

    def test_aggregate_calculateTheSumOfTheProvidedSequence(self):
        result = From(self.items).aggregate(
            lambda current, next_: current + next_)
        self.assertEquals(result, 55)

    def test_aggregate_calculateTheSumOfTheProvidedSequenceAndDoubleTheResult(self):
        result = From(self.items).aggregate(
            lambda current, next_: current + next_,
            resultfn=lambda result: result * 2)
        self.assertEquals(result, 110)

    def test_aggregate_calculateTheSumOfTheProvidedSequenceWithAnInitialValue(self):
        result = From(self.items).aggregate(
            lambda current, next_: current + next_,
            seed=10)
        self.assertEquals(result, 65)

    def test_all_returnsTrueWhenAllItemsEvalutateToTrue(self):
        self.assertTrue(From(self.items).all(lambda x: x % 1 == 0))

    def test_all_returnsTrueWhenAllItemsEvalutateToFalse(self):
        self.assertFalse(From(self.items).all(lambda x: x % 2 == 0))

    def test_any_returnsTrueWhenOneItemEvalutateToTrue(self):
        self.assertTrue(From(self.items).any(lambda x: x == 3))

    def test_any_returnsTrueWhenNoItemsEvalutateToTrue(self):
        self.assertFalse(From(self.items).any(lambda x: x == 15))

    def test_average_calculatesTheAverageValueInTheSequence(self):
        self.assertEquals(From(self.items).average(), 5)

    def test_cast_convertAllItemsToStrings(self):
        self.assertEquals(
            From(self.items).cast(str).tolist(),
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])

    def test_concat_combineAnotherSequenceWithTheExistingOne(self):
        self.assertEquals(
            From([1, 2, 3]).concat([4, 5, 6]).tolist(),
            [1, 2, 3, 4, 5, 6])

    def test_contains_returnsTrueIfTheItemExistsInTheSequence(self):
        self.assertTrue(From(self.items).contains(5))

    def test_count_returnsTheNumberOfItemsInTheSequence(self):
        self.assertEquals(From(self.items).count(), 10)

    def test_count_returnsTheNumberOfItemsInTheSequenceThatMatchGivenPredicate(self):
        self.assertEquals(From(self.items).count(lambda x: x > 5), 5)

    def test_defaultifempty_returnsNewFromWithProvidedValueIfSeqIsEmpty(self):
        self.assertEquals(
            From([]).defaultifempty([1, 2, 3]).tolist(),
            [1, 2, 3])

    def test_defaultifempty_returnsNewFromWithExistingSeqIfItContainsItems(self):
        self.assertEquals(
            From(self.items).defaultifempty([1, 2, 3]).tolist(),
            list(self.items))

    def test_distict_returnsUniqueSetOfItems(self):
        self.assertEquals(
            From([1, 1, 2, 3]).distict().tolist(),
            [1, 2, 3])

    def test_elementat_returnsItemLocatedAtTheProvidedIndexWhenSeqIsIndexable(self):
        self.assertEquals(
            From(self.items).elementat(2),
            3)

    def test_elementat_returnsItemLocatedAtTheProvidedIndexWhenSeqIsNotIndexable(self):
        self.assertEquals(
            From(x for x in self.items).elementat(2),
            3)

    def test_elementatordefault_returnsProperItemFromTheIndex(self):
        self.assertEquals(
            From(self.items).elementatordefault(2, 10),
            3)

    def test_elementatordefault_returnsDefaultIfIndexIsOutOfRange(self):
        self.assertEquals(
            From(self.items).elementatordefault(12, 10),
            10)

    def test_except_getAllItemsExceptTheOnesProvided(self):
        self.assertEquals(
            From(self.items).except_([3, 6, 9]).tolist(),
            [1, 2, 4, 5, 7, 8, 10])

    def test_except_getAllItemsExceptTheOnesInTheProvidedSequence(self):
        self.assertEquals(
            From(self.items).except_(iter([3, 6, 9])).tolist(),
            [1, 2, 4, 5, 7, 8, 10])

    def test_first_getsTheFirstItemInTheSequence(self):
        self.assertEquals(
            From(self.items).first(),
            1)

    def test_first_getsTheFirstItemInNonIndexableSequence(self):
        self.assertEquals(
            From(iter(self.items)).first(),
            1)

    def test_firstordefault_returnsDefaultValueWhenNoItemIsReturnedFromFirst(self):
        self.assertEquals(
            From([]).firstordefault(7),
            7)

    def test_groupby_properlyGroupsItems(self):
        groups = From([1, 3, 2, 1]).groupby().tolist()
        self.assertEquals(list(groups[0]), [1, [1, 1]])
        self.assertEquals(list(groups[1]), [3, [3]])
        self.assertEquals(list(groups[2]), [2, [2]])

    def test_groupby_properlyGroupsItemsWithKeyFunction(self):
        items = [1, 3, 2, 3]
        groups = From(items).groupby(keyfunc=lambda k: k + 1).tolist()
        self.assertEquals(list(groups[0]), [2, [1]])
        self.assertEquals(list(groups[1]), [4, [3, 3]])
        self.assertEquals(list(groups[2]), [3, [2]])

    def test_groupby_properlyAppliesElementFunction(self):
        items = [1, 3, 2, 2]
        groups = From(items).groupby(elementfunc=lambda k: k + 1).tolist()
        self.assertEquals(list(groups[0]), [1, [2]])
        self.assertEquals(list(groups[1]), [3, [4]])
        self.assertEquals(list(groups[2]), [2, [3, 3]])

    def test_groupby_properlyAppliesResultFunction(self):
        groups = From([1, 3, 2, 1, 2, 3]).groupby(
            resultfunc=lambda r: (r[0], sum(r[1]))).tolist()
        self.assertEquals(list(groups[0]), [1, 2])
        self.assertEquals(list(groups[1]), [3, 6])
        self.assertEquals(list(groups[2]), [2, 4])

    def test_groupjoin_collectionsJoinAndGroupProperly(self):
        outer = [[1, 'a'], [2, 'b'], [3, 'c']]
        inner = [[1, 'A'], [2, 'B'], [2, 'bb'], [4, 'D']]
        groups = From(outer).groupjoin(
            inner,
            lambda x: x[0],
            lambda y: y[0],
            lambda out, in_: [out, in_]).tolist()
        self.assertEquals(groups[0], [[1, 'a'], [[1, 'A']]])
        self.assertEquals(groups[1], [[2, 'b'], [[2, 'B'], [2, 'bb']]])
        self.assertEquals(groups[2], [[3, 'c'], []])

    def test_groupjoin_collectionsJoinAndGroupProperly(self):
        outer = [[1, 'a']]
        inner = [[1, 'A']]
        groups = From(outer).groupjoin(
            inner,
            lambda x: x[0],
            lambda y: y[0],
            lambda out, in_: [out[0], [out[1], in_[0][1]]]).tolist()
        self.assertEquals(groups[0], [1, ['a', 'A']])

    def test_intersect_providesUniquSetOfValuesThatOnlyAppearInBothSequences(self):
        l1 = [1, 3, 3, 5, 6]
        l2 = [3, 6, 7, 8]
        result = From(l1).intersect(l2).tolist()
        self.assertEquals(result, [3, 6])

    def test_join_collectionsProperlyJoinTogether(self):
        outer = [[1, 'a'], [2, 'b'], [3, 'c']]
        inner = [[1, 'A'], [2, 'B'], [2, 'bb'], [4, 'D']]
        joined = From(outer).join(
            inner,
            lambda x: x[0],
            lambda y: y[0],
            lambda out, in_: [out, in_]).tolist()
        self.assertEquals(joined[0], [[1, 'a'], [1, 'A']])
        self.assertEquals(joined[1], [[2, 'b'], [2, 'B']])
        self.assertEquals(joined[2], [[2, 'b'], [2, 'bb']])

    def test_last_returnsLastItemFromSequence(self):
        self.assertEquals(From(iter(self.items)).last(), 10)

    def test_last_returnsLastItemFromList(self):
        self.assertEquals(From(self.items).last(), 10)

    def test_last_returnsLastOddNumberFromList(self):
        self.assertEquals(From(self.items).last(lambda i: i % 2 != 0), 9)

    def test_lastordefault_returnsDefaultWhenListIsEmpty(self):
        self.assertEquals(From([]).lastordefault(5), 5)

    def test_max_returnsItemWithTheHighestValue(self):
        self.assertEquals(From([1, 5, 9, 10]).max(), 10)

    def test_max_returnsItemWithTheHighestValueAndMeetsTheProvidedPredicate(self):
        self.assertEquals(From([9, 10, 15, 20]).max(lambda x: x < 20), 15)

    def test_min_returnsItemWithTheSmallestValue(self):
        self.assertEquals(From([1, 5, 9, 10]).min(), 1)

    def test_min_returnsItemWithTheSmallestValueAndMeetsTheProvidedPredicate(self):
        self.assertEquals(From([9, 10, 15, 20]).min(lambda x: x > 10), 15)

    def test_oftype_returnsAllItemsOfTheProvidedType(self):
        items = [1, 'a', 2.0, lambda x: x]
        self.assertEquals(From(items).oftype(float).tolist(), [2.0])

    def test_orderby_ordersItemsBasedOnTheProvidedKeySelector(self):
        items = [(4, "Q"), (2, "Z"), (7, "M")]
        expected = [(2, "Z"), (4, "Q"), (7, "M")]
        actual = From(items).orderby(lambda x: x[0]).tolist()
        self.assertEquals(actual, expected)

    def test_orderbydecending_ordersItemsInReverseOrderBasedOnTheProvidedKeySelector(self):
        items = [(4, "Q"), (2, "Z"), (7, "M")]
        expected = [(7, "M"), (4, "Q"), (2, "Z")]
        actual = From(items).orderbydecending(lambda x: x[0]).tolist()
        self.assertEquals(actual, expected)

    def test_reverse_reversesTheOrderOfTheList(self):
        expected = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        self.assertEquals(From(self.items).reverse().tolist(), expected)

    def test_reverse_reversesTheOrderOfTheSequence(self):
        expected = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        self.assertEquals(From(iter(self.items)).reverse().tolist(), expected)

    def test_select_returnsNewFromWithEachItemProcessedThroughTheProvidedFunction(self):
        expected = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        actual = From(self.items).select(lambda x: x * 2).tolist()
        self.assertEquals(actual, expected)

    def test_selectmany_flattensListOfLists(self):
        data = [[1, 2], [3, 4], [5, 6]]
        expected = [1, 2, 3, 4, 5, 6]
        actual = From(data).selectmany(lambda row, rowindex: row).tolist()
        self.assertEquals(actual, expected)

    def test_selectmany_flattensListOfListsAndIncludeIndexInResults(self):
        data = [['a', 'b'], ['c', 'd'], ['e', 'f']]
        expected = ['0a', '0b', '1c', '1d', '2e', '2f']
        actual = From(data).selectmany(
            lambda row, rowindex: From(row).select(
                lambda item: "{0}{1}".format(rowindex, item))).tolist()
        self.assertEquals(actual, expected)

    def test_selectmany_flattensListOfListsAndDoublesTheResultItemsValue(self):
        data = [[1, 2], [3, 4], [5, 6]]
        expected = [2, 4, 6, 8, 10, 12]
        actual = From(data).selectmany(
            lambda row, rowindex: row,
            lambda orig, flattened: flattened * 2).tolist()
        self.assertEquals(actual, expected)

    def test_sequence_equal_twoTypesOfSequencesWithTheSameDataAreEqual(self):
        seq1 = [1, 2, 3]
        seq2 = set([1, 2, 3])
        self.assertTrue(From(seq1).sequence_equal(seq2))

    def test_sequence_equal_twoTypesOfSequencesWithTheDifferentDataAreNotEqual(self):
        seq1 = [1, 3, 2]
        seq2 = set([1, 2, 3])
        self.assertFalse(From(seq1).sequence_equal(seq2))

    def test_sequence_equal_twoLengthsOfSequencesAreNotEqual(self):
        seq1 = [1, 2, 3, 4]
        seq2 = set([1, 2, 3])
        self.assertFalse(From(seq1).sequence_equal(seq2))

    def test_single_returnsOneItemWhenTheSeqOnlyContainsSingleItem(self):
        seq = [2]
        self.assertEquals(From(seq).single(), 2)

    def test_single_raisesIndexErrorWhenNumberOfItemsMatchedIsZero(self):
        seq = []
        self.assertRaises(IndexError, From(seq).single)

    def test_single_raisesIndexErrorWhenNumberOfItemsMatchedIsGreaterThanOne(self):
        self.assertRaises(IndexError, From(self.items).single)

    def test_singleordefault_returnsDefaultItemIfSeqIsEmpty(self):
        seq = []
        self.assertEquals(From(seq).singleordefault(5), 5)

    def test_singleordefault_returnsFirstMatchedItem(self):
        seq = [5]
        self.assertEquals(From(seq).singleordefault(6), 5)

    def test_skip_skipsTheGivenNumberOfItemsThenReturnsTheRemainderOfTheSeq(self):
        self.assertEquals(From(self.items).skip(5).tolist(), [6, 7, 8, 9, 10])

    def test_skip_skippingMoreItemsThanInTheSeqReturnsEmptySeq(self):
        self.assertEquals(From(self.items).skip(15).tolist(), [])

    def test_skipwhile_skipItemsWhileTheGivenPredicateIsTrue(self):
        seq = [2, 4, 6, 1, 3, 5, 6]
        actual = From(seq).skipwhile(lambda x, i: x % 2 == 0).tolist()
        expected = [1, 3, 5, 6]
        self.assertEquals(actual, expected)

    def test_sum_returnTheSumOfTheItemsInTheSequence(self):
        self.assertEquals(From(self.items).sum(), 55)

    def test_sum_returnTheSumOfTheItemsInTheSequenceThatMatchTheGivenSelector(self):
        self.assertEquals(From(self.items).sum(lambda x: x < 4), 6)

    def test_take_returnsTheSpecifiedNumberOfItemsFromTheSequence(self):
        self.assertEquals(From(self.items).take(3).tolist(), [1, 2, 3])

    def test_takewhile_returnsTheSpecifiedNumberOfItemsFromTheSequenceThatMatchTheGivenSelector(self):
        actual = From(self.items).takewhile(lambda x, i: i < 2).tolist()
        expected = [1, 2]
        self.assertEquals(actual, expected)

    def test_toarray_returnsTheSequeceAsAnArray(self):
        self.assertEquals(
            From([1, 2, 3]).toarray('i'),
            array('i', [1, 2, 3]))

    def test_todictionary_returnsTheSequenceAsNewDictionary(self):
        data = [["a", "1"], ["b", "2"], ["c", "3"]]
        expected = { "a": ["a", "1"], "b": ["b", "2"], "c": ["c", "3"]}
        actual = From(data).todictionary(lambda x: x[0])
        self.assertEquals(actual, expected)

    def test_tolist_returnsTheSequenceAsNewList(self):
        self.assertEquals(
            From([1, 2, 3]).tolist(),
            [1, 2, 3])

    def test_toseq_returnTheSequeceAsNewSequence(self):
        self.assertEquals(
            list(From(x for x in range(5)).toseq()),
            list(x for x in range(5)))

    def test_union_returnsUniqueSetOfItemsFromTwoSequencesWhilePreservingOrder(self):
        seq1 = [2, 1, 4, 5, 4]
        seq2 = [6, 4, 7, 8, 1]
        expected = [2, 1, 4, 5, 6, 7, 8]
        actual = From(seq1).union(seq2).tolist()
        self.assertEquals(actual, expected)

    def test_where_filtersItemsFromTheSequenceThatDontMatchThePredicate(self):
        self.assertEquals(
            From(self.items).where(lambda item: item > 5).tolist(),
            [6, 7, 8, 9, 10])

    def test_wherei_filtersItemsFromTheSequenceThatDontMatchThePredicate(self):
        self.assertEquals(
            From(self.items).wherei(lambda item, i: item > 5).tolist(),
            [6, 7, 8, 9, 10])
