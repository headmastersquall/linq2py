#!/usr/bin/env python

# TODO: Add create a orderby class that contains a thenby method.
# TODO: Add tolookup(keyselector)

import itertools
from array import array
from collections import OrderedDict
from functools import reduce

__all__ = ["From"]


def identity(x):
    return x


class From(object):

    def __init__(self, seq):
        """
        Create a new From instance providing a sequence of items to perform
        operations on.
        """
        self.seq = seq

    def __iter__(self):
        return (item for item in self.seq)

    def aggregate(self, accumulatorfn, seed=0, resultfn=identity):
        """
        Applies the given accumulator function to the sequence.

        The accumulator must accept two parameters of current, and next, and
        return a result.

        Seed is the initial value to be used in the accumulator.

        resultfn is applied to the final result before it is returned.
        """
        return resultfn(reduce(accumulatorfn, self.seq, seed))

    def all(self, pred):
        """
        Returns true if each item in the sequence evaluates to true when
        applied to the given predicate.
        """
        return all(itertools.imap(pred, self.seq))

    def any(self, pred):
        """
        Returns true if any item in the sequence returns true when
        applied to the given predicate.
        """
        return any(itertools.imap(pred, self.seq))

    def average(self):
        """
        Calculates the average value from a numeric sequence.
        """
        items = self.tolist()
        return sum(items) / len(items)

    def cast(self, fn):
        """
        Applies the given function to each item in the sequence to convert them
        to another type.
        """
        return From(itertools.imap(fn, self.seq))

    def concat(self, *iterables):
        """
        Returns a new From object with the provided iterables concatenated with
        the existing sequence.
        """
        return From(itertools.chain(self.seq, *iterables))

    def contains(self, item):
        """
        Returns true if the provided item exists in the sequence.
        """
        return len([x for x in self.seq if x == item]) >= 1

    def count(self, pred=lambda x: True):
        """
        Counts the number of items in the sequence.  An optional predicate can
        be provided and only items that are given to the predicate that return
        True will be counted.
        """
        return len(list(filter(pred, self.seq)))

    def defaultifempty(self, default):
        """
        Returns a new From with the provided value if the sequence is empty,
        otherwise returns a new From with the current sequence.
        """
        s1, s2 = itertools.tee(self.seq)
        try:
            s1.next()
            return From(s2)
        except StopIteration:
            return From(default)

    def distict(self):
        """
        Returns a new From containing a unique set of items from the sequence.
        """
        return From(set(self.seq))

    def elementat(self, index):
        """
        Returns the item in the sequence located at the provided index.
        """
        return list(itertools.islice(self.seq, index, index + 1))[0]

    def elementatordefault(self, index, default):
        """
        Returns the item in the sequence located at the provided index.
        """
        try:
            return self.elementat(index)
        except IndexError:
            return default

    def except_(self, seq):
        """
        Returns a new From containing all items except those that appear in
        the provided sequence.
        """
        otherseq = From(seq).tolist()
        return From(item for item in self.seq
                    if item not in otherseq)

    def first(self, pred=lambda x: True):
        """
        Returns the first item in the sequence.  If a predicate is provided,
        return the first item in the sequence that returns True when applied
        to the predicate.
        """
        for item in self.seq:
            if pred(item):
                return item

    def firstordefault(self, default, pred=lambda x: True):
        """
        Returns the first item in the sequence.  If no item is matches the
        predicate, the default value is returned.
        """
        item = self.first(pred)
        return item if item else default

    def groupby(
            self,
            keyfunc=identity,
            elementfunc=identity,
            resultfunc=identity):
        """
        Groups items by keyfunc and applies elementfunc to each element.
        Finally, resultfunc is applied to each group result.
        """
        od = OrderedDict()
        for item in self.seq:
            key = keyfunc(item)
            if key not in od:
                od[key] = []
            od[key].append(elementfunc(item))
        return From(resultfunc(item) for item in od.items())

    def groupjoin(
            self,
            inner,
            outerkeyselector,
            innerkeyselector,
            resultselector):
        """
        Joins two sequences by the provided key selectors and groups the
        results.  The results are then processed through the resultselector.

        inner is another sequence of data that the current sequence will be
        joined with.

        outerkeyselector is a function that evaluates the value to be used as
        the key for the outer collection.  The outer collection is the one
        which was initially passed into From.

        innerkeyselector is a function that evaluates the value to be used as
        the key for the inner collection.  The inner collection is the one
        passed into this method as inner.
        """
        od = OrderedDict()
        for item in self.seq:
            key = outerkeyselector(item)
            if key not in od:
                od[key] = [item]
        for item in inner:
            key = innerkeyselector(item)
            if key in od:
                od[key].append(item)
        return From(resultselector(items[0], items[1:])
                    for key, items in od.items())

    def intersect(self, seq):
        """
        Returns a set of values that only appear in both sequences.
        """
        s1 = set(self.seq)
        s2 = set(seq)
        return From(item for item in s1 if item in s2)

    def join(self, inner, outerkeyselector, innerkeyselector, resultselector):
        """
        Joins two sequences by the provided key selectors.  The results are
        then processed through the resultselector.

        inner is another sequence of data that the current sequence will be
        joined with.

        outerkeyselector is a function that evaluates the value to be used as
        the key for the outer collection.  The outer collection is the one
        which was initially passed into From.

        innerkeyselector is a function that evaluates the value to be used as
        the key for the inner collection.  The inner collection is the one
        passed into this method as inner.
        """
        d = {}
        for item in self.seq:
            d[outerkeyselector(item)] = item

        def joingenerator():
            for item in inner:
                key = innerkeyselector(item)
                if key in d:
                    yield (d[key], item)

        return From(resultselector(out, in_) for out, in_ in joingenerator())

    def last(self, pred=lambda x: True):
        """
        Returns the last element in the sequence.
        """
        last = None
        for item in self.seq:
            if pred(item):
                last = item
        if not last:
            raise IndexError(
                "No items in the sequence matched the given predicate")
        return last

    def lastordefault(self, default, pred=lambda x: True):
        """
        Returns the last item in the sequence, or the value provided if no item
        was found.
        """
        try:
            return self.last(pred)
        except IndexError:
            return default

    def max(self, pred=lambda x: True):
        """
        Returns the item with the highest value and meets the provided
        predicate.
        """
        return max(itertools.ifilter(pred, self.seq))

    def min(self, pred=lambda x: True):
        """
        Returns the item with the smallest value and meets the provided
        predicate.
        """
        return min(itertools.ifilter(pred, self.seq))

    def oftype(self, type_):
        """
        Returns all items in the sequence that are of the given type.
        """
        return From(x for x in self.seq if isinstance(x, type_))

    def orderby(self, keyselector=identity):
        """
        Returns a new From with the sequence ordered by the provided key
        selector.
        """
        return From(sorted(self.seq, key=keyselector))

    def orderbydecending(self, keyselector=identity):
        """
        Returns a new From with the sequence ordered in reverse order by the
        provided key selector.
        """
        return From(sorted(self.seq, key=keyselector, reverse=True))

    def reverse(self):
        """
        Returns a new From with the sequence reversed.
        """
        return From(reversed(self.tolist()))

    def select(self, fn):
        """
        Returns a new From with each item in the sequence processed through
        the provided function.
        """
        return From(itertools.imap(fn, self.seq))

    def selectmany(
        self,
        collectionselector,
            resultselector=lambda orig, flatval: flatval):
        """
        Enumerates through an iterable collection of iterables and flattens
        them to a single iterable of items returned as a From.

        The collectionselector is used to extract the collection from each
        item in the sequence.  Two parameters are passed into this function.
        The first is the row itself, and the second is the index of the row.
        An iterable must be returned from this function which contains the
        values to included in the final flattened sequence.

        The resultselector function is applied to each item in the flattened
        sequence and is available to transform the flattened data.  It also
        accepts two parameters.  The first being the original row, and the
        second being the flattened value.  This function is optional and will
        not make changes to the data by default.
        """

        def selectmanygenerator():
            for idx, coll in enumerate(self.seq):
                for item in collectionselector(coll, idx):
                    yield resultselector(coll, item)
        return From(x for x in selectmanygenerator())

    def sequence_equal(self, seq):
        """
        Returns True if boths sequences contain the same data.
        """
        seq1 = self.tolist()
        seq2 = From(seq).tolist()

        if len(seq1) != len(seq2):
            return False

        for item1, item2 in itertools.izip(seq1, seq2):
            if item1 != item2:
                return False
        return True

    def single(self, pred=lambda x: True):
        """
        Returns a single item if it is the only item that is matched by the
        predicate.  Raises an IndexError if more than one item is matched, or
        if the sequence is empty.
        """
        matched = filter(pred, self.seq)
        if len(matched) != 1:
            raise IndexError("Single must only match one value.")
        return matched[0]

    def singleordefault(self, default, pred=lambda x: True):
        """
        Returns the default value if the sequence is empty, otherwize calls
        single.
        """
        s1, s2 = itertools.tee(self.seq)
        try:
            s1.next()
            return From(s2).single(pred)
        except StopIteration:
            return default

    def skip(self, num):
        """
        Skips the given number of items in the sequence, then returns the rest
        as a new From.
        """
        return self.skipwhile(lambda item, i: i < num)

    def skipwhile(self, selector):
        """
        Skips items in the sequence while the given selector returns True.
        The selector must accept two parameters.  The first being the item
        in the sequence, and the second being the index of the item.
        """
        def skipgenerator():
            pred = selector
            enumerator = enumerate(self.seq)
            for index, item in enumerator:
                if pred(item, index):
                    continue
                else:
                    pred = lambda it, ix: False
                    yield item
        return From(item for item in skipgenerator())

    def sum(self, selector=lambda x: True):
        """
        Returns the sum of items in the sequence that match the given selector.
        """
        return sum(itertools.ifilter(selector, self.seq))

    def take(self, num):
        """
        Returns a new From with the specified number of items from the
        sequence.
        """
        return self.takewhile(lambda item, i: i < num)

    def takewhile(self, selector):
        """
        Returns a new From with the items that match the given selector.
        The selector must accept two parameters.  The first being the item
        in the sequence, and the second being the index of the item.
        """
        def takegenerator():
            enumerator = enumerate(self.seq)
            for index, item in enumerator:
                if selector(item, index):
                    yield item
                else:
                     break
        return From(item for item in takegenerator())

    def toarray(self, typecode):
        """
        Returns the sequence as a new array using the provided typecode.
        The available typecodes can be found here:
        http://docs.python.org/2/library/array.html?highlight=array#array
        """
        return array(typecode, self.seq)

    def todictionary(self, keyselector, valueselector=identity):
        """
        Converts the sequence to a dictionary using the result from the
        keyselector function as the key.  The row in the sequence is the value
        and is passed through the valueselector function.
        """
        d = {}
        for item in self.seq:
            d[keyselector(item)] = valueselector(item)
        return d

    def tolist(self):
        """
        Returns the current sequence as a new list.
        """
        return [x for x in self.seq]

    def toseq(self):
        """
        Returns the current sequence as a new sequence.
        """
        return (x for x in self.seq)

    def union(self, *iterables):
        """
        Returns a new From containing a set of values where the item in each
        sequence only appears once.
        """
        newset = []
        for item in self.concat(*iterables):
            if item not in newset:
                newset.append(item)
        return From(newset)

    def where(self, pred):
        """
        Filters items in the sequence to only those that match the provided
        predicate.  The predicate must accept two parameters.  The first being
        the item in the sequence, and the second being the index of the item.
        """
        return From(item for i, item in enumerate(self.seq)
                    if pred(item, i))
