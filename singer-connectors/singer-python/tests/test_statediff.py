import unittest
import singer.statediff as statediff
from singer.statediff import Add, Remove, Change


class TestPaths(unittest.TestCase):

    def test_simple_dict(self):

        self.assertEqual(
            [(('a',), 1),
             (('b',), 2)],
            statediff.paths({'a': 1, 'b': 2}))

    def test_nested_dict(self):
        self.assertEqual(
            [(('a', 'b'), 1),
             (('a', 'c'), 2),
             (('d', 'e'), 3)],
            statediff.paths(
                {
                    'a': {
                        'b': 1,
                        'c': 2
                    },
                    'd': {
                        'e': 3
                    }
                }
            )
        )

    def test_simple_array(self):
        self.assertEqual(
            [((0,), 'blue'),
             ((1,), 'green')],
            statediff.paths(
                ['blue', 'green']))

    def test_nested_array(self):
        self.assertEqual(
            [((0, 0), 'blue'),
             ((0, 1), 'red'),
             ((1, 0), 'green')],
            statediff.paths([['blue', 'red'], ['green']]))

    def test_arrays_in_dicts(self):
        self.assertEqual(
            [(('a', 0), 'blue'),
             (('a', 1), 'red'),
             (('b', 0), 'green')],
            statediff.paths(
                {
                    'a': ['blue', 'red'],
                    'b': ['green']
                }
            )
        )

    def test_none(self):
        self.assertEqual([], statediff.paths(None))


class TestDiff(unittest.TestCase):

    def test_add(self):
        self.assertEqual(
            [Add(('a',), 1),
             Add(('b',), 2)],
            statediff.diff({}, {'a': 1, 'b': 2}))

    def test_remove(self):
        self.assertEqual(
            [Remove(('a',), 1),
             Remove(('b',), 2)],
            statediff.diff({'a': 1, 'b': 2}, {}))

    def test_change(self):
        self.assertEqual(
            [Change(('a',), 1, 100),
             Change(('b',), 2, 200)],
            statediff.diff({'a': 1, 'b': 2},
                           {'a': 100, 'b': 200}))

    def test_null_input_for_old(self):
        self.assertEqual(
            [Add(('a',), 1)],
            statediff.diff(None, {'a': 1}))

    def test_null_input_for_new(self):
        self.assertEqual(
            [Remove(('a',), 1)],
            statediff.diff({'a': 1}, None))

    def test_null_input_for_both(self):
        self.assertEqual([], statediff.diff(None, None))

    def test_null_at_leaf(self):
        self.assertEqual(
            [Change(('a',), 1, None),
             Change(('b',), None, 2)],
            statediff.diff({'a': 1, 'b': None},
                           {'a': None, 'b': 2}))
