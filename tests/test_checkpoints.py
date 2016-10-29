import unittest
from checkpoints import checkpoints
import pandas as pd
import numpy as np


class TestInteractions(unittest.TestCase):

    def setUp(self):
        checkpoints.enable()

    def testEnable(self):
        pd.Series.safe_map
        pd.DataFrame.safe_apply

    def testFlush(self):
        checkpoints._results = ['foo', 'bar']
        checkpoints._index = pd.Index([1,2])
        checkpoints.flush()
        self.assertEqual(checkpoints._results, [])
        self.assertEqual(checkpoints._index, None)
        pass

    def testResultsEmpty(self):
        checkpoints.flush()
        self.assertEqual(checkpoints.results, None)

    def testResultsScalars(self):
        checkpoints._results = ['foo', 'bar']
        checkpoints._index = pd.Index([1,2])
        checkpoints._caller = "safe_apply"
        self.assertTrue(np.array_equal(checkpoints.results.values, np.array(['foo', 'bar'])))

    def testInvalidAttr(self):
        with self.assertRaises(AttributeError):
            checkpoints.loremipsum

    def testDisable(self):
        checkpoints.disable()
        with self.assertRaises(AttributeError):
            pd.Series.safe_map
            pd.DataFrame.safe_apply
        checkpoints.enable()

    def tearDown(self):
        checkpoints.disable()


class TestSeriesMethods(unittest.TestCase):

    def setUp(self):
        checkpoints.enable()

    def testPartialScalarFunctionalInput(self):
        checkpoints.flush()
        srs = pd.Series(np.random.random(100))
        srs[50] = 0

        def breaker(val):
            if val == 0:
                raise IOError
            else:
                return val

        import warnings
        warnings.filterwarnings('ignore')
        with self.assertRaises(IOError):
            srs.safe_map(breaker)
        self.assertEquals(len(checkpoints._results), 50)
        self.assertIsNot(checkpoints._index, None)
        srs[50] = 1
        result = srs.safe_map(breaker)
        self.assertIsNot(result, None)
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(len(result), 100)

    def testCompleteScalarFunctionalInput(self):
        checkpoints.flush()
        srs = pd.Series(np.random.random(100))
        m1 = np.average(srs.map(lambda val: val - 0.5))
        m2 = np.average(srs.safe_map(lambda val: val - 0.5))
        self.assertAlmostEqual(m1, m2)

    def testCompleteListFunctionalInput(self):
        checkpoints.flush()
        self.assertTrue(
            np.array_equal(pd.Series([1,2,3]).safe_map(lambda v: [1,2]),
                           pd.Series([1, 2, 3]).map(lambda v: [1, 2]))
        )

    def testCompleteSeriesFunctionalInput(self):
        checkpoints.flush()
        s1 = pd.Series([1, 2, 3]).safe_map(lambda v: pd.Series([1,2]))
        s2 = pd.Series([1, 2, 3]).map(lambda v: pd.Series([1,2]))
        self.assertTrue(isinstance(s1[0], pd.Series) and isinstance(s2[0], pd.Series))
        self.assertTrue(np.array_equal(s1[0].values, s2[0].values))
        self.assertTrue(np.array_equal(s1[1].values, s2[1].values))

    def testCompleteDictFunctionalInput(self):
        checkpoints.flush()
        d1 = pd.Series([1, 2, 3]).safe_map(lambda v: pd.Series({'a': 1, 'b': 2}))
        d2 = pd.Series([1, 2, 3]).map(lambda v: pd.Series({'a': 1, 'b': 2}))
        self.assertTrue(isinstance(d1[0], pd.Series) and isinstance(d2[0], pd.Series))
        self.assertTrue(np.array_equal(d1[0].values, d1[0].values))
        self.assertTrue(np.array_equal(d2[1].values, d2[1].values))

    def testCompleteDataFrameFunctionalInput(self):
        checkpoints.flush()
        d1 = pd.Series([1, 2, 3]).safe_map(lambda v: pd.DataFrame({'a': [1, 2], 'b': [2, 3]}))
        d2 = pd.Series([1, 2, 3]).map(lambda v: pd.DataFrame({'a': [1, 2], 'b': [2, 3]}))
        self.assertTrue(isinstance(d1[0], pd.DataFrame) and isinstance(d2[0], pd.DataFrame))
        self.assertTrue(np.array_equal(d1[0].values, d1[0].values))
        self.assertTrue(np.array_equal(d2[1].values, d2[1].values))

    def testNotImplemented(self):
        with self.assertRaises(NotImplementedError):
            pd.Series([1]).safe_map(lambda v: v, na_action='ignore')

    def tearDown(self):
        checkpoints.disable()


class TestDataFrameMethods(unittest.TestCase):

    def setUp(self):
        checkpoints.enable()

    def testPartialScalarFunctionalInput(self):
        checkpoints.flush()
        df = pd.DataFrame(np.random.random(100).reshape((20, 5)))
        df[2][4] = 0

        def breaker(srs):
            if 0 in srs.values:
                raise IOError
            else:
                return srs.sum()

        import warnings
        warnings.filterwarnings('ignore')
        with self.assertRaises(IOError):
            df.safe_apply(breaker, axis='columns')
        self.assertEquals(len(checkpoints._results), 4)
        self.assertIsNot(checkpoints._index, None)

    def testCompleteScalarFunctionalInput(self):
        checkpoints.flush()
        df = pd.DataFrame(np.random.random(100).reshape((20, 5)))
        m1 = np.average(df.apply(sum))
        m2 = np.average(df.safe_apply(sum))
        self.assertAlmostEqual(m1, m2)

    def testCompleteListFunctionalInput(self):
        checkpoints.flush()
        self.assertTrue(
            np.array_equal(pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: [1, 2]),
                           pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: [1, 2]))
        )

    def testCompleteSeriesFunctionalInput(self):
        checkpoints.flush()
        s1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: pd.Series(['A','B']))
        s2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).apply(lambda v: pd.Series(['A','B']))
        self.assertTrue(s1.equals(s2))

    def testCompleteSeriesFunctionalInputColumnar(self):
        checkpoints.flush()
        s1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: pd.Series(['A','B']), axis=1)
        s2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).apply(lambda v: pd.Series(['A','B']), axis=1)
        self.assertTrue(s1.equals(s2))

    def testCompleteDictFunctionalInput(self):
        checkpoints.flush()
        d1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: pd.Series({'a': 1, 'b': 2}))
        d2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).apply(lambda v: pd.Series({'a': 1, 'b': 2}))
        self.assertTrue(d1.equals(d2))

    def testCompleteDictFunctionalInputColumnar(self):
        checkpoints.flush()
        d1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: pd.Series({'a': 1, 'b': 2}), axis=1)
        d2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).apply(lambda v: pd.Series({'a': 1, 'b': 2}), axis=1)
        self.assertTrue(d1.equals(d2))

    # This cannot be done. It should raise a NotImplementedError, except that there's no natural break point in the
    # code for doing so. It's documented as a limitation in the README.
    # def testCompleteDataFrameFunctionalInput(self):
    #     checkpoints.flush()
    #     d1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: pd.DataFrame({'a': [1, 2], 'b': [2, 3]}))
    #     d2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).apply(lambda v: pd.DataFrame({'a': [1, 2], 'b': [2, 3]}))
    #     self.assertTrue(d1.equals(d2))

    def testFunctionWithKwargs(self):
        checkpoints.flush()

        def f(srs, **kwargs):
            return 1 + sum(kwargs.values())

        r = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(f, somearg=5)
        self.assertTrue(np.array_equal(r.values, [6, 6]))

    def testNotImplemented(self):
        with self.assertRaises(NotImplementedError):
            pd.DataFrame({'a':[1], 'b': [2]}).safe_apply(lambda v: v, broadcast=True)
        with self.assertRaises(NotImplementedError):
            pd.DataFrame({'a': [1], 'b': [2]}).safe_apply(lambda v: v, raw=True)
        with self.assertRaises(NotImplementedError):
            pd.DataFrame({'a': [1], 'b': [2]}).safe_apply(lambda v: v, reduce=True)

    def tearDown(self):
        checkpoints.disable()
