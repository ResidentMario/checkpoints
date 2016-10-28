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
        self.assertTrue(np.array_equal(checkpoints.results.values, np.array(['foo', 'bar'])))

    def testResultsSeries(self):
        checkpoints._results = [pd.Series([1,2,3], index=list('ABC')), pd.Series([4,5,6], index=list('ABC'))]
        checkpoints._index = pd.Index(['First', 'Second'])
        results = checkpoints.results
        self.assertTrue(np.array_equal(results.index.values, pd.Index(['First', 'Second'])))
        self.assertTrue(np.array_equal(results.columns.values, pd.Index(list('ABC'))))
        self.assertTrue(np.array_equal(results.values, np.array([1,2,3,4,5,6]).reshape((2,3))))

    def testResultsDataFrame(self):
        checkpoints._results = [pd.DataFrame({"A": [1,2], "B": [3,4]}, index=['First', 'Second']),
                                pd.DataFrame({"A": [5, 6], "B": [7, 8]}, index=['Third', 'Fourth'])]
        checkpoints._index = pd.Index(['First', 'Second', 'Third', 'Fourth'])
        results = checkpoints.results
        self.assertTrue(np.array_equal(results.index.values, pd.Index(['First', 'Second', 'Third', 'Fourth'])))
        self.assertTrue(np.array_equal(results.columns.values, pd.Index(list('AB'))))
        self.assertTrue(np.array_equal(results.values, np.array([[1, 3],[2, 4],[5, 7],[6, 8]])))

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

    def testCompleteOutput(self):
        checkpoints.flush()
        # import pdb; pdb.set_trace()
        srs = pd.Series(np.random.random(100))
        m1 = np.average(srs.map(lambda val: val - 0.5))
        m2 = np.average(srs.safe_map(lambda val: val - 0.5))
        self.assertAlmostEqual(m1, m2)

    def testPartialOutput(self):
        checkpoints.flush()
        # import pdb; pdb.set_trace()
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

    def tearDown(self):
        checkpoints.disable()


# class TestDataFrameMethods(unittest.TestCase):
#
#     def setUp(self):
#         checkpoints.enable()
#
#     def testCompleteOutput(self):
#         checkpoints.flush()
#         # import pdb; pdb.set_trace()
#         srs = pd.Series(np.random.random(100))
#         m1 = np.average(srs.map(lambda val: val - 0.5))
#         m2 = np.average(srs.safe_map(lambda val: val - 0.5))
#         self.assertAlmostEqual(m1, m2)
#
#     def testPartialOutput(self):
#         checkpoints.flush()
#         # import pdb; pdb.set_trace()
#         srs = pd.Series(np.random.random(100))
#         srs[50] = 0
#
#         def breaker(val):
#             if val == 0:
#                 raise IOError
#             else:
#                 return val
#
#         import warnings
#         warnings.filterwarnings('ignore')
#         with self.assertRaises(IOError):
#             srs.safe_map(breaker)
#         self.assertEquals(len(checkpoints._results), 50)
#         self.assertIsNot(checkpoints._index, None)
#
#     def tearDown(self):
#         checkpoints.disable()
