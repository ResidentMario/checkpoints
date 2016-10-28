import pandas as pd
from pandas.core.frame import DataFrame, Series
import warnings


class CheckpointStateMachine:

    def __init__(self, **kwargs):
        self.results = []

    @staticmethod
    def disable():
        del DataFrame.safe_apply
        del Series.safe_map

    def enable(self):

        def safe_apply(df, func, **kwargs):

            # If flushed, restart from scratch.
            if 'flush' in kwargs and kwargs['flush'] == True:
                self.results = []

            # Shorten the jobs list to the DataFrame elements remaining.
            df_remaining = df.iloc[len(self.results):]

            # Replace the original applied `func` with a stateful wrapper, `new_func`.
            def new_func(srs):
                try:
                    self.results.append(func(srs))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    warnings.warn("Failure on index {0}".format(len(self.results)), UserWarning)
                    raise

            # Populate `self.results`.
            if 'axis' in kwargs and (kwargs['axis'] == 1 or kwargs['axis'] == 'columns'):
                for _, srs in df_remaining.iterrows():
                    new_func(srs)
            else:
                for _, srs in df_remaining.iteritems():
                    new_func(srs)

            # If we got here, then we didn't exit out due to an exception, and we can finish the method successfully.
            # Let `pandas.apply` handle concatenation using a trivial combiner.
            out = pd.Series(range(len(self.results))).apply(lambda i: self.results[i])
            out.index = df.index

            # Reset the results set for the next iteration.
            self.results = []

            # Return
            return out

        DataFrame.safe_apply = safe_apply

        def safe_map(srs, func, **kwargs):
            # If flushed, restart from scratch.
            if 'flush' in kwargs and kwargs['flush'] == True:
                self.results = []

            # Shorten the jobs list to the DataFrame elements remaining.
            srs_remaining = srs.iloc[len(self.results):]

            # Replace the original applied `func` with a stateful wrapper, `new_func`.
            def new_func(val):
                try:
                    self.results.append(func(val))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    warnings.warn("Failure on index {0}".format(len(self.results)), UserWarning)
                    raise

            # Populate `self.results`.
            for _, val in srs_remaining.iteritems():
                new_func(val)

            # If we got here, then we didn't exit out due to an exception, and we can finish the method successfully.
            # Let `pandas.apply` handle concatenation using a trivial combiner.
            out = pd.Series(range(len(self.results))).apply(lambda i: self.results[i])
            out.index = srs.index

            # Reset the results set for the next iteration.
            self.results = []

            # Return
            return out

        Series.safe_map = safe_map


checkpoints = CheckpointStateMachine()
disable = checkpoints.disable
enable = checkpoints.enable