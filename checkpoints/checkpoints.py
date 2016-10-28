import pandas as pd
from pandas.core.frame import DataFrame, Series
import warnings


class CheckpointStateMachine:

    def __init__(self, **kwargs):
        self._results = []
        self._index = None
        # cf. self.results, virtualized using `__getattr__()`.

    @staticmethod
    def disable():
        """
        Core runtime method, disables all of the `checkpoints` safe mappers.
        """
        del DataFrame.safe_apply
        del Series.safe_map

    def enable(self):
        """
        Core runtime method, enables all of the `checkpoints` safe mappers.
        """

        def safe_apply(df, func, **kwargs):
            """
            Core method, implements a cached version of `pandas.DataFrame.apply`.
            """

            # If flushed, restart from scratch.
            if 'flush' in kwargs and kwargs['flush'] == True:
                self.flush()

            # If index is not defined, define it.
            if self._index is None:
                self._index = df.index

            # Shorten the jobs list to the DataFrame elements remaining.
            df_remaining = df.iloc[len(self._results):]

            # Replace the original applied `func` with a stateful wrapper.
            def wrapper(srs):
                try:
                    self._results.append(func(srs))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    warnings.warn("Failure on index {0}".format(len(self._results)), UserWarning)
                    raise

            # Populate `self.results`.
            if 'axis' in kwargs and (kwargs['axis'] == 1 or kwargs['axis'] == 'columns'):
                for _, srs in df_remaining.iterrows():
                    wrapper(srs)
            else:
                for _, srs in df_remaining.iteritems():
                    wrapper(srs)

            # If we got here, then we didn't exit out due to an exception, and we can finish the method successfully.
            # Let `pandas.apply` handle concatenation using a trivial combiner.
            out = self.results

            # Reset the results set for the next iteration.
            self._results = []
            self._index = None

            # Return
            return out

        DataFrame.safe_apply = safe_apply

        def safe_map(srs, func, **kwargs):
            """
            Core method, implements a cached version of `pandas.Series.map`.
            """

            # If flushed, restart from scratch.
            if 'flush' in kwargs and kwargs['flush'] == True:
                self.flush()

            # If index is not defined, define it.
            if self._index is None:
                self._index = srs.index

            # Shorten the jobs list to the DataFrame elements remaining.
            srs_remaining = srs.iloc[len(self._results):]

            # Replace the original applied `func` with a stateful wrapper.
            def wrapper(val):
                try:
                    self._results.append(func(val))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    warnings.warn("Failure on index {0}".format(len(self._results)), UserWarning)
                    raise

            # Populate `self.results`.
            for _, val in srs_remaining.iteritems():
                wrapper(val)

            # If we got here, then we didn't exit out due to an exception, and we can finish the method successfully.
            # Let `pandas.apply` handle concatenation using a trivial combiner.
            out = self.results

            # Reset the results set for the next iteration.
            self._results = []
            self._index = None

            # Return
            return out

        Series.safe_map = safe_map

    def __getattr__(self, item):
        """
        Implements a lazy getter for fetching partial results using `checkpoints.results`.
        """
        # import pdb; pdb.set_trace()
        if item == "results":
            if len(self._results) == 0:
                return None
            elif not isinstance(self._results[0], DataFrame):
                out = pd.Series(range(len(self._results))).apply(lambda i: self._results[i])
                out.index = self._index[:len(out)]
                return out
            else:
                out = pd.concat(self._results)
                return out
        else:
            raise AttributeError

    def flush(self):
        """
        Flushes the contents of the `checkpoints` state machine.
        """
        self._results = []
        self._index = None


checkpoints = CheckpointStateMachine()
disable = checkpoints.disable
enable = checkpoints.enable
flush = checkpoints.flush
