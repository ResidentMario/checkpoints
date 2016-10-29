import pandas as pd
from pandas.core.frame import DataFrame, Series
import warnings


class CheckpointStateMachine:

    def __init__(self, **kwargs):
        self._results = []
        self._index = None
        self._caller = None
        self._axis = 0
        # cf. self.results, virtualized using `__getattr__()`.

    def disable(self):
        """
        Core runtime method, disables all of the `checkpoints` safe mappers.
        """
        self.flush()
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
                kwargs.pop('flush')
                self.flush()

            # If index is not defined, define it. Note that this is not the index of the original DataFrame,
            # but the index of concatenation. If we are slicing and then concatenating on the columns (0),
            # the index that we must append at the end is of the column headers; if we are slicing and concatenating on
            # the rows (1), it's the row headers, e.g. the original index.
            #
            # See `__getattr__` for more shenanigans around this.
            if self._index is None:
                if 'axis' in kwargs and (kwargs['axis'] == 1 or kwargs['axis'] == 'columns'):
                    self._axis = 1
                    self._index = df.index
                else:
                    self._axis = 0
                    self._index = df.columns

            # If caller is not defined, define it.
            if self._caller is None:
                self._caller = "safe_apply"

            # Prune **kwargs of unimplemented pd.DataFrame.apply features---as of pandas 0.19, `broadcast`, `raw`,
            # and `reduce`.
            if 'broadcast' in kwargs or 'raw' in kwargs or 'reduce' in kwargs:
                raise NotImplementedError

            # Shorten the jobs list to the DataFrame elements remaining.
            df_remaining = df.iloc[len(self._results):]

            # Replace the original applied `func` with a stateful wrapper.
            def wrapper(srs, **kwargs):
                try:
                    self._results.append(func(srs, **kwargs))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    warnings.warn("Failure on index {0}".format(len(self._results)), UserWarning)
                    raise

            # Populate `self.results`.
            if 'axis' in kwargs and (kwargs['axis'] == 1 or kwargs['axis'] == 'columns'):
                kwargs.pop('axis')
                for _, srs in df_remaining.iterrows():
                    wrapper(srs, **kwargs)
            else:
                if 'axis' in kwargs:
                    kwargs.pop('axis')
                for _, srs in df_remaining.iteritems():
                    wrapper(srs, **kwargs)

            # If we got here, then we didn't exit out due to an exception, and we can finish the method successfully.
            # Let `pandas.apply` handle concatenation using a trivial combiner.
            out = self.results

            # Reset the results set for the next iteration.
            self.flush()

            # Return
            return out

        DataFrame.safe_apply = safe_apply

        def safe_map(srs, func, *args, **kwargs):
            """
            Core method, implements a cached version of `pandas.Series.map`.
            """

            # If flushed, restart from scratch.
            if 'flush' in kwargs and kwargs['flush'] == True:
                self.flush()

            # If index is not defined, define it. Same with caller.
            if self._index is None:
                self._index = srs.index
            if self._caller is None:
                self._caller = "safe_map"

            # Prune **kwargs of unimplemented pd.Series.map features---only one as of pandas 0.19, `na_action`.
            if 'na_action' in kwargs:
                raise NotImplementedError

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
            self.flush()

            # Return
            return out

        Series.safe_map = safe_map

    def __getattr__(self, item):
        """
        Implements a lazy getter for fetching partial results using `checkpoints.results`.
        """
        if item != "results":
            raise AttributeError
        elif self._caller == "safe_map":
            if len(self._results) == 0:
                return None
            else:
                out = pd.Series(self._results)
                out.index = self._index[:len(out)]
                return out
        elif self._caller == "safe_apply":
            # import pdb; pdb.set_trace()
            if len(self._results) == 0:
                return None
            elif isinstance(self._results[0], Series):
                # Note that `self._index` is not the index of the DataFrame, but the index of concatenation. If we
                # are slicing and then concatenating on the columns (0), the index that we must append at the end is
                # of the column headers; if we are slicing and concatenating on the rows (1), it's the row headers,
                # e.g. the original index.
                #
                # Which of these two is the case totally changes which strategy we follow for gluing the data back
                # into a DataFrame matching our original design, requiring all of the tedium of keeping track of it
                # both here and above, when we bind `_axis` and `_index` variables.
                # import pdb; pdb.set_trace()
                if self._axis == 1:
                    out = pd.DataFrame(self._results, index=self._index)
                else:
                    out = pd.concat(self._results, axis=1)
                    out.columns = self._index
                return out
            elif isinstance(self._results[0], DataFrame):
                pass
            else:
                out = pd.Series(self._results)
                out.index = self._index[:len(out)]
                return out

    def flush(self):
        """
        Flushes the contents of the `checkpoints` state machine.
        """
        self._results = []
        self._index = None
        self._caller = None


checkpoints = CheckpointStateMachine()
disable = checkpoints.disable
enable = checkpoints.enable
flush = checkpoints.flush
