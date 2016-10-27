# # This code can be put in any Python module, it does not require IPython
# # itself to be running already.  It only creates the magics subclass but
# # doesn't instantiate it yet.
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic)
import pandas as pd
from pandas.core.frame import DataFrame, Series


# The class MUST call this class decorator at creation time
@magics_class
class CheckpointMagics(Magics):

    def __init__(self, **kwargs):
        super(CheckpointMagics, self).__init__(**kwargs)
        self.results = []

    @line_magic
    def checkpoints(self, line):
        if line == "enable":
            self.enable()
        elif line == "disable":
            self.disable()

    @staticmethod
    def disable():
        del DataFrame.safe_apply

    def enable(self):
        from pandas.core.frame import DataFrame, Series

        def safe_apply(df, func, **kwargs):

            # If flushed, restart from scratch.
            if 'flush' in kwargs and kwargs['flush'] == True:
                self.results = []

            # Shorten the jobs list to the DataFrame elements remaining.
            df_remaining = df.iloc[len(self.results):]
            # print(len(df_remaining))

            # Replace the original applied `func` with a stateful wrapper, `new_func`.
            def new_func(srs):
                # import pdb; pdb.set_trace()
                try:
                    self.results.append(func(srs))
                except Exception as e:
                    print("Failure on index {0}".format(len(self.results)))
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
                # import pdb; pdb.set_trace()
                try:
                    self.results.append(func(val))
                except Exception:
                    print("Failure on index {0}".format(len(self.results)))
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


def load_ipython_extension(ipython):
    ip = ipython
    ip.register_magics(CheckpointMagics)

if __name__ == "__main__":
    load_ipython_extension(get_ipython())