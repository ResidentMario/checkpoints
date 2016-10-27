# # This code can be put in any Python module, it does not require IPython
# # itself to be running already.  It only creates the magics subclass but
# # doesn't instantiate it yet.
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic)
import pandas as pd
from pandas.core.frame import DataFrame, Series
import math


# The class MUST call this class decorator at creation time
@magics_class
class CheckpointMagics(Magics):

    def __init__(self, **kwargs):
        super(CheckpointMagics, self).__init__(**kwargs)
        self.enabled = False
        self.apply = DataFrame.apply
        self.map = Series.map
        self.applymap = DataFrame.applymap
        self.results = []

    @line_magic
    def checkpoints(self, line):
        if line == "enable":
            self.enable()
        elif line == "disable":
            self.disable()

    def disable(self):
        DataFrame.apply = self.apply
        Series.map = self.map
        DataFrame.applymap = self.applymap

    def enable(self):
        from pandas.core.frame import DataFrame, Series

        def safe_apply(df, func, **kwargs):

            # Shorten the jobs list to the DataFrame elements remaining.
            df_remaining = df.iloc[len(self.results):]

            # Replace the original applied `func` with a stateful wrapper, `new_func`.
            def new_func(srs):
                # import pdb; pdb.set_trace()
                success = True
                try:
                    self.results.append(func(srs))
                except Exception as e:
                    success = False
                    raise
                finally:
                    if not success:
                        print(self.results)
                        self.results = self.results[math.ceil(len(self.results) / 2):]

            # Populate `self.results`.
            df_remaining.apply(new_func, **kwargs)

            # If we got here, then we didn't exit out due to an exception, and we can finish the method successfully.
            # Let `pandas.apply` handle concatenation using a trivial combiner.
            out = pd.Series(range(len(self.results))).apply(lambda i: self.results[i])
            out.index = df.index

            # Reset the results set for the next iteration.
            self.results = []

            # Return
            return out

        DataFrame.safe_apply = safe_apply

def load_ipython_extension(ipython):
    ip = ipython
    ip.register_magics(CheckpointMagics)

if __name__ == "__main__":
    load_ipython_extension(get_ipython())