funcs = []


def enable(func):
    def wrapper(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print("Error caught!")
            raise e
    return wrapper(func, *args, **kwargs)

# # This code can be put in any Python module, it does not require IPython
# # itself to be running already.  It only creates the magics subclass but
# # doesn't instantiate it yet.
# from IPython.core.magic import (Magics, magics_class, line_magic,
#                                 cell_magic)
# import pandas as pd
#
#
# # The class MUST call this class decorator at creation time
# @magics_class
# class CheckpointMagics(Magics):
#
#     def __init__(self, **kwargs):
#         super(CheckpointMagics, self).__init__(**kwargs)
#         self.enabled = False
#         self.apply = pd.DataFrame.apply
#         self.map = pd.Series.map
#         self.applymap = pd.DataFrame.applymap
#
#     @line_magic
#     def checkpoints(self, line):
#         if line == "enabled":
#             self.enable()
#         elif line == "disabled":
#             self.disable()
#
#     def disable(self):
#         pd.DataFrame.apply = self.apply
#         pd.Series.map = self.map
#         pd.DataFrame.applymap = self.applymap
#
#
# def load_ipython_extension(ipython):
#     ip = ipython
#     ip.register_magics(CheckpointMagics)
#
# if __name__ == "__main__":
#     load_ipython_extension(get_ipython())