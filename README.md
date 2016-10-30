# checkpoints [![PyPi version](https://img.shields.io/pypi/v/checkpoints.svg)](https://pypi.python.org/pypi/missingno/) ![t](https://img.shields.io/badge/status-alpha-red.svg)

![demo](http://i.imgur.com/paxQ51Y.gif)

`checkpoints` is an experimental module which imports new `pandas.DataFrame.safe_apply` and `pandas.Series.safe_map`
expressions, stop-and-start versions of the `pandas.DataFrame.apply` and `pandas.Series.map` operations which caches
partial results in between runtimes in case an exception is thrown.

This means that the next time these functions are called, the operation will pick up back where it failed, instead
of all the way back at the beginning of the map. After all, there's nothing more aggrevating than waiting ages for a
process to complete, only to lose all of your data on the last iteration!

Just `pip install checkpoints` to get started.

## Why?

For a writeup with a practical example of what `checkpoints` can do for you see [this post on my personal blog](http://www.residentmar.io/2016/10/29/saving-progress-pandas.html).

## Quickstart

To start, import `checkpoints` and enable it:

    >>> from checkpoints import checkpoints
    >>> checkpoints.enable()

This will augment your environment with `pandas.Series.safe_map` and `pandas.DataFrame.safe_apply` methods. Now
suppose we create a `Series` of floats, except for one invalid entry smack in the middle:

    >>> import pandas as pd; import numpy as np
    >>> rand = pd.Series(np.random.random(100))
    >>> rand[50] = "____"

Suppose we want to remean this data. If we apply a naive `map`:

    >>> rand.map(lambda v: v - 0.5)

        TypeError: unsupported operand type(s) for -: 'str' and 'float'

Not only are the results up to that point lost, but we're also not actually told where the failure occurs! Using
`safe_map` instead:

    >>> rand.safe_map(lambda v: v - 0.5)

        <ROOT>/checkpoint/checkpoints/checkpoints.py:96: UserWarning: Failure on index 50
        TypeError: unsupported operand type(s) for -: 'str' and 'float'

All of the prior results are cached, and we can retrieve them at will with `checkpoints.results`:

    >>> checkpoints.results

        0    -0.189003
        1     0.337332
        2    -0.143698
        3    -0.312296
        ...
        47   -0.188995
        48   -0.286550
        49   -0.258107
        dtype: float64

`checkpoints` will store the partial results until either the process fully completes or it is explicitly told to get
 rid of them using `checkpoints.flush()`:

    >>> checkpoints.flush()
    >>> checkpoints.results
        None

You can also induce this by passing a `flush=True` argument to `safe_map`.

`pd.DataFrame.safe_apply` is similar:

    >>> rand = pd.DataFrame(np.random.random(100).reshape((20,5)))
    >>> rand[2][10] = "____"
    >>> rand.apply(lambda srs: srs.sum())

        TypeError: unsupported operand type(s) for +: 'float' and 'str'

    >>> rand.safe_apply(lambda srs: srs.sum())

        <ROOT>/checkpoint/checkpoints/checkpoints.py:49: UserWarning: Failure on index 2
        TypeError: unsupported operand type(s) for +: 'float' and 'str'

    >>> checkpoints.results

        0    9.273607
        1    8.259637
        2    8.359239
        3    7.873243
        dtype: float64

Finally, the disable checkpoints:

    >>> checkpoints.disable()

## Performance

Maintaining checkpoints introduces some overhead, but really not that much. `DataFrame` performance differs by a
reasonably small constant factor, while `Series` performance is one-to-one:

![Performance charts](http://i.imgur.com/jFIgXOG.png)

## Technicals

Under the hood, `checkpoints` implements a [state machine](https://en.wikipedia.org/wiki/Finite-state_machine),
`CheckpointStateMachine`, which uses a simple list to keep track of which entries have and haven't been mapped yet.
The function fed to `safe_*` is placed in a `wrapper` which redirects its output to a `results` list. When a map is
interrupted midway, then rerun, `safe_*` partitions the input, using the length of `results` to return to the first
non-outputted entry, and continues to run the `wrapper` on that slice.

An actual `pandas` object isn't generated until **all** entries have been mapped. At that point `results` is
repackaged into a `Series` or `DataFrame`, wiped, and a `pandas` object is returned, leaving `CheckpointStateMachine`
 ready to handle the next set of inputs.

## Limitations

* Another feature useful for long-running methods are progress bars, but as of now there is no way to integrate
`checkpoints` with e.g. [`tqdm`](https://github.com/tqdm/tqdm). The workaround is to estimate the time cost of your
process beforehand.
* `pandas.DataFrame.safe_apply` jobs on functions returning `DataFrame` are not currently implemented, and will
simply return `None`. This means that e.g. the following will silently fail:

    `>>> pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).safe_apply(lambda v: pd.DataFrame({'a': [1, 2], 'b': [2, 3]}))`


* The `Series.map` `na_action` parameter is not implemented; nor are any of `broadcast`, `raw`, or `reduce` for
`DataFrame.apply`.

## See also

`checkpoints` provides a form of [defensive programming](https://en.wikipedia.org/wiki/Defensive_programming). If
you're a fan of this sort of thing, you should also check out [`engarde`](https://github.com/TomAugspurger/engarde).

## Contributing

Bugs? Thoughts? Feature requests? [Throw them at the bug tracker and I'll take a look](https://github.com/ResidentMario/missingno/issues).

As always I'm very interested in hearing feedback&mdash;reach out to me at `aleksey@residentmar.io`.
