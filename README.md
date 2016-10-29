# checkpoints ![t](https://img.shields.io/badge/status-alpha-red.svg)

`checkpoints` is a small (for now) module that imports new `pandas.DataFrame.safe_apply` and `pandas.Series.safe_map`
expressions, stop-and-start versions of the `pandas.DataFrame.apply` and `pandas.Series.map` operations which caches
partial results in between runtimes in case an exception is thrown.

This means that the next time these functions are called, the operation will pick up back where it failed, instead
of all the way back at the beginning of the map. After all, there's nothing more aggrevating than waiting ages for a
process to complete, only to lose all of your data on the last iteration!

Just `pip install checkpoints` to get started.

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

### Illustration

Suppose that we have a list of phone numbers which we believe corresponds to restaurants in New York City (e.g. from
the city's [restaurant inspection data](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j)), and we would like to match those
with currently opened locations using the [Yelp! API](https://www.yelp.com/developers/documentation/v2/phone_search).

In other words, we are performing a fuzzy match, throwing over 27000 phone numbers of indeterminate quality at a
(slow, networked) external API whose corner cases we don't immediately understand. At first, we might write something
 that looks like this:

    # Ignoring API auth and data source details
    >>> def yelp(num):
            business = client.phone_search(num).businesses[0]
            return {'Yelp Name': business.name,
                    'Yelp Address': business.location.address,
                    'Yelp Latitude': business.location.coordinate.latitude,
                    'Yelp Longitude': business.location.coordinate.longitude}

    >>> yelp_data = restaurants['PHONE'].map(yelp)

What could go wrong? Lots. In order of increasing subtlety:

* The list could contain `np.nan` values.
* The API could fail to find a location with this associated phone number.
* The API could return only partial information, lacking a `business.location` object.
* The API could return only partial information, lacking a `business.location.coordinate` object.
* The list could contain obviously invalid entries (e.g. entry number 2702 is `__________`).
* The list could contain subtly invalid entries&mdash;numbers, but not phone numbers (e.g. entry number 23720
is `1646644665.0`).
* The network could glitch and fail on you mid-process, causing an `HTTPError`.

If you are initially ignorant of these problems, your `apply` will stumble into these, forcing you to fix each in
turn, losing all of the data from your previous partial runs in the process. With the second-to-last error especially,
you'll run for 22 minutes before you error out and lose everything, and if you're foolish enough to try to do all
27000+ numbers in one run, the second-to-last issue could potentially kill your process after 2 **hours** of processing
 time.

`checkpoints` provides a better way, using a newly registered `pandas.Series.safe_map` method:

    >>> yelp_data = restaurants['PHONE'].safe_map(yelp)

Every time this function is called, the operation will pick up back where it failed, instead of starting all over
again. This really very simple change makes it completely safe to just go ahead and chuck everything at it, all at
once; any errors you encounter you can fix by patching the `yelp` method along the way, without losing
any results in the middle of things.

Put another way, here's the completely correct `yelp` method that you **wouldn't** have to come up with ahead of time:

    def yelp(num):
        if not num:
            return None
        else:
            try:
                business = client.phone_search(num).businesses[0]
                if business and business.location and business.location.coordinate:
                    return {'Yelp Name': business.name,
                            'Yelp Address': business.location.address,
                            'Yelp Latitude': business.location.coordinate.latitude,
                            'Yelp Longitude': business.location.coordinate.longitude}
                else:  # Partial information, skip.
                    return None
            except IndexError:  # Phone search failed!
                return None  # Phone search failed!
            except yelp.errors.InvalidParameter:  # Invalid number!
                return None

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
