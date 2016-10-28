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

    >>> import checkpoints
    >>> checkpoints.enable()

You can later disable it using `checkpoints.disable()`.

...[WIP]...

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


## Contributing

Bugs? Thoughts? Feature requests? [Throw them at the bug tracker and I'll take a look](https://github.com/ResidentMario/missingno/issues).

As always I'm very interested in hearing feedback&mdash;reach out to me at `aleksey@residentmar.io`.
