import collections

# Named tuples for holding add, change, and remove operations
Add = collections.namedtuple('Add', ['path', 'newval'])
Change = collections.namedtuple('Change', ['path', 'oldval', 'newval'])
Remove = collections.namedtuple('Remove', ['path', 'oldval'])


def paths(data, base=None):
    """Walk a data structure and return a list of (path, value) tuples, where
    each path is the path to a leaf node in the data structure and the
    value is the value it points to. Each path will be a tuple.

    """
    if base is None:
        base = ()

    result = []
    if isinstance(data, dict):
        for key, val in sorted(data.items()):
            result.extend(paths(val, base + (key,)))

    elif isinstance(data, list):
        for i, val in enumerate(data):
            result.extend(paths(val, base + (i,)))

    elif base:
        result.append((base, data))

    return result


def diff(oldstate, newstate):
    """Compare two states, returning a list of Add, Change, and Remove
    objects.

    Add(path, newval) means path exists in newstate but not oldstate and
    its value in newstate is newval.

    Change(path, oldval, newval) means that path exists in both oldstate
    and newstate but has different values. oldval is the val in oldstate
    and newval is the value in newstate.

    Remove(path, oldval) means the path exists in oldstate but not in
    newstate, and the value in oldstate is oldval.

    """

    # Convert oldstate and newstate from a deeply nested dict into a
    # single-level dict, mapping a path to a value.
    olddict = dict(paths(oldstate))
    newdict = dict(paths(newstate))

    # Build the list of all paths in both oldstate and newstate to iterate
    # over.
    all_paths = set()
    all_paths.update(set(olddict.keys()))
    all_paths.update(set(newdict.keys()))

    result = []
    for path in sorted(all_paths):
        if path in olddict:
            if path in newdict:
                if olddict[path] == newdict[path]:
                    pass # Don't emit anything if values are the same
                else:
                    result.append(Change(path, olddict[path], newdict[path]))
            else:
                result.append(Remove(path, olddict[path]))
        else:
            result.append(Add(path, newdict[path]))
    return result
