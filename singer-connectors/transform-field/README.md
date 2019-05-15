# transform-field

Transformation component between [Singer](https://www.singer.io/) taps and targets.

### To run

Put it between a tap and a target with simple unix pipes:

`some-singer-tap | transform-field --transformations [transformations.json] | some-singer-target`


### To run tests:

1. Install python dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install nose
```

1. To run tests:
```
  nosetests --where=tests
```

