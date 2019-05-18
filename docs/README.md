# PipelineWise Documentation

PipelineWise is a Data Pipeline Framework using the [Singer.io ETL specification](https://singer.io)
specification to ingest and replicate data from various sources to various destinations.

The documentation is using [Sphinx](http://www.sphinx-doc.org) and the the popular
[Read the Docs Theme](https://sphinx-rtd-theme.readthedocs.io).

## To build the documentation

1. Install python dependencies and run python linter
```
  pip install sphinx sphinx-rtd-theme
  make html
```

The document generating into `_build/html`. Open `index.html` in your web browser.

 ## Auto Deployment

CircleCI automatically generating and publishing the documentation on every merged
commit to the PipelineWise GitHub Pages at http://transferwise.github.io/pipelinewise.
