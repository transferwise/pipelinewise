# PipelineWise Documentation

The documentation uses [Sphinx](https://www.sphinx-doc.org/) and the
[Read the Docs Theme](https://sphinx-rtd-theme.readthedocs.io).

## To build the documentation

1. Install the Python dependencies and generate the HTML documentation:

```
pip install sphinx sphinx-rtd-theme
make html
```

The generated documentation is written to `_build/html`. Open
`_build/html/index.html` in your browser.

## Automatic deployment

The `publish_doc` GitHub Actions workflow builds and publishes the documentation
to [PipelineWise GitHub Pages](https://transferwise.github.io/pipelinewise/) after
changes are merged.
