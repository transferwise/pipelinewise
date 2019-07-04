# PipelineWise Documentation

The documentation is using [Sphinx](http://www.sphinx-doc.org) and the the popular
[Read the Docs Theme](https://sphinx-rtd-theme.readthedocs.io).

## To build the documentation

1. Install python dependencies and generate the HTML documentation
```
  pip install sphinx sphinx-rtd-theme
  make html
```

The document generating into `_build/html`. Open `index.html` in your web browser.

 ## Auto Deployment

CircleCI automatically generating and publishing the documentation on every merged
commit to the PipelineWise GitHub Pages at http://transferwise.github.io/pipelinewise.
