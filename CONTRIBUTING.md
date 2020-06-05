# Contributing to PipelineWise

When contributing to this repository, please first discuss the change you wish to make via issue, [slack channel](https://singer-io.slack.com/archives/CNL7DL597), or any other method with the owners of this repository before making a change.

We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

Please keep all your communication respectful. 

## On PipelineWise at TransferWise
PipelineWise is the ELT engine used at TransferWise to move data from +150 sources to different targets, 
the main sources include Mysql/MariaDB, Postgres, S3 buckets and targets include Snowflake and S3 buckets, 
this means we are extra careful when making changes to/dealing with PRs touching any of the connectors that are used at TransferWise.


## We Develop with Github
We use github to host code, to track public issues and feature requests from the community, as well as accept pull requests.


## We Use [Github Flow](https://guides.github.com/introduction/flow/index.html), So All Code Changes Happen Through Pull Requests
Pull requests are the best way to propose changes to the codebase (we use [Github Flow](https://guides.github.com/introduction/flow/index.html)). We actively welcome your pull requests:

1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add tests: unit and End-2-End if possible.
3. If you've added a new feature or changed the behavior of existing one, update the tests and the relevant documentation in [README.md](./README.md) and [online documentation code](./docs).
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

Pull requests trigger CI checks which run on a private CircleCI hosted on TransferWise's infrastructure.  


## Any contributions you make will be under the Apache License Version 2.0.
In short, when you submit code changes, your submissions are understood to be under the same [Apache License Version 2.0](./LICENSE) that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using Github's [issues](https://github.com/transferwise/pipelinewise/issues)
We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/transferwise/pipelinewise/issues/new); it's that easy!

## Write bug reports with detail, background and setup
Here's [a great example from Craig Hockenberry](http://www.openradar.me/11905408)

**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Describe your source/target setup.
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

People *love* thorough bug reports. I'm not even kidding.

## Use a Consistent Coding Style

* Tabs for indentation 
* Google docstring format for Python documentation.
* Single quotes for string literals 
* We've started using SonarLint PyCharm plugin to detect code complexity among other issues to improve code quality.
* You can try running `find pipelinewise tests -type f -name '*.py' | xargs unify --check-only` and `pylint pipelinewise tests` for style unification

## Versioning
We use [Semantic versioning](https://semver.org/).

## License
By contributing, you agree that your contributions will be licensed under its Apache License Version 2.0
