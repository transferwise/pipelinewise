#!/bin/bash
#
# Auto generate and deploy documentation to github pages at http://transferwise.github.io/pipelinewise
#
# Script is using the idea of a blog post at https://www.alkaline-ml.com/2018-12-23-automate-gh-builds/

# Fail out on an error
set -e

# This is a hack, but we have to make sure we're only ever running this
# from the top level of the package and not in the subdirectory...
# Shouldn't ever be an issue the way we've got this setup, and you'll
# want to change it a bit to make it work with your project structure.
if [[ ! -f .circleci/config.yml || ! -f .github/config.yml ]]; then
    echo "This must be run from the gh_doc_automation project directory"
    exit 1
fi

# Check if every environment variable set
if [ -z "$GH_NAME" ]; then
    echo "GH_NAME environment variable must be set. This is required to commit and push the generated doc to github. Tip: generate a Personal GitHub Access Token."
    exit 1
fi

if [ -z "$GH_EMAIL" ]; then
    echo "GH_EMAIL environment variable must be set. This is required to commit and push the generated doc to github. Tip: generate a Personal GitHub Access Token."
    exit 1
fi

if [ -z "$GH_TOKEN" ]; then
    echo "GH_TOKEN environment variable must be set. This is required to commit and push the generated doc to github. Tip: generate a Personal GitHub Access Token."
    exit 1
fi

# Install dependencies in a virtual env
python3 -m venv ~/venv-doc
. ~/venv-doc/bin/activate
pip install --upgrade pip
pip install sphinx sphinx-rtd-theme

# CD into docs, make them. If you're not using Sphinx, you'll probably
# have a different build script.
cd docs
make clean html
cd ..

# Move the docs to the top-level directory, stash for checkout
mv docs/_build/html ./

# The html/ directory will stay there when we stash
git stash

# Checkout our gh-pages branch, remove everything but .git
git checkout gh-pages
git pull origin gh-pages

# Make sure to set the credentials! You'll need these environment vars
# set in the "Environment Variables" section in Circle CI
git config user.email "$GH_EMAIL" > /dev/null 2>&1
git config user.name "$GH_NAME" > /dev/null 2>&1

# Remove all files that are not in the .git dir
find . -not -name ".git/*" -type f -maxdepth 1 -delete

# Remove the remaining artifacts. Some of these are artifacts of the
# LAST gh-pages build, and others are remnants of the package itself.
# You will have to amend this to be more specific to your project.
declare -a leftover=(".cache/"
                     ".idea/"
                     ".vscode/"
                     ".virtualenvs/"
                     "docs/"
                     "_downloads/"
                     "_images/"
                     "_modules/"
                     "_sources/"
                     "_static/")

# Check for each left over file/dir and remove it, or echo that it's
# not there.
for left in "${leftover[@]}"
do
    rm -rf "$left"
done

# We need this empty file for git not to try to build a jekyll project.
# If your project *is* Jekyll, then this doesn't apply to you...
touch .nojekyll
cp -R html/* ./
rm -r html/

# Add everything, get ready for commit. But only do it if we're on
# master. If you want to deploy on different branches, you can change
# this.
echo "Current branch ref: $GITHUB_REF"
if [[ "$GITHUB_REF" =~ ^refs/heads/master$|^[0-9]+\.[0-9]+\.X$ ]]; then
    git add --all
    # Make sure "|| echo" is at the end to avoid error codes when no changes to commit
    git commit -m "[ci skip] publishing updated documentation..." || echo 

    # We have to re-add the origin with the GH_TOKEN credentials. You
    # will need this SSH key in your environment variables on Circle.
    # Make sure you change the <project>.git pattern at the end!
    git remote rm origin
    git remote add origin https://"$GH_NAME":"$GH_TOKEN"@github.com/transferwise/pipelinewise.git

    # NOW we should be able to push it
    git push origin gh-pages
else
    echo "Not on master branch, so won't push doc"
fi
