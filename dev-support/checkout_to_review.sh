#!/bin/bash

# usage: dev-support/checkout_to_review.sh repo:branch, e.g.: dev-support/checkout_to_review.sh Markz666:newten-7031-2

git clean -f
git checkout -f

echo "the param is: $1 "

OIFS="$IFS"
IFS=':'
read -a repo_and_branch <<< "${1}"
IFS="$OIFS"

git branch -D ${repo_and_branch[1]}
git fetch ${repo_and_branch[0]} ${repo_and_branch[1]}
git checkout -b ${repo_and_branch[1]} ${repo_and_branch[0]}/${repo_and_branch[1]}
