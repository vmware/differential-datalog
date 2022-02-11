# Contributing to the Differential Datalog project

We appreciate your interest in contributing to DDlog.

## Project management

We mostly use github tools for managing the project.  Bugs and
questions should be filed as [github
issues](https://github.com/vmware/differential-datalog/issues).
Improvements should be submitted as [pull
requests](https://github.com/vmware/differential-datalog/pulls).

## Contributor License Agreement

You will need to sign a CLA (Contributor License Agreement) to
contribute code to Hillview under an MIT license.  This is very
standard.

## Using git to contribute.

Fork the repository using the "fork" button on github, by following these instructions:
https://help.github.com/articles/fork-a-repo/

Here is a step-by-step guide to submitting contributions:

1. Create a new branch for each fix; give it a nice suggestive name:
   - `git branch yourBranchName`
   - `git checkout yourBranchName`
2. `git add <files that changed>`
3. `git commit -s -m "Description of commit"`
   Use separate commits with informative messages for each logically independent change.
4. `git fetch upstream`
5. `git rebase upstream/master`
6. Resolve conflicts, if any
   (rebase won't work if you don't; as you find conflicts you will need
    to `git add` the files you have merged, and then you may need to use
    `git rebase --continue` or `git rebase --skip`)
7. Test, analyze merged version.
8. `git push -f origin yourBranchName`.
9. Create a pull request to merge your new branch into master (using the web ui).
10. Delete your branch after the merging has been done `git branch -D yourBranchName`

## Running tests

The tests are run by a shell script in the root directory called `test.sh`.
To run all tests invoke:

> ./test.sh all

To get a list of all the tests you can just invoke `./test.sh` without
any arguments.  It will print a list of test names.