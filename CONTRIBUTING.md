# Contributing to the Differential Datalog project

We appreciate your interest in contributing to DDlog.  Please read our
[Developer Certificate of Origin](https://cla.vmware.com/dco). All
contributions to this repository must be signed as described on that
page. Your signature certifies that you wrote the patch or have the
right to pass it on as an open-source patch.

## Project management

We mostly use github tools for managing the project.  Bugs and
questions should be filed as [github
issues](https://github.com/vmware/differential-datalog/issues).
Improvements should be submitted as [pull
requests](https://github.com/vmware/differential-datalog/pulls).

## Contributor License Agreement

You will need to sign a CLA (Contributor License Agreement) to
contribute code to DDlog under an MIT license.  This is standard.

## Using git to contribute.

Fork the repository using the "fork" button on github, by following these instructions:
https://help.github.com/articles/fork-a-repo/.  The main step is the following:

```shell
git remote add upstream https://github.com/vmware/differential-datalog.git
```

Here is a step-by-step guide to submitting contributions:

1. Create a new branch for each fix; give it a nice suggestive name:
> `git checkout -b yourBranchName`
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

We follow the conventions on [How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/).

Be sure to include any related GitHub issue references in the commit
message.  See [GFM
syntax](https://guides.github.com/features/mastering-markdown/#GitHub-flavored-markdown)
for referencing issues and commits.

## Updating pull requests

If your PR fails to pass CI or needs changes based on code review,
you'll most likely want to squash these changes into existing commits.

If your pull request contains a single commit or your changes are
related to the most recent commit, you can simply amend the commit.

``` shell
git add .
git commit --amend
git push --force-with-lease origin my-new-feature
```

If you need to squash changes into an earlier commit, you can use:

``` shell
git add .
git commit --fixup <commit>
git rebase -i --autosquash main
git push --force-with-lease origin my-new-feature
```

## Running tests

The tests are run by a shell script in the root directory called `test.sh`.
To run all tests invoke:

> ./test.sh all

Running all tests takes a long time.  To get a list of all the tests
you can just invoke `./test.sh` without any arguments.  It will print
a list of test names.  For a faster testing process we recommend:

> ./test.sh simple

Please note that our continuous integration pipeline uses some private
runners to run some costly tests, and thus passing all tests in the
repository may not be enough to validate a PR.
