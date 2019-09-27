# GitLab CI setup

- GitLab mirror of the DDlog repo (required for integration with GitLab CI):
    - https://gitlab.com/ddlog/differential-datalog
    - TODO: Add a webhook to the github repo to trigger GitLab sync

- Docker container for GitLab CI
    - Docker Hub organization to host GitLab CI container images:
      - https://cloud.docker.com/u/ddlog/repository/docker/ddlog/gitlab-ci
    - Dockerfile to build GitLab CI container images:
      - `tools/Dockerfile` (not the best place for it, but it must be in the same
        directory tree with other installation scripts in `tools`)
    - Script to compile GitLab CI container image using the Dockerfile
      - `./gitlab-ci/build-docker-image.sh`
      - The image only needs to be generated when:
        - Upgrading to a newer version of Rust
        - Upgrading to a newer version of Haskell resolver
        - Upgrading to a newer version of Python
        - New Java, Python or Haskell dependencies are introduced
        - Upgrading to a newer version of FlatBuffers
        - Switching to a different JDK
      - Upon success, the script prints a command line to upload the image
        to Docker Hub.

- GitLab CI script:
    - `.gitlab-ci.yml`

## GitLab runner configuration

Some of the jobs in the CI script require more CPU or memory than available
to GitLab shared runners.  These jobs are tagged to run on project-specific
runners hosted by VMware, e.g.:

```
.test-imported-souffle
    extends: .install-ddlog
    tags:
        - ddlog-ci-1
```

These runners are attached to the DDlog GitLab organization, so in order to use
them with your fork of the DDlog repo, you must transfer the fork (or the GitLab
mirror of the fork) to the DDlog organization (see below).

I followed these [instructions](https://docs.gitlab.com/runner/install/linux-manually.html)
to configure the GitLab runners.

## Creating a GitLab mirror for your own fork of DDlog

Follow these steps to connect your fork of the DDlog repo to GitLab CI,
so that you can test your changes, e.g., before submitting a PR against
the main DDlog repo.

- You will need a [gitlab.com](https://gitlab.com) account.
- Log into your GitLab account.
- Click `New Project`.
- Choose `CI/CD for external repo` project type.
- Specify your fork of `differential-datalog` as the repository to import.
- That's it. GitLab should now automatically pick up your changes during
  the next periodic scan (every 30 minutes).
  <!--To kick off the CI pipeline
  instantly, go to `Settings->Repository->Mirroring repositories` and click
  `Update now`.-->
  You can see the status of the CI pipeline under the `CI/CD` tab.
- In order to use VMware-hosted GitLab runners, you need to transfer the
  mirror to the DDlog organization.  To do this, first change the path
  to the mirror (under `Settings->General->Advanced->Change path`)
  from `differential-datalog` to, e.g., `differential-datalog-user-name`
  and then use `Settings->General->Advanced->Transfer project` to move
  the project to the DDlog org (you must become an owner of the org first).

<!-- TODO: Figure out how to add a webhook to start GitLab CI tests instantly.
-->
