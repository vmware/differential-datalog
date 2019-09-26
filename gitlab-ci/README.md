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
  the next periodic scan (every 30 minutes).  To kick off the CI pipeline
  instantly, go to `Settings->Repository->Mirroring repositories` and click
  `Update now`.  You can see the status of the CI pipeline under the `CI/CD`
  tab.
- TODO: Figure out how to add a webhook to start GitLab CI tests instantly.
