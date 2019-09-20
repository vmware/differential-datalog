# GitLab CI setup

- GitLab mirror of the DDlog repo (required for integration with GitLab CI):
    - https://gitlab.com/ryzhyk/differential-datalog
    - TODO: create a GitLab org to host the repo instead
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
