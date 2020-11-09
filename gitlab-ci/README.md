# GitLab CI setup

- GitLab mirror of the DDlog repo (required for integration with GitLab CI):
    - https://gitlab.com/ddlog/differential-datalog

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
    - To clean Docker cache when it starts to take too much space (happens after
      running CI for a while):
        `docker system prune -a`
        `docker system prune --volumes`
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

A copy of `/etc/gitlab-runner/config.toml`:

```
concurrent = 8
check_interval = 0

[session_server]
  session_timeout = 1800

[[runners]]
  name = "ddlog-ci-1"
  url = "https://gitlab.com"
  token = "Bjx-aJUGEH6Fp6rBEXkf"
  executor = "docker"
  [runners.custom_build_dir]
  [runners.docker]
    tls_verify = false
    image = "ddlog/gitlab-ci:latest"
    privileged = false
    disable_entrypoint_overwrite = false
    oom_kill_disable = false
    disable_cache = false
    volumes = ["/cache"]
    shm_size = 0
    network_mode = "host"
    limit = 1
  [runners.cache]
    [runners.cache.s3]
    [runners.cache.gcs]
```

Since docker hub is now throttling pulls from unauthenticated users, I also
created registry mirrors on all machines that run gitlab runners by following
[these instructions](https://about.gitlab.com/blog/2020/10/30/mitigating-the-impact-of-docker-hub-pull-requests-limits/),
namely:

```
docker run -d -p 6000:5000 \
    -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io \
    --restart always \
    --name registry registry:2
```

and added the following to docker's `daemon.json`:

```
{
  "registry-mirrors": ["http://127.0.0.1:6000"]
}
```


## Creating a GitLab mirror for your own fork of DDlog

Follow these steps to connect your fork of the DDlog repo to GitLab CI,
so that you can test your changes, e.g., before submitting a PR against
the main DDlog repo.

- You will need a [gitlab.com](https://gitlab.com) account.
- Log into your GitLab account.
- Click `New Project`.
- Choose `CI/CD for external repo` project type.
- Specify your fork of `differential-datalog` as the repository to import.
- That's it. GitLab should now automatically pick up your changes and start the
  CI pipeline.
- In order to use VMware-hosted GitLab runners, you need to transfer the
  mirror to the DDlog organization.  To do this, first change the path
  to the mirror (under `Settings->General->Advanced->Change path`)
  from `differential-datalog` to, e.g., `differential-datalog-user-name`
  and then use `Settings->General->Advanced->Transfer project` to move
  the project to the DDlog org (you must become an owner of the org first).
