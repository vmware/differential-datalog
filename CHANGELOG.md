# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

- Speedup the compiler: eliminated several performance bottlenecks, most notably
  in the type inference algorithm.  This yields an OOM speedup on large DDlog
  projects.
