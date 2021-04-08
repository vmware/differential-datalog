#!/bin/sh

# Adapted from https://github.com/commercialhaskell/stack

set -eux

retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

fetch_stack_osx() {
  curl -skL https://www.stackage.org/stack/osx-x86_64 | tar xz --strip-components=1 --include '*/stack' -C ~/.local/bin
}

fetch_stack_linux() {
  curl -sL https://www.stackage.org/stack/linux-x86_64 | tar xz --wildcards --strip-components=1 -C ~/.local/bin '*/stack'
}

fetch_stack_windows() {
  curl -L https://www.stackage.org/stack/windows-x86_64 > stack.exe && unzip stack.exe -d ~/.local/bin
}

# We need stack to generate cabal files with precise bounds, even for cabal
# builds.
pwd
mkdir -p ~/.local/bin
if [ "$(uname)" = "Darwin" ]; then
  retry fetch_stack_osx
elif [ "$(uname)" = "Linux" ]; then
  retry fetch_stack_linux
else
  retry fetch_stack_windows
  # On Travis windows stack setup below fails with an error about
  # certificates missing.  So we install ghc manually.
  if [ "x${TRAVIS}" = "xtrue" ]; then
      choco install haskell-dev
  fi
fi

retry stack --no-terminal setup
