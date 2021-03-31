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
mkdir -p ~/.local/bin
if [ "$(uname)" = "Darwin" ]; then
  retry fetch_stack_osx
elif [ "$(uname)" = "Linux" ]; then
  retry fetch_stack_linux
else
  retry fetch_stack_windows
  # This certificate file was downloaded by hand using a web browser, by navigating
  # to downloads.haskell.org and clicking the "lock" icon.
  # In the absence of this certificate the stack setup command below fails on Windows
  # This file may need to be renewed when the certificate expires or is revoked.
  certutil -addstore -f "CA" ~/tools/haskell-org.pem
fi

retry stack --no-terminal setup
