#!/bin/sh

# Adapted from https://github.com/commercialhaskell/stack

set -eux

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


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
  retry stack --no-terminal setup
elif [ "$(uname)" = "Linux" ]; then
  retry fetch_stack_linux
  retry stack --no-terminal setup
else
  retry fetch_stack_windows
  if [ -z "${TRAVIS}" ]; then
      retry stack --no-terminal setup
  else
      # Travis Windows VM lacks root CA certificate to validate
      # download.haskell.org
      SSL_CERT_FILE=${THIS_DIR}/ca-certificates.crt retry stack --no-terminal setup
  fi
fi

