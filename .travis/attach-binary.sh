#!/bin/sh
set -o errexit -o verbose

OWNER=ryzhyk
REPO=ddlog
LIBS=./lib
JAVA_FILES=./java

if test ! "$BUILD_BINARY" || test ! "$TRAVIS_TAG"
then
  echo 'This is not a release build.'
  exit 1
elif test ! "$GITHUB_TOKEN"
then
  echo 'The GITHUB_TOKEN environment variable is not set!'
  exit 2
else
  echo "Building binary for $TRAVIS_OS_NAME to $TRAVIS_TAG..."
  stack build --ghc-options -O2 --pedantic
  echo "Attaching binary for $TRAVIS_OS_NAME to $TRAVIS_TAG..."
  #OWNER="$(echo "$TRAVIS_REPO_SLUG" | cut -f1 -d/)"
  DIST_DIR=./$REPO
  mkdir -p "$DIST_DIR/bin"
  BIN="$(stack path --local-install-root)/bin/ddlog"
  BUNDLE_NAME="$REPO-$TRAVIS_TAG-$TRAVIS_OS_NAME.tar.gz"
  cp "$BIN" "$DIST_DIR/bin/"
  cp -r $LIBS "$DIST_DIR/"
  cp -r $JAVA_FILES "$DIST_DIR/"
  tar -czf "$BUNDLE_NAME" "$DIST_DIR"
  echo "SHA256:"
  shasum -a 256 "$BUNDLE_NAME"
  ghr -t "$GITHUB_TOKEN" -u "$OWNER" -r "$REPO" --replace "$(git describe --tags)" "$BUNDLE_NAME"
fi
