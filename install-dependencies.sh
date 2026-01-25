#!/bin/bash
# Script that installs the dependencies needed to build and run Differential Datalog

echo "Installing dependencies for DDlog"
echo "This script should be invoked with '. ./install-dependencies.sh' to set up the environment properly"

require_pkg() {
  local pkg="$1"
  if ! dpkg -s "${pkg}" >/dev/null 2>&1; then
    echo "Installing missing package: ${pkg}"
    sudo apt install -y "${pkg}"
  fi
}

preflight_linux_apt() {
  local pkgs=(
    build-essential
    libgmp-dev
    zlib1g-dev
    libtinfo-dev
    default-jdk
    libgoogle-perftools-dev
  )

  for pkg in "${pkgs[@]}"; do
    require_pkg "${pkg}"
  done

  # If DDLOG_STATIC=1 is set, user wants static linking (requires additional packages)
  if [[ "${DDLOG_STATIC}" == "1" ]]; then
    echo "DDLOG_STATIC=1 set; static linking may require additional libc/libstdc++ static packages."
    echo "If static linking fails, install glibc static libs for your distro or unset DDLOG_STATIC."
  fi
}

case "$OSTYPE" in
    linux*)
      if ! command -v apt >/dev/null 2>&1; then
        echo "Unsupported Linux package manager. Please install dependencies manually."
        exit 1
      fi
      preflight_linux_apt
      ;;
    darwin*) ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1;;
esac

RUST_VERSION="1.93.0"

echo "Installing Haskell"
./tools/install-stack.sh

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain ${RUST_VERSION} -y
export PATH=$HOME/.cargo/bin:$PATH

# If another version is installed the previous line does not do anything
rustup default ${RUST_VERSION}
rustup toolchain install ${RUST_VERSION}
rustup component add rustfmt
rustup component add clippy

pip3 install parglare==0.12.0

./tools/install-flatbuf.sh
cd flatbuffers
export CLASSPATH=`pwd`"/java":$CLASSPATH
export PATH=`pwd`:$PATH
cd ..

GITDIR=$(git rev-parse --git-dir)

# handle if invoked from worktrees
if [ $(git rev-parse --is-inside-work-tree) == "true" ];then
	if [ ! -d ${GITDIR}/hooks ]; then
		echo "Worktree = true"
		echo "Creating hooks directory under ${GITDIR}"
		mkdir ${GITDIR}/hooks
	fi
fi

# Link to pre-commit script
ln -sf $(pwd)/tools/prepush.sh ${GITDIR}/hooks/pre-push
