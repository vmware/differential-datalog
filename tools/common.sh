#!/bin/sh

retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

HOME_LOCAL_BIN=${HOME}/.local/bin

create_home_local_bin() {
	if [ ! -d ${HOME_LOCAL_BIN} ]; then
		mkdir -p ${HOME_LOCAL_BIN}
	fi
}

export PATH=${HOME_LOCAL_BIN}:${PATH}
