#!/bin/bash
set -e
set -u
set -o pipefail

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__proj_dir="$(dirname "$__dir")"
__proj_name="$(basename "$__proj_dir")"

. "${__dir}/common.sh"

acbuild_ver="v0.4.0"

if [[ -d "${__proj_dir}/dist" ]]; then
    _warning "dist already exists.  Hint: run make clean"
    exit 0
fi

mkdir -p "${__proj_dir}/dist"

_debug "verifying we are running on a Linux system"
if [[ $OSTYPE != "linux-gnu" ]]
then
    _error "This script can only be run from a Linux system"
    exit 1
fi

_info "Downloading acbuild tool"
set +e
$(cd ${__proj_dir}/dist && mkdir acbuild-${acbuild_ver} && wget -L -O- https://github.com/containers/build/releases/download/${acbuild_ver}/acbuild-${acbuild_ver}.tar.gz | tar zxv)
set -e
PATH="$PATH:${__proj_dir}/dist/acbuild-${acbuild_ver}"

_info "packaging ${__proj_dir}"

_info "running: acbuild begin"
acbuild begin

_info "running: acbuild set-name ${__proj_name}"
acbuild set-name ${__proj_name}

_info "running: rsync $VIRTUAL_ENV .venv-relocatable -a --copy-links -v"
rsync $VIRTUAL_ENV/ .venv-relocatable -q --delete -a --copy-links -v

_info "running: acbuild copy $VIRTUAL_ENV .venv-relocatable"
acbuild copy .venv-relocatable .venv

_info "running: acbuild copy ${__proj_dir}/plugin.py plugin.py"
acbuild copy ${__proj_dir}/plugin.py plugin.py

_info "running: acbuild set-exec ./.venv/bin/python plugin"
acbuild set-exec ./.venv/bin/python plugin.py

_info "running: write ${__proj_dir}/dist/${__proj_name}.aci"
mkdir -p "${__proj_dir}/dist"
acbuild write ${__proj_dir}/dist/${__proj_name}.aci

_info "running: acbuild end"
acbuild end

_info "running: rm -rf ${__proj_dir}/acbuild-${acbuild_ver}"
rm -rf ${__proj_dir}/dist/acbuild-${acbuild_ver}

_info "done"