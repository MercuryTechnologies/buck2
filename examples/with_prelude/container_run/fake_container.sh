#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# Stands in for Apple's `container` CLI in check_container_path.sh: prints the
# volume it was asked to mount, then runs the entrypoint directly with the
# given --workdir and --env K=V.
[ "${1-}" = run ] || exit 0 # e.g. `system start` or the watchdog's `stop`
shift
workdir=.
entry=
while [ "$#" -gt 0 ]; do
    case $1 in
        --volume)
            printf 'volume=%s\n' "$2"
            shift 2
            ;;
        --workdir)
            workdir=$2
            shift 2
            ;;
        --env)
            case $2 in
                *=*) export "${2?}" ;;
            esac
            shift 2
            ;;
        --entrypoint)
            entry=$2
            shift 2
            ;;
        --)
            shift 2 # `--` and the image
            break
            ;;
        --rm | -i | -t | --init | --quiet) shift ;;
        *) shift 2 ;;
    esac
done
cd "$workdir" && exec "$entry" "$@"
