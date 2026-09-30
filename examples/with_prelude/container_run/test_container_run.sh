#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# Tests for prelude/container/tools/container_run.sh, the `container_run`
# launcher. Usage:
#
#   sh test_container_run.sh [LAUNCHER]
#
# Set SHELLS="dash bash ..." to run the launcher under several shells (default:
# sh, which is bash 3.2 on macOS). Fake `container`, `docker`, `uname`,
# `launchctl` and `sysctl` programs make the macOS code path testable anywhere.

# Literal '$' in single quotes and word splitting of $std/$big are intended.
# shellcheck disable=SC2016,SC2086

set -u
here=$(cd "$(dirname "$0")" && pwd -P)
L=${1:-$here/../../../prelude/container/tools/container_run.sh}
case $L in
    /*) ;;
    *) L=$(pwd -P)/$L ;;
esac
SHELLS=${SHELLS:-sh}
T=$(mktemp -d)
T=$(cd "$T" && pwd -P)
trap 'rm -rf "$T"' EXIT
F=$T/fakes
mkdir -p "$F" "$T/repo/sub" "$T/repo2" "$T/mnt"
ln -s "$T/repo" "$T/link"

cat >"$F/container" <<'EOF'
#!/bin/sh
if [ "${1-}" = system ]; then printf 'FAKE system %s\n' "$*" >&2; exit "${FAKE_START_EXIT:-0}"; fi
printf '<%s>' "$@"; printf '\n'
e=
for k in SECRET BUCK_RUN_BUILD_ID; do
    eval "v=\${$k-__unset__}"
    [ "$v" = __unset__ ] || e="$e $k=[$v]"
done
[ -z "$e" ] || printf 'env:%s\n' "$e"
exit "${FAKE_EXIT:-0}"
EOF
cp "$F/container" "$F/docker"
printf '#!/bin/sh\nprintf "%%s\\n" "${FAKE_UNAME:-Darwin}"\n' >"$F/uname"
printf '#!/bin/sh\nexit "${FAKE_LAUNCHCTL:-0}"\n' >"$F/launchctl"
cat >"$F/sysctl" <<'EOF'
#!/bin/sh
case $2 in
    hw.ncpu) printf '12\n' ;;
    hw.memsize) printf '38654705664\n' ;;
    *) exit 1 ;;
esac
EOF
cat >"$F/inner" <<'EOF'
#!/bin/sh
printf 'inner:'; printf '<%s>' "$@"; printf ' A=[%s]\n' "${A-unset}"; exit "${INNER_EXIT:-0}"
EOF
chmod +x "$F"/*
export PATH="$F:/usr/bin:/bin"
unset BUCK_CONTAINER_RUN BUCK_CONTAINER_RUN_ARGS BUCK_CONTAINER_RUN_VERBOSE BUCK_RUN_BUILD_ID A SECRET
R=$T/repo

fails=0
check() { # NAME EXPECTED ACTUAL
    if [ "$2" = "$3" ]; then
        printf 'ok   %s\n' "$1"
    else
        printf 'FAIL %s\n  expected: %s\n  actual:   %s\n' "$1" "$2" "$3"
        fails=$((fails + 1))
    fi
}

for sh in $SHELLS; do
    printf '## %s\n' "$sh"
    run() {
        $sh "$L" "$@" </dev/null 2>&1
        printf 'rc=%s' "$?"
    }
    base="<run><--rm><-i><--progress><none><--init><--platform><linux/arm64><--label><buck2.target=//t:t><--volume><$R:$R>"
    tail="<--cpus><12><--memory><18432M><--env><BUCK_RUN_BUILD_ID>"
    std="--target //t:t --image img --platform linux/arm64 --cpus host --memory host"

    out=$(cd "$R/sub" && SECRET=s3 BUCK_RUN_BUILD_ID=b1 run $std --project-root "$R" \
        --env-passthrough SECRET --env-passthrough UNSET --env 'A=x y' \
        --run-arg --publish --run-arg 127.0.0.1:80:80 \
        -- /abs/bin 'a b' '' -x "it's" 'q"q' '*' -- '$HOME')
    check "quoting and order" "$base<--workdir><$R/sub>$tail<--env><SECRET><--env><UNSET><--env><A=x y><--publish><127.0.0.1:80:80><--entrypoint></abs/bin><img><a b><><-x><it's><q\"q><*><--><\$HOME>
env: SECRET=[s3] BUCK_RUN_BUILD_ID=[b1]
rc=0" "$out"

    out=$(cd "$T/link/sub" && run $std --project-root "$R" -- /abs/bin)
    check "symlinked cwd maps to the physical path" "$base<--workdir><$R/sub>$tail<--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$T/repo2" && run $std --project-root "$R" -- /abs/bin)
    check "/repo2 is not inside /repo" "container_run: note: '$T/repo2' is not inside the project root '$R'; running in '$R' instead
$base<--workdir><$R>$tail<--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$R" && run $std --project-root . -- buck-out/bin x)
    check "relative root and argv0" "$base<--workdir><$R>$tail<--entrypoint><$R/buck-out/bin><img><x>
rc=0" "$out"

    out=$(cd "$R/sub" && run $std --project-root "$R/" -- python3)
    check "trailing slash on root, bare argv0" "$base<--workdir><$R/sub>$tail<--entrypoint><python3><img>
rc=0" "$out"

    out=$(cd "$R" && run $std --project-root "$R" -- ./rel)
    check "./relative argv0" "$base<--workdir><$R>$tail<--entrypoint><$R/./rel><img>
rc=0" "$out"

    out=$(cd "$T/repo2" && run $std --project-root "$R" -- rel/bin 2>&1 | tail -2)
    check "relative argv0 outside the root" "container_run: cannot run 'rel/bin' in the container: the current directory '$T/repo2' is outside the project root '$R'
rc=125" "$out"

    out=$(cd "$R" && FAKE_UNAME=Linux INNER_EXIT=42 run $std --project-root "$R" --env 'A=b=c' --run-arg --x -- "$F/inner" 'a b' '')
    check "direct on Linux: exec, env, exit code" "inner:<a b><> A=[b=c]
rc=42" "$out"

    out=$(cd "$R" && BUCK_CONTAINER_RUN=never run $std --project-root "$R" -- "$F/inner")
    check "BUCK_CONTAINER_RUN=never" "inner:<> A=[unset]
rc=0" "$out"

    out=$(cd "$R" && FAKE_UNAME=Linux BUCK_CONTAINER_RUN=always run $std --project-root "$R" -- /abs/bin)
    check "BUCK_CONTAINER_RUN=always" "$base<--workdir><$R>$tail<--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$R" && FAKE_EXIT=37 run $std --project-root "$R" -- /abs/bin | tail -1)
    check "container exit code" "rc=37" "$out"

    out=$(cd "$R" && run --cli nosuch $std --project-root "$R" -- /abs/bin)
    check "missing CLI" "125" "${out##*rc=}"
    check "missing CLI message" "container_run: 'nosuch' was not found on PATH or in /usr/local/bin or /opt/homebrew/bin." "$(printf '%s\n' "$out" | head -1)"

    out=$(BUCK_CONTAINER_RUN=yes run -- x)
    check "bad mode" "container_run: BUCK_CONTAINER_RUN must be auto, always or never (got 'yes')
rc=125" "$out"

    out=$(run --image i)
    out=$out$(run --image i --)
    out=$out$(run --bogus -- x)
    out=$out$(run --image)
    out=$out$(FAKE_UNAME=Linux run --env 1A=b -- x)
    check "protocol errors" "container_run: internal error: missing '--' before the command
rc=125container_run: internal error: no command to run
rc=125container_run: internal error: unknown option '--bogus'
rc=125container_run: internal error: option '--image' needs a value
rc=125container_run: internal error: bad variable name in '--env 1A=b'
rc=125" "$out"

    out=$(cd "$R" && FAKE_LAUNCHCTL=1 run $std --project-root "$R" -- /abs/bin | head -2)
    check "services are started when not registered" "container_run: starting container services (container system start)
FAKE system system start --disable-kernel-install" "$out"

    out=$(cd "$R" && FAKE_LAUNCHCTL=1 FAKE_START_EXIT=1 run $std --project-root "$R" -- /abs/bin | tail -1)
    check "failing to start services" "rc=125" "$out"

    out=$(cd "$R" && run $std --project-root "$R" --mount "$T/mnt" --mount /no/such -- /abs/bin)
    check "mounts" "container_run: note: skipping mount '/no/such': it does not exist on this machine
$base<--workdir><$R>$tail<--volume><$T/mnt:$T/mnt><--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$R" && run $std --project-root "$R" --mount /a:b -- /abs/bin)
    check "mount with ':'" "container_run: mount '/a:b' contains ':', which the container CLI cannot mount
rc=125" "$out"

    out=$(cd "$R" && BUCK_CONTAINER_RUN_ARGS='-p 9:9  --memory 4G *' run $std --project-root "$R" -- /abs/bin)
    check "BUCK_CONTAINER_RUN_ARGS" "$base<--workdir><$R>$tail<-p><9:9><--memory><4G><*><--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$R" && run --cli docker --flavor docker $std --project-root "$R" -- /abs/bin)
    check "docker flavor" "<run><--rm><-i><--quiet><--init><--platform><linux/arm64><--label><buck2.target=//t:t><--volume><$R:$R><--workdir><$R><--env><BUCK_RUN_BUILD_ID><--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$R" && run --target '//t:t[a=b]' --image img --project-root "$R" -- /abs/bin)
    check "no label for targets containing '='" "<run><--rm><-i><--progress><none><--init><--volume><$R:$R><--workdir><$R><--env><BUCK_RUN_BUILD_ID><--entrypoint></abs/bin><img>
rc=0" "$out"

    out=$(cd "$R" && BUCK_CONTAINER_RUN_VERBOSE=1 run $std --project-root "$R" -- /abs/bin "it's" | head -1)
    check "verbose" "container_run: running: '$F/container' 'run' '--rm' '-i' '--progress' 'none' '--init' '--platform' 'linux/arm64' '--label' 'buck2.target=//t:t' '--volume' '$R:$R' '--workdir' '$R' '--cpus' '12' '--memory' '18432M' '--env' 'BUCK_RUN_BUILD_ID' '--entrypoint' '/abs/bin' 'img' 'it'\\''s'" "$out"

    # Long argument lists must stay linear: appending to "$@" in a loop is
    # quadratic in bash 3.2.
    big=$(i=0; while [ $i -lt 5000 ]; do printf 'a%s ' $i; i=$((i + 1)); done)
    start=$(date +%s)
    out=$(cd "$R" && run $std --project-root "$R" -- /abs/bin $big | tail -1)
    elapsed=$(($(date +%s) - start))
    check "5000 arguments" "rc=0" "$out"
    check "5000 arguments in under 20s" "fast" "$([ "$elapsed" -lt 20 ] && printf fast || printf 'slow (%ss)' "$elapsed")"
done

if [ "$fails" = 0 ]; then
    printf 'all passed\n'
else
    printf '%s failed\n' "$fails"
    exit 1
fi
