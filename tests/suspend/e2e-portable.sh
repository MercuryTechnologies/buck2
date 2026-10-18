#!/usr/bin/env bash
# Option B end to end on one machine, with remote execution: test plan T1 tests 7 to 9.
#
#   e2e-portable.sh <buck2 binary> <workdir> <RE buckconfig> [watcher]
#
# 7. Main builds at c0 in a checkout at <workdir>/repo, with a configured host identity, and
#    runs `buck2 suspend --export`. The checkout, its buck-out and its daemon dir are deleted.
#    A fresh clone at a later commit c1 goes to the same path, with no buck-out. Under another
#    hostname (a UTS namespace) and the same identity, `buck2 resume --import --require` and a
#    build. The targets that run must be exactly those whose inputs the range c0..c1 changes,
#    plus the local action a portable resume dirties, and each output must equal a cold build's
#    of c1 at the same path. The same bundle under another identity, under another system (a
#    mount namespace whose /run/current-system/sw is another path: a toolchain change) and for
#    another binary is refused by `resume --import` in under a second, having read only the
#    MANIFEST, with no daemon started, even from a bundle padded to gigabytes; and a daemon given
#    the image without the import's check (unpacked by hand) refuses it with the same reason.
# 8. Crash points, each followed by a resume and a build whose outputs must equal the cold
#    build's: kill -9 of the daemon at several points of a portable suspend (its CAS upload
#    included); kill -9 of the client while it writes the bundle; kill -9 of the client while
#    it unpacks one; kill -9 of the daemon while it restores; a full disk under the suspend, the
#    export and the import (a tmpfs, so sudo).
# 9. Concurrency: a command that starts during a suspend; a build that starts during a resume;
#    a second `resume --import` during a resume.
#
# The RE buckconfig holds [buck2_re_client] (read only from config files) and is copied into
# each checkout's untracked .buckconfig.d/. Every command runs under `timeout`, so a stuck daemon
# shows as rc=124. STOP_AFTER=t7 ends the run after test 7. Writes <workdir>/results/summary.tsv (one `ok` or `FAIL` line per check) and a
# log per step, and exits non-zero if any check failed.
set -uo pipefail
B=$1; W=$2; RE_CONFIG=$3; WATCHER=${4:-fs_hash_crawler}
RECORDS="python3 -I $(dirname "$(readlink -f "$0")")/bundle-records.py"
RE_PROPERTIES=${RE_PROPERTIES:-'{"OSFamily": "Linux", "Arch": "amd64", "Pool": "usw2", "container-image": "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"}'}
T=${TIMEOUT:-300}
R=$W/results; P=$W/repo; SEED=$W/seed; BUNDLE=$W/image.bundle
if mountpoint -q "$P/buck-out" 2>/dev/null; then sudo umount -l "$P/buck-out"; fi
if mountpoint -q "$W/full" 2>/dev/null; then sudo umount -l "$W/full"; fi
rm -rf "$W"; mkdir -p "$R" "$SEED"
DAEMON_DIR=$HOME/.buck/buckd$P
fail=0
G=(git -c user.name=e2e -c user.email=e2e@example.com -c commit.gpgsign=false)

# ---- the fixture -------------------------------------------------------------------------------
# r0..r19: a remote chain, r_i reads src/i.txt and r_{i-1}. x0..x4: remote, independent, each
# reads src/x_i.txt. globbed: remote, reads glob/*.txt. sha: local_only, writes $GITHUB_SHA, as a
# nix realisation reads its machine. after: remote, reads sha and r19. A salt keeps the actions out
# of earlier runs' action cache, so main executes them.
SALT=$(date +%s%N)
cd "$SEED" || exit 1
mkdir -p platforms toolchains src glob
cat > .buckconfig <<CFG
[cells]
root = .
prelude = prelude
toolchains = toolchains
none = none
[cell_aliases]
config = prelude
ovr_config = prelude
fbcode = none
fbsource = none
fbcode_macros = none
buck = none
[external_cells]
prelude = bundled
[parser]
target_platform_detector_spec = target:root//...->prelude//platforms:default
[build]
execution_platforms = root//platforms:hybrid
[buck2]
file_watcher = $WATCHER
materializations = deferred
sqlite_materializer_state = true
[buck2_hydration]
enable_paging = true
suspend = true
host_identity = e2e-runner-image-1
CFG
cat > platforms/defs.bzl <<BZL
def _impl(ctx):
    p = ExecutionPlatformInfo(label = ctx.label.raw_target(), configuration = ConfigurationInfo(constraints = {}, values = {}),
        executor_config = CommandExecutorConfig(local_enabled = True, remote_enabled = True, remote_cache_enabled = True,
            allow_cache_uploads = False, use_limited_hybrid = True, remote_execution_properties = $RE_PROPERTIES,
            remote_execution_use_case = "buck2-default", remote_output_paths = "output_paths"))
    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [p])]
hybrid_platform = rule(impl = _impl, attrs = {})
BZL
printf 'load(":defs.bzl", "hybrid_platform")\nhybrid_platform(name = "hybrid")\n' > platforms/BUCK
printf 'load("@prelude//toolchains:demo.bzl", "system_demo_toolchains")\nsystem_demo_toolchains()\n' > toolchains/BUCK
cat > local.bzl <<'BZL'
def _impl(ctx):
    out = ctx.actions.declare_output(ctx.label.name + ".txt")
    ctx.actions.run(cmd_args("sh", "-c", 'echo "$GITHUB_SHA" > "$1"', "--", out.as_output()), category = "local_sha", local_only = True)
    return [DefaultInfo(default_output = out)]
local_sha = rule(impl = _impl, attrs = {})
BZL
printf 'load(":local.bzl", "local_sha")\n' > BUCK
for i in $(seq 0 19); do
  echo "src $i" > src/$i.txt
  if [ $i -eq 0 ]; then dep=""; else dep=", \":r$((i-1))\""; fi
  printf 'genrule(name = "r%d", srcs = ["src/%d.txt"%s], out = "r%d.txt", cmd = "cat $SRCS > $OUT; echo r%d %s >> $OUT")\n' \
    $i $i "$dep" $i $i "$SALT" >> BUCK
done
for i in $(seq 0 4); do
  echo "x $i" > src/x_$i.txt
  printf 'genrule(name = "x%d", srcs = ["src/x_%d.txt"], out = "x%d.txt", cmd = "cat $SRCS > $OUT; echo x%d %s >> $OUT")\n' \
    $i $i $i $i "$SALT" >> BUCK
done
echo "glob a" > glob/a.txt
printf 'genrule(name = "globbed", srcs = glob(["glob/*.txt"]), out = "globbed.txt", cmd = "cat $SRCS > $OUT; echo %s >> $OUT")\n' "$SALT" >> BUCK
printf 'local_sha(name = "sha")\n' >> BUCK
printf 'genrule(name = "after", srcs = [":sha", ":r19"], out = "after.txt", cmd = "cat $SRCS > $OUT")\n' >> BUCK
printf 'buck-out/\n.buckconfig.d/\n' > .gitignore
"${G[@]}" init -q -b main && "${G[@]}" add -A && "${G[@]}" commit -q -m c0
c0=$(git rev-parse HEAD)
# c0..c1: an edit of src/12.txt, an edit of src/5.txt reverted in a later commit (nothing to
# rebuild), and a file added under glob/.
echo "edited in c1" >> src/12.txt; "${G[@]}" commit -q -am "c1a: edit 12"
echo "edited, then reverted" >> src/5.txt; "${G[@]}" commit -q -am "c1b: edit 5"
"${G[@]}" revert --no-edit HEAD > /dev/null
echo "glob b" > glob/b.txt; "${G[@]}" add -A; "${G[@]}" commit -q -m "c1c: add glob/b.txt"
c1=$(git rev-parse HEAD)
ALL="$(for i in $(seq 0 19); do printf 'r%d ' $i; done)$(for i in $(seq 0 4); do printf 'x%d ' $i; done)globbed sha after"
EXPECTED="$(for i in $(seq 12 19); do printf 'r%d ' $i; done)globbed sha after"
TARGETS=$(for t in $ALL; do printf '//:%s ' $t; done)

# ---- helpers -----------------------------------------------------------------------------------
step() { # label, cmd...: run under the timeout, log, and record rc, wall time and commands
  local l=$1; shift; local t0=$(date +%s.%N)
  timeout "$T" "$@" > "$R/$l.log" 2>&1; local rc=$?
  local t1=$(date +%s.%N)
  local cmds=$(grep -o 'Commands: [0-9]*[^)]*)' "$R/$l.log" | tail -1)
  printf '%s\trc=%s\twall=%.3fs\t%s\n' "$l" $rc $(echo "$t1 $t0" | awk '{print $1-$2}') "${cmds:-}" | tee -a "$R/summary.tsv"
  return $rc
}
check() { # label, condition...
  if "${@:2}"; then echo "ok	$1" | tee -a "$R/summary.tsv"; else echo "FAIL	$1" | tee -a "$R/summary.tsv"; fail=1; fi
}
note() { echo "$*" | tee -a "$R/summary.tsv"; }
ran_targets() { # step label: the targets whose actions that step's build ran (any executor)
  local id=$(grep -o 'Build ID: [0-9a-f-]*' "$R/$1.log" | tail -1 | cut -d' ' -f3)
  "$B" log what-ran "$P"/buck-out/v2/log/*"_${id}_events.pb.zst" > "$R/$1.what-ran" 2>&1
  awk -F'\t' '{ split($2, a, " "); if (a[1] ~ /^root\/\/:/) print substr(a[1], 8) }' "$R/$1.what-ran" | sort -u | tr '\n' ' '
}
sorted() { echo "$@" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' '; }
outputs() { # step label, dir: copy each target's output from that step's --show-full-output
  mkdir -p "$2"
  for t in $ALL; do
    local path=$(grep -E "^root//:$t " "$R/$1.log" | awk '{print $2}')
    if [ -n "$path" ] && [ -f "$path" ]; then cp "$path" "$2/$t"; fi
  done
}
same_outputs() { # dir: every target's output equals the cold build's
  local t; for t in $ALL; do cmp -s "$1/$t" "$W/out-cold/$t" || { echo "differs: $t" >> "$R/differs.txt"; return 1; }; done
}
build_and_compare() { # label: build every target, then compare each output with the cold build's
  step "$1" "$B" build $TARGETS --show-full-output
  local rc=$?
  outputs "$1" "$W/out-$1"
  check "$1: builds (rc=$rc)" test $rc -eq 0
  check "$1: every output equals the cold build's" same_outputs "$W/out-$1"
}
no_stuck_daemon() { # label: the daemon answers, or there is none
  timeout 60 "$B" status > "$R/$1-status.log" 2>&1; local rc=$?
  check "$1: no stuck daemon (status rc=$rc)" test $rc -ne 124
}
kill_daemon() { timeout 60 "$B" kill > /dev/null 2>&1; }
# other_host cmd...: run under hostname e2e-other-host, same uid and gid (see e2e.sh). A script,
# so that `timeout` can run it.
other_host=$W/other-host
cat > "$other_host" <<'SH'
#!/usr/bin/env bash
exec unshare --user --map-root-user --uts sh -c \
  'uid=$0; gid=$1; shift; hostname e2e-other-host &&
     exec unshare --user --map-user="$uid" --map-group="$gid" "$@"' \
  "$(id -u)" "$(id -g)" "$@"
SH
chmod +x "$other_host"
find_daemon() { # the pid of this checkout's daemon, found by its working directory (buck-out/v2)
  local p d
  for p in $(pgrep -u "$(id -u)" -f 'buck2d\[repo\]'); do
    d=$(readlink "/proc/$p/cwd" 2>/dev/null)
    if [ "$d" = "$P/buck-out/v2" ] || [ "$d" = "$P" ]; then echo "$p"; return; fi
  done
}
resume_line() { grep -o '^\(resumed\|cold\)[^;]*' "$R/$1.log" | head -1; }
fresh_checkout() { # commit: delete the checkout, its buck-out and its daemon dir; clone at the path
  kill_daemon
  if mountpoint -q "$P/buck-out" 2>/dev/null; then sudo umount -l "$P/buck-out"; fi
  rm -rf "$P" "$DAEMON_DIR"
  git clone -q "$SEED" "$P" && git -C "$P" checkout -q "$1"
  mkdir -p "$P/.buckconfig.d" && cp "$RE_CONFIG" "$P/.buckconfig.d/re.buckconfig"
}

note "buck2 $("$B" --version 2>&1 | head -1) watcher=$WATCHER salt=$SALT c0=$c0 c1=$c1 host=$(hostname) $(date -u +%FT%TZ)"
DIFF=$(git -C "$SEED" diff --name-only --no-renames "$c0" "$c1" | tr '\n' ' ')
note "git diff --name-only c0 c1: $DIFF"
check "the fixture's range changes src/12.txt and glob/b.txt only" test "$DIFF" = "glob/b.txt src/12.txt "
"$other_host" true 2> "$R/unshare-probe.log" || { note "FAIL	unshare cannot make another hostname"; exit 1; }

# ---- 7. main at c0, suspend --export; fresh clone at c1, import, resume, build ------------------
fresh_checkout "$c0"; cd "$P" || exit 1
sleep 3  # sources older than buck2's racy window, as a checkout usually is
export GITHUB_SHA=sha-main
step t7-main-build "$B" build $TARGETS
check "t7: main ran every target" test "$(sorted $(ran_targets t7-main-build))" = "$(sorted $ALL)"
step t7-suspend-export "$B" suspend --export "$BUNDLE"
check "t7: the exported image is portable" grep -q 'suspended (portable)' "$R/t7-suspend-export.log"
check "t7: the bundle was written" test -s "$BUNDLE"
note "t7: $(grep -o 'suspended[^,]*, [^,]*, [^,]*, [^,]*' "$R/t7-suspend-export.log" | head -1); $(grep -o 'exported .*' "$R/t7-suspend-export.log" | sed "s#$W/##")"

cd "$W"; fresh_checkout "$c1"; cd "$P" || exit 1
check "t7: the fresh clone has no buck-out" test ! -e "$P/buck-out"
sleep 3
export GITHUB_SHA=sha-pr
step t7-import-resume "$other_host" "$B" resume --import "$BUNDLE" --require
note "t7: $(grep -o '^resumed:.*\|^cold:.*' "$R/t7-import-resume.log" | head -1)"
check "t7: resumed under another hostname with the same identity" grep -q '^resumed:' "$R/t7-import-resume.log"
check "t7: outputs are declared again from the CAS, none lost" grep -qE 'artifacts: [0-9]+ reactivated, [1-9][0-9]* declared again, [0-9]+ to fetch from the CAS' "$R/t7-import-resume.log"
check "t7: the local action is dirtied" grep -q '; 1 locally executed actions to run again' "$R/t7-import-resume.log"
step t7-resumed-build "$B" build $TARGETS --show-full-output
RESUMED=$(sorted $(ran_targets t7-resumed-build))
note "t7: resumed build ran: $RESUMED"
note "t7: expected:          $(sorted $EXPECTED)"
check "t7: the resumed build ran exactly the range's targets and the local action" test "$RESUMED" = "$(sorted $EXPECTED)"
outputs t7-resumed-build "$W/out-t7-resumed-build"
note "t7: watcher: $(grep -o 'Resumed file watcher:.*' "$DAEMON_DIR/v2/buckd.stderr" 2>/dev/null | tail -1)"
check "t7: the local output holds the PR's environment" grep -qx sha-pr "$W/out-t7-resumed-build/sha"
check "t7: r12 holds c1's edit" grep -q 'edited in c1' "$W/out-t7-resumed-build/r12"
check "t7: globbed holds the added file" grep -q 'glob b' "$W/out-t7-resumed-build/globbed"

# The comparison: c1 cold at the same path, no image, no buck-out, no daemon dir.
kill_daemon; rm -rf "$P/buck-out" "$DAEMON_DIR"
step t7-cold-build "$B" build $TARGETS --show-full-output
COLD=$(sorted $(ran_targets t7-cold-build))
note "t7: cold build ran: $COLD"
check "t7: the cold build ran every target" test "$COLD" = "$(sorted $ALL)"
outputs t7-cold-build "$W/out-cold"
check "t7: the cold build wrote every output" test "$(ls "$W/out-cold" | wc -l)" -eq "$(echo $ALL | wc -w)"
check "t7: every resumed output equals the cold build's" same_outputs "$W/out-t7-resumed-build"

# The same bundle on a machine it is not for: refused at import, from the MANIFEST alone.
# other_system cmd...: run with /run/current-system/sw resolving to another path, as after a
# system switch, under hostname e2e-other-host.
other_system=$W/other-system
# A /run of its own, holding only current-system: its sw is another path, which keeps the real
# system-path's bin so that PATH still finds every tool. Bind-mounted, so nothing need run between
# the mount and the command, and nothing is added to the environment the daemon checks.
rm -rf "$W/fake-run" "$W/fake-system" "$W/fake-system-path"
mkdir -p "$W/fake-run" "$W/fake-system" "$W/fake-system-path"
ln -s "$(readlink -f /run/current-system/sw)/bin" "$W/fake-system-path/bin"
ln -s "$W/fake-system-path" "$W/fake-system/sw"
ln -s "$W/fake-system" "$W/fake-run/current-system"
cat > "$other_system" <<SH
#!/usr/bin/env bash
exec unshare --user --map-root-user --uts --mount sh -c \
  'uid=\$0; gid=\$1; shift; hostname e2e-other-host && mount --bind "$W/fake-run" /run &&
     exec unshare --user --map-user="\$uid" --map-group="\$gid" "\$@"' \
  "\$(id -u)" "\$(id -g)" "\$@"
SH
chmod +x "$other_system"
note "t7i: system here: $(readlink -f /run/current-system/sw 2>/dev/null || echo none); under other_system: $("$other_system" readlink -f /run/current-system/sw)"
wall() { awk -F'\t' -v l="$1" '$1 == l { sub("wall=", "", $3); sub("s$", "", $3); print $3 }' "$R/summary.tsv" | tail -1; }
under_a_second() { awk -v w="$(wall "$1")" 'BEGIN { exit !(w != "" && w < 1.0) }'; }
no_daemon() { test -z "$(find_daemon)" && test ! -e "$DAEMON_DIR/buckd.info"; }
refused_at_import() { # label, reason: the import refused for that reason, fast, leaving nothing
  check "$1: refused at import: $2" grep -qF "Not importing the suspend bundle: $2" "$R/$1.log"
  check "$1: in under a second ($(wall "$1")s)" under_a_second "$1"
  check "$1: no daemon started" no_daemon
  check "$1: no image installed" test ! -e "$P/buck-out/v2/cache/suspend/MANIFEST"
}
IDENTITY_REASON="the host identity differs from the suspended daemon's"
SYSTEM_REASON="the system (architecture, OS, /run/current-system/sw) differs from the suspended daemon's"
kill_daemon; rm -rf "$P/buck-out" "$DAEMON_DIR"
sed -i 's/^host_identity = e2e-runner-image-1$/host_identity = e2e-runner-image-2/' .buckconfig
step t7i-other-identity "$other_host" "$B" resume --import "$BUNDLE"
refused_at_import t7i-other-identity "$IDENTITY_REASON"
git checkout -q .buckconfig
rm -rf "$P/buck-out" "$DAEMON_DIR"
step t7i-other-system "$other_system" "$B" resume --import "$BUNDLE"
refused_at_import t7i-other-system "$SYSTEM_REASON"
rm -rf "$P/buck-out" "$DAEMON_DIR"
# The first command of a freshly written binary takes 1.7 to 3.5 s whatever it does (`--version`
# included, 2026-10-18 on the workbox), so run one first: the step then times the refusal.
cat "$B" > "$W/buck2-other"; printf 'x' >> "$W/buck2-other"; chmod +x "$W/buck2-other"
t0=$(date +%s.%N); "$W/buck2-other" --version > /dev/null 2>&1
note "t7i: the other binary's first command (--version) took $(echo "$(date +%s.%N) $t0" | awk '{printf "%.3f", $1-$2}')s"
step t7i-other-binary "$other_host" "$W/buck2-other" resume --import "$BUNDLE"
refused_at_import t7i-other-binary "the buck2 binary differs from the suspended daemon's"
rm -f "$W/buck2-other"

# The same refusal from a bundle of gigabytes: PAD_GB of random bytes right after the MANIFEST's
# records, which a matching import reads past and refuses as damage.
PAD=$W/padded.bundle
if [ "${PAD_GB:-2}" -gt 0 ]; then
  $RECORDS pad "$BUNDLE" "$PAD" "$(( ${PAD_GB:-2} << 30 ))"
  note "t7i: padded bundle $(stat -c %s "$PAD") bytes"
  kill_daemon; rm -rf "$P/buck-out" "$DAEMON_DIR"
  sed -i 's/^host_identity = e2e-runner-image-1$/host_identity = e2e-runner-image-2/' .buckconfig
  step t7i-padded-other-identity "$other_host" "$B" resume --import "$PAD"
  refused_at_import t7i-padded-other-identity "$IDENTITY_REASON"
  git checkout -q .buckconfig
  rm -rf "$P/buck-out" "$DAEMON_DIR"
  step t7i-padded-same-identity "$other_host" "$B" resume --import "$PAD"
  note "t7i: the same padded bundle under the matching identity: rc=$? in $(wall t7i-padded-same-identity)s: $(grep -o 'Not importing[^"]*' "$R/t7i-padded-same-identity.log" | head -1)"
  check "t7i: the matching import read past the MANIFEST and refused the bytes after it" grep -q 'Not importing the suspend bundle: the bundle is damaged' "$R/t7i-padded-same-identity.log"
  kill_daemon; rm -f "$PAD"
fi

# The daemon's own check, for an image no import checked: unpacked by hand into the cache dir.
unpack_by_hand() { rm -rf "$P/buck-out" "$DAEMON_DIR"; mkdir -p "$P/buck-out/v2/cache" && $RECORDS unpack "$BUNDLE" "$P/buck-out/v2/cache"; }
unpack_by_hand
sed -i 's/^host_identity = e2e-runner-image-1$/host_identity = e2e-runner-image-2/' .buckconfig
step t7i-daemon-other-identity "$other_host" "$B" resume
note "t7i: daemon, other identity: $(resume_line t7i-daemon-other-identity)"
check "t7i: the daemon refuses another identity with the same reason" grep -q "^cold: $IDENTITY_REASON" "$R/t7i-daemon-other-identity.log"
kill_daemon; git checkout -q .buckconfig
unpack_by_hand
step t7i-daemon-other-system "$other_system" "$B" resume
note "t7i: daemon, other system: $(resume_line t7i-daemon-other-system)"
check "t7i: the daemon refuses another system with the same reason" grep -qF "cold: $SYSTEM_REASON" "$R/t7i-daemon-other-system.log"
kill_daemon
unpack_by_hand
step t7i-daemon-same "$other_host" "$B" resume --require
note "t7i: daemon, same identity and system, another hostname: $(resume_line t7i-daemon-same)"
check "t7i: the hand-unpacked image resumes under another hostname with the same identity" grep -q '^resumed:' "$R/t7i-daemon-same.log"
kill_daemon
if [ "${STOP_AFTER:-}" = t7 ]; then note "result fail=$fail $(date -u +%FT%TZ) (STOP_AFTER=t7)"; exit $fail; fi

# ---- 8. crash points ---------------------------------------------------------------------------
# Each starts from a warm daemon of c1 with GITHUB_SHA=sha-pr, so its outputs must equal the cold
# build's.
warm() { # label
  kill_daemon; rm -rf "$P/buck-out/v2/cache/suspend"
  step "$1-warm" "$B" build $TARGETS > /dev/null
}
# 8a. kill -9 of the daemon during `suspend --portable` (the CAS upload runs inside it), at
#     several delays; what the image directory held afterwards says which phase was hit.
for delay in 0.05 0.12 0.16 0.2 0.5 1.0 1.4 1.55 1.7 2.0; do
  l=t8a-$delay
  warm "$l"
  pid=$(find_daemon)
  timeout "$T" "$B" suspend --portable > "$R/$l-suspend.log" 2>&1 &
  cpid=$!
  sleep "$delay"; kill -9 "$pid" 2>/dev/null; wait $cpid; src=$?
  note "$l: suspend rc=$src, image dir: $(ls "$P/buck-out/v2/cache/suspend" 2>/dev/null | tr '\n' ' ')"
  step "$l-resume" "$B" resume
  note "$l: $(resume_line "$l-resume")"
  check "$l: the start after the kill resumed a complete image or started cold" grep -q '^\(resumed\|cold\)' "$R/$l-resume.log"
  build_and_compare "$l-build"
  no_stuck_daemon "$l"
done

# 8b. kill -9 of the client while it writes the bundle (after the daemon has gone).
hit=0
for try in 1 2 3 4 5; do
  l=t8b-$try
  warm "$l"
  out=$W/bundle-8b.tar.zst; rm -f "$out" "$out".tmp.*
  timeout "$T" "$B" suspend --export "$out" > "$R/$l-suspend.log" 2>&1 &
  cpid=$!
  for _ in $(seq 1 4000); do ls "$out".tmp.* > /dev/null 2>&1 && break; kill -0 $cpid 2>/dev/null || break; sleep 0.002; done
  if ls "$out".tmp.* > /dev/null 2>&1; then kill -9 $cpid; hit=1; fi
  wait $cpid
  [ $hit = 1 ] && break
  note "$l: the export finished before the kill"
done
if [ $hit = 1 ]; then
  check "t8b: no bundle after a kill during the export" test ! -e "$out"
  note "t8b: left behind: $(ls "$W" | grep 'bundle-8b' | tr '\n' ' ')"
  step t8b-resume "$B" resume
  note "t8b: $(resume_line t8b-resume)"
  check "t8b: the local image is untouched and resumes" grep -q '^resumed:' "$R/t8b-resume.log"
  build_and_compare t8b-build
  no_stuck_daemon t8b
else
  note "NOTRUN	t8b: the export always finished before the kill"
fi

# 8c. kill -9 of the client while it unpacks a bundle: no MANIFEST, so the next start is cold.
hit=0
for try in 1 2 3 4 5; do
  l=t8c-$try
  kill_daemon; rm -rf "$P/buck-out/v2/cache/suspend" "$P/buck-out/v2/cache/dice_state"
  timeout "$T" "$B" resume --import "$BUNDLE" > "$R/$l-import.log" 2>&1 &
  cpid=$!
  for _ in $(seq 1 4000); do ls -d "$P"/buck-out/v2/cache/suspend.import.* > /dev/null 2>&1 && break; kill -0 $cpid 2>/dev/null || break; sleep 0.002; done
  if ls -d "$P"/buck-out/v2/cache/suspend.import.* > /dev/null 2>&1; then kill -9 $cpid; hit=1; fi
  wait $cpid
  [ $hit = 1 ] && break
  note "$l: the import finished before the kill"
done
if [ $hit = 1 ]; then
  # Killed before the MANIFEST went in, the next start must not resume; killed after it (while
  # the empty import directory was being removed), the image is complete and may.
  if [ -e "$P/buck-out/v2/cache/suspend/MANIFEST" ]; then manifest=1; else manifest=0; fi
  note "t8c: MANIFEST present after the kill: $manifest; left: $(ls "$P/buck-out/v2/cache" | tr '\n' ' ')"
  step t8c-resume "$B" resume
  note "t8c: $(resume_line t8c-resume)"
  check "t8c: the next start resumes only a complete import" sh -c "test $manifest = 1 || ! grep -q '^resumed:' '$R/t8c-resume.log'"
  build_and_compare t8c-build
  no_stuck_daemon t8c
else
  note "NOTRUN	t8c: the import always finished before the kill"
fi

# 8d. kill -9 of the daemon while it restores an imported image: after it has claimed the
#     MANIFEST (renamed it MANIFEST.consumed) and before it reports. A daemon killed before the
#     claim leaves an unused image, which the next start may resume; one killed after must not
#     leave an image anything resumes.
claimed_kill=0
for try in 1 2 3 4 5; do
  l=t8d-$try
  kill_daemon; rm -rf "$P/buck-out" "$DAEMON_DIR"
  timeout "$T" "$other_host" "$B" resume --import "$BUNDLE" > "$R/$l-import.log" 2>&1 &
  cpid=$!
  pid=
  for _ in $(seq 1 20000); do
    if [ -e "$P/buck-out/v2/cache/suspend/MANIFEST.consumed" ]; then pid=$(find_daemon); [ -n "$pid" ] && break; fi
    kill -0 $cpid 2>/dev/null || break
    sleep 0.001
  done
  reported=$(grep -o '^\(resumed\|cold\)[^;]*' "$R/$l-import.log" | head -1)
  [ -n "$pid" ] && kill -9 "$pid"
  wait $cpid; irc=$?
  state=$(ls "$P/buck-out/v2/cache/suspend" 2>/dev/null | tr '\n' ' ')
  note "$l: resume --import rc=$irc, killed pid ${pid:-none}, reported before the kill: ${reported:-nothing}, image dir after: ${state:-gone}"
  if [ -n "$pid" ] && [ -z "$reported" ]; then claimed_kill=1; break; fi
done
check "t8d: a daemon was killed after its claim and before it reported" test $claimed_kill = 1
step t8d-resume "$B" resume
note "t8d: $(resume_line t8d-resume)"
check "t8d: the claimed image is not used twice" sh -c "! grep -q '^resumed:' '$R/t8d-resume.log'"
build_and_compare t8d-build
no_stuck_daemon t8d

# 8e. a full disk: buck-out on a tmpfs.
kill_daemon; rm -rf "$P/buck-out" "$DAEMON_DIR"; mkdir -p "$P/buck-out"
sudo mount -t tmpfs -o "size=2g,uid=$(id -u),gid=$(id -g)" tmpfs "$P/buck-out"
fill() { # leave about 64 KiB free on buck-out
  local free=$(df --output=avail -B1 "$P/buck-out" | tail -1)
  fallocate -l $((free - 65536)) "$P/buck-out/filler" 2>/dev/null || dd if=/dev/zero of="$P/buck-out/filler" bs=1M count=$((free / 1048576 - 1)) 2>/dev/null
}
step t8e-warm "$B" build $TARGETS > /dev/null
fill
step t8e-suspend-full "$B" suspend --portable
src=$?
note "t8e: suspend on a full disk rc=$src: $(grep -io 'no space[^"]*\|error[^"]*' "$R/t8e-suspend-full.log" | head -1)"
check "t8e: the suspend fails on a full disk" test $src -ne 0
check "t8e: no MANIFEST after the failed suspend" test ! -e "$P/buck-out/v2/cache/suspend/MANIFEST"
no_stuck_daemon t8e-after-suspend
kill_daemon; rm -f "$P/buck-out/filler"
step t8e-resume "$B" resume
note "t8e: $(resume_line t8e-resume)"
check "t8e: the start after the failed suspend does not resume" sh -c "! grep -q '^resumed:' '$R/t8e-resume.log'"
build_and_compare t8e-build
# The export onto a full disk.
mkdir -p "$W/full"; sudo mount -t tmpfs -o "size=64k,uid=$(id -u),gid=$(id -g)" tmpfs "$W/full"
step t8e-export-full "$B" suspend --export "$W/full/b.tar.zst"
src=$?
note "t8e: export onto a full disk rc=$src; left in it: $(ls -A "$W/full" | tr '\n' ' ')"
check "t8e: the export onto a full disk fails" test $src -ne 0
check "t8e: the failed export leaves nothing" test -z "$(ls -A "$W/full")"
step t8e-resume-after-export "$B" resume
check "t8e: the local image survives a failed export" grep -q '^resumed:' "$R/t8e-resume-after-export.log"
build_and_compare t8e-build-after-export
sudo umount -l "$W/full"
# The import onto a full disk.
kill_daemon; rm -rf "$P/buck-out/v2/cache/suspend" "$P/buck-out/v2/cache/dice_state"
fill
step t8e-import-full "$B" resume --import "$BUNDLE"
src=$?
note "t8e: import onto a full disk rc=$src: $(grep -io 'no space[^"]*\|error[^"]*' "$R/t8e-import-full.log" | head -1)"
check "t8e: the import onto a full disk fails" test $src -ne 0
check "t8e: no MANIFEST after the failed import" test ! -e "$P/buck-out/v2/cache/suspend/MANIFEST"
kill_daemon; rm -f "$P/buck-out/filler"
step t8e-resume-after-import "$B" resume
check "t8e: the start after the failed import does not resume" sh -c "! grep -q '^resumed:' '$R/t8e-resume-after-import.log'"
build_and_compare t8e-build-after-import
no_stuck_daemon t8e
kill_daemon; sudo umount -l "$P/buck-out"; rm -rf "$DAEMON_DIR"

# ---- 9. concurrency ----------------------------------------------------------------------------
# 9a. a command that starts while a suspend runs (the review's race fix): either the suspend
#     fails and the daemon keeps running, or the command starts a daemon after it; either way
#     nothing resumes a stale image, and the build is right.
for delay in 0.1 0.5 1.0; do
  l=t9a-$delay
  warm "$l"
  timeout "$T" "$B" suspend --portable > "$R/$l-suspend.log" 2>&1 &
  cpid=$!
  sleep "$delay"
  step "$l-uquery" "$B" uquery //:r0
  wait $cpid; src=$?
  note "$l: suspend rc=$src: $(grep -o 'cannot run beside[^;]*\|a command started while[^;]*\|suspended[^:]*: image' "$R/$l-suspend.log" | head -1); uquery: $(grep -o 'resumed[^;]*\|cold[^;]*' "$R/$l-uquery.log" | head -1)"
  check "$l: the command that raced the suspend succeeded" grep -q 'root//:r0' "$R/$l-uquery.log"
  build_and_compare "$l-build"
  no_stuck_daemon "$l"
done
# 9b. a build that starts while `resume --import` restores, and 9c. a second `resume --import`
#     at the same time.
for delay in 0.05 0.3; do
  l=t9b-$delay
  kill_daemon; rm -rf "$P/buck-out" "$DAEMON_DIR"
  timeout "$T" "$other_host" "$B" resume --import "$BUNDLE" --require > "$R/$l-import.log" 2>&1 &
  cpid=$!
  sleep "$delay"
  timeout "$T" "$B" resume --import "$BUNDLE" > "$R/$l-second-import.log" 2>&1 &
  c2=$!
  step "$l-build" "$B" build $TARGETS --show-full-output
  brc=$?
  wait $cpid; irc=$?; wait $c2; i2rc=$?
  note "$l: import rc=$irc ($(resume_line "$l-import")); second import rc=$i2rc: $(grep -v '^\s*$' "$R/$l-second-import.log" | tail -1 | cut -c1-160); build rc=$brc"
  check "$l: the import and the build both finished (no timeout)" test $irc -ne 124 -a $brc -ne 124 -a $i2rc -ne 124
  check "$l: the second import did not replace the image under a running daemon" sh -c "test $i2rc -ne 0 || grep -q '^resumed:' '$R/$l-second-import.log'"
  outputs "$l-build" "$W/out-$l-build"
  check "$l: the build during the resume is right" same_outputs "$W/out-$l-build"
  no_stuck_daemon "$l"
done

kill_daemon
note "result fail=$fail $(date -u +%FT%TZ)"
exit $fail
