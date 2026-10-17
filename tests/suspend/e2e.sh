#!/usr/bin/env bash
# End-to-end check of `buck2 suspend` / `buck2 resume` on a 40-genrule chain, all local
# (no RE, no remote cache), fs_hash_crawler or notify watcher, deferred materializer with sqlite state.
#   e2e.sh <buck2 binary> <workdir> [watcher]
# Writes <workdir>/results/summary.tsv and one log per step.
set -uo pipefail
B=$1; W=$2; WATCHER=${3:-fs_hash_crawler}
R=$W/results; F=$W/fixture
rm -rf "$W"; mkdir -p "$R" "$F/src" "$F/platforms" "$F/toolchains"
cat > "$F/.buckconfig" <<CFG
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
execution_platforms = root//platforms:local
[buck2]
file_watcher = $WATCHER
materializations = deferred
sqlite_materializer_state = true
[buck2_hydration]
enable_paging = true
suspend = true
CFG
cat > "$F/platforms/defs.bzl" <<'BZL'
def _impl(ctx):
    p = ExecutionPlatformInfo(label = ctx.label.raw_target(), configuration = ConfigurationInfo(constraints = {}, values = {}),
        executor_config = CommandExecutorConfig(local_enabled = True, remote_enabled = False, remote_cache_enabled = False,
            allow_cache_uploads = False, use_limited_hybrid = False, remote_execution_properties = {}, remote_execution_use_case = "buck2-default"))
    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [p])]
local_platform = rule(impl = _impl, attrs = {})
BZL
printf 'load(":defs.bzl", "local_platform")\nlocal_platform(name = "local")\n' > "$F/platforms/BUCK"
printf 'load("@prelude//toolchains:demo.bzl", "system_demo_toolchains")\nsystem_demo_toolchains()\n' > "$F/toolchains/BUCK"
: > "$F/BUCK"
for i in $(seq 0 39); do echo "base $i" > "$F/src/$i.txt"; if [ $i -eq 0 ]; then dep=""; else dep=", \":g$((i-1))\""; fi
  printf 'genrule(name = "g%d", srcs = ["src/%d.txt"%s], out = "g%d.txt", cmd = "sleep 0.5; cat $SRCS > $OUT")\n' $i $i "$dep" $i >> "$F/BUCK"; done
# One target that reads a buckconfig value builds pass with -c (scenario 8).
printf 'genrule(name = "cfg", out = "cfg.txt", cmd = "sleep 0.5; echo %%s > $OUT" %% read_config("suspend_e2e", "v", "unset"))\n' >> "$F/BUCK"
cd "$F"
# Files written within buck2's racy window (2 s) of a file-watcher sync are reported changed again
# at the next one; start the scenario with sources older than that, as a checkout usually is.
sleep 3
step() { # label, cmd...: the command runs under a trace id of its own, kept in $R/<label>.id
  local l=$1; shift; local id=$(cat /proc/sys/kernel/random/uuid); echo "$id" > "$R/$l.id"
  local t0=$(date +%s.%N); (export BUCK_WRAPPER_UUID=$id; "$@") > "$R/$l.log" 2>&1; local rc=$?; local t1=$(date +%s.%N)
  local cmds=$(grep -o 'Commands: [0-9]*[^)]*)' "$R/$l.log" | tail -1)
  printf '%s\trc=%s\twall=%.3fs\t%s\n' "$l" $rc $(echo "$t1 $t0" | awk '{print $1-$2}') "${cmds:-}" | tee -a $R/summary.tsv
}
daemon_pid() { cat "$HOME/.buck/buckd/$(echo "$F" | sed 's#/#_#g' | sed 's#^_##')/v2/buckd.info" 2>/dev/null | grep -o '"pid":[0-9]*' | cut -d: -f2; }
# The event log of the command a step ran, by its trace id. `buck2 log last` sorts the logs by
# name and a log can appear after its command returns, so it read the resume's log for the build
# after a resume (same second, "resume" sorts after "build"), and the previous command's log for
# a quick build. Waits up to 30 s for the log to appear.
evlog() {
  local id f i; id=$(cat "$R/$1.id" 2>/dev/null) || return
  for i in $(seq 1 60); do
    f=$(ls buck-out/v2/log/*_"$id"_events.pb.zst 2>/dev/null | head -1)
    [ -n "$f" ] && { echo "$f"; return; }
    sleep 0.5
  done
}
# A step's analyses, what-ran and what-ran count. A step whose event log is not found reads `?`,
# not 0, since a zero from no log looks like a zero from a build that ran nothing.
analyses() { local f; f=$(evlog "$1"); [ -n "$f" ] || { echo '?'; return; }; "$B" log show "$f" 2>/dev/null | grep -c '"Analysis"'; }
what_ran() { local f; f=$(evlog "$1"); [ -n "$f" ] || { echo "no event log for step $1"; return; }; "$B" log what-ran "$f" 2>&1; }
what_ran_count() { local f; f=$(evlog "$1"); [ -n "$f" ] || { echo '?'; return; }; "$B" log what-ran "$f" 2>/dev/null | grep -vc '^$'; }
: > $R/summary.tsv
echo "buck2 $("$B" --version 2>&1 | head -1) watcher=$WATCHER $(date -u +%FT%TZ)" | tee -a $R/summary.tsv

# Baselines: a cold daemon, then the same daemon warm.
step cold-build "$B" build //:g39
echo "cold-build analyses=$(analyses cold-build)" | tee -a $R/summary.tsv
step warm-build "$B" build //:g39
echo "warm-build analyses=$(analyses warm-build)" | tee -a $R/summary.tsv
# A new daemon over the same buck-out (kill, then build): what a job without suspend gets.
"$B" kill > /dev/null 2>&1
step killed-daemon-build "$B" build //:g39
echo "killed-daemon-build analyses=$(analyses killed-daemon-build)" | tee -a $R/summary.tsv
"$B" kill > /dev/null 2>&1
step daemon-start-no-image "$B" resume
step warm-build-2 "$B" build //:g39
step warm-build-3 "$B" build //:g39
step pageout-status "$B" debug hydration status

# 1. suspend, resume, build: nothing runs.
step suspend-1 "$B" suspend
sleep 1; echo "after suspend: daemon processes=$(pgrep -fc "$B daemon" || true)" | tee -a $R/summary.tsv
du -sb buck-out/v2/cache/suspend buck-out/v2/cache/dice_state 2>&1 | tee -a $R/summary.tsv
ls -la buck-out/v2/cache/suspend >> $R/summary.tsv 2>&1
step resume-1 "$B" resume --require
step resumed-build "$B" build //:g39
echo "resumed-build analyses=$(analyses resumed-build) what-ran=$(what_ran_count resumed-build)" | tee -a $R/summary.tsv
what_ran resumed-build > $R/resumed-build.what-ran

# 2. suspend, edit one source, resume through a build: only that genrule and its dependents run.
step suspend-2 "$B" suspend
echo "edited" >> src/20.txt
step resumed-edit-build "$B" build //:g39
what_ran resumed-edit-build > $R/resumed-edit-build.what-ran
echo "resumed-edit-build ran: $(grep -o 'root//:g[0-9]*' $R/resumed-edit-build.what-ran | sort -u -t g -k2 -n | tr '\n' ' ')" | tee -a $R/summary.tsv
step resume-2-status "$B" resume

# 3. edit between the last build and suspend, with no build in between: picked up after resume.
echo "edited before suspend" >> src/30.txt
step suspend-3 "$B" suspend
step resumed-after-pre-suspend-edit "$B" build //:g39
what_ran resumed-after-pre-suspend-edit > $R/resumed-after-pre-suspend-edit.what-ran
echo "resumed-after-pre-suspend-edit ran: $(grep -o 'root//:g[0-9]*' $R/resumed-after-pre-suspend-edit.what-ran | sort -u -t g -k2 -n | tr '\n' ' ')" | tee -a $R/summary.tsv
g39=$(find buck-out/v2 -path '*__g39__*' -name g39.txt -type f | head -1)
echo "g39 contains the pre-suspend edit: $(grep -c "edited before suspend" "$g39" 2>&1) ($g39)" | tee -a $R/summary.tsv

# 4. a different binary refuses the image.
step suspend-4 "$B" suspend
cp "$B" "$W/buck2-other"; printf 'x' >> "$W/buck2-other"
step other-binary-resume "$W/buck2-other" resume --require
"$W/buck2-other" kill >/dev/null 2>&1

# 5. a changed root buckconfig refuses the image.
"$B" build //:g39 > /dev/null 2>&1
step suspend-5 "$B" suspend
printf '[suspend_e2e]\nchanged = 1\n' >> .buckconfig
step changed-config-resume "$B" resume --require
step changed-config-build "$B" build //:g39

# 6. a changed environment refuses the image, naming the variable.
step suspend-6 "$B" suspend
step changed-env-resume env SUSPEND_E2E_VAR=1 "$B" resume --require

# 7. a daemon with the setting off deletes the image; turning it back on starts cold.
"$B" kill > /dev/null 2>&1
"$B" build //:g39 > /dev/null 2>&1
step suspend-7 "$B" suspend
sed -i 's/^suspend = true/suspend = false/' .buckconfig
step setting-off-build "$B" build //:g39
ls buck-out/v2/cache/ > $R/cache-after-setting-off.txt 2>&1
"$B" kill > /dev/null 2>&1
sed -i 's/^suspend = false/suspend = true/' .buckconfig
step setting-on-again-resume "$B" resume --require

# 8. builds that pass -c: a suspend and a resume given the same -c keep everything; without it,
#    the suspend's own transaction records the setting unset, and the build after the resume
#    reruns what reads it.
"$B" kill > /dev/null 2>&1
"$B" build -c suspend_e2e.v=1 //:g39 //:cfg > /dev/null 2>&1
step suspend-8-same-c "$B" suspend -c suspend_e2e.v=1
step resume-8-same-c "$B" resume --require -c suspend_e2e.v=1
step resumed-build-same-c "$B" build -c suspend_e2e.v=1 //:g39 //:cfg
echo "resumed-build-same-c analyses=$(analyses resumed-build-same-c) what-ran=$(what_ran_count resumed-build-same-c)" | tee -a $R/summary.tsv
step suspend-8-no-c "$B" suspend
step resume-8-no-c "$B" resume --require
step resumed-build-no-c "$B" build -c suspend_e2e.v=1 //:g39 //:cfg
what_ran resumed-build-no-c > $R/resumed-build-no-c.what-ran
echo "resumed-build-no-c analyses=$(analyses resumed-build-no-c) ran: $(grep -o 'root//:[a-z0-9]*' $R/resumed-build-no-c.what-ran | sort -u | tr '\n' ' ')" | tee -a $R/summary.tsv

# 9. a command that starts while a suspend runs: the suspend fails and deletes its image, or
#    the command starts after it and deletes the MANIFEST; either way nothing resumes it.
"$B" build //:g39 > /dev/null 2>&1
("$B" suspend > $R/suspend-9-race.log 2>&1; echo "suspend-9-race rc=$?" | tee -a $R/summary.tsv) &
sleep 0.1
"$B" uquery //:g0 > $R/race-uquery.log 2>&1
wait
echo "race: suspend said: $(grep -o 'cannot run beside[^;]*\|a command started while[^;]*\|suspended: image' $R/suspend-9-race.log | head -1); uquery: $(grep -o 'Connected to new buck2 daemon\|resumed[^;]*\|cold[^;]*' $R/race-uquery.log | head -1)" | tee -a $R/summary.tsv
# A failed suspend leaves the daemon running, so `resume` would report how that daemon started;
# kill it first, so the next daemon shows whether any image survived.
"$B" kill > /dev/null 2>&1
step resume-9-after-race "$B" resume
echo "after-race: $(grep -o '^\(resumed\|cold\)[^;]*' $R/resume-9-after-race.log | head -1)" | tee -a $R/summary.tsv

# 10. the host identity (`buck2_hydration.host_identity`, by default the hostname). Another
#     hostname is made with a user and UTS namespace: root in an outer one sets the hostname, and
#     an inner one maps the caller's uid and gid back, so the user check still passes.
other_host() { # cmd... run under hostname e2e-other-host, same uid and gid
  unshare --user --map-root-user --uts sh -c \
    'uid=$0; gid=$1; shift; hostname e2e-other-host &&
       exec unshare --user --map-user="$uid" --map-group="$gid" "$@"' \
    "$(id -u)" "$(id -g)" "$@"
}
if other_host true 2> "$R/unshare-probe.log"; then
  # 10a. default identity: another hostname refuses, naming the identity.
  "$B" kill > /dev/null 2>&1
  "$B" build //:g39 > /dev/null 2>&1
  step suspend-10a "$B" suspend
  step other-host-default-identity-resume other_host "$B" resume --require
  "$B" kill > /dev/null 2>&1
  # 10b. a configured identity: another hostname with the same identity resumes, and builds
  #      nothing.
  sed -i 's/^suspend = true$/suspend = true\nhost_identity = e2e-identity-a/' .buckconfig
  "$B" build //:g39 > /dev/null 2>&1
  step suspend-10b "$B" suspend
  step other-host-same-identity-resume other_host "$B" resume --require
  step other-host-same-identity-build "$B" build //:g39
  echo "other-host-same-identity-build what-ran=$(what_ran_count other-host-same-identity-build)" | tee -a $R/summary.tsv
  "$B" kill > /dev/null 2>&1
  # 10c. another identity refuses, naming it, even on the same host.
  "$B" build //:g39 > /dev/null 2>&1
  step suspend-10c "$B" suspend
  sed -i 's/^host_identity = e2e-identity-a$/host_identity = e2e-identity-b/' .buckconfig
  step other-identity-resume "$B" resume --require
  sed -i '/^host_identity = /d' .buckconfig
  for l in other-host-default-identity-resume other-host-same-identity-resume other-identity-resume; do
    echo "$l: $(grep -o '\(resumed\|cold\)[^;]*' "$R/$l.log" | head -1)" | tee -a $R/summary.tsv
  done
else
  echo "identity scenarios not run: unshare cannot make another hostname ($(head -c 200 "$R/unshare-probe.log"))" | tee -a $R/summary.tsv
fi

# 11. a chained resume keeps the local actions of the daemon before it: resume on the same host
#     (nothing dirtied), build nothing, suspend again. The second image must still list the
#     actions the first daemon ran, so that a resume elsewhere runs them again.
"$B" kill > /dev/null 2>&1
sed -i 's/^suspend = true$/suspend = true\nhost_identity = e2e-identity-a/' .buckconfig
for i in $(seq 0 39); do echo "chain $i" > "src/$i.txt"; done
touch -d '1 hour ago' src/*.txt
step chain-build "$B" build //:g39
step suspend-11a "$B" suspend
step resume-11a "$B" resume --require
step chain-resumed-build "$B" build //:g39
step suspend-11b "$B" suspend
locals() { grep -o '[0-9]* locally executed actions' "$R/$1.log" | head -1 | cut -d' ' -f1; }
echo "chain: first image lists $(locals suspend-11a) local actions, the chained one $(locals suspend-11b)" | tee -a $R/summary.tsv
if other_host true 2> /dev/null; then
  step other-host-chained-resume other_host "$B" resume --require
  step other-host-chained-build "$B" build //:g39
  echo "other-host-chained-resume: $(grep -o '\(resumed\|cold\)[^;]*\|[0-9]* locally executed actions to run again' "$R/other-host-chained-resume.log" | tr '\n' ' ')" | tee -a $R/summary.tsv
  echo "other-host-chained-build what-ran=$(what_ran_count other-host-chained-build)" | tee -a $R/summary.tsv
fi
sed -i '/^host_identity = /d' .buckconfig

"$B" kill > /dev/null 2>&1
echo "done $(date -u +%FT%TZ)" | tee -a $R/summary.tsv
