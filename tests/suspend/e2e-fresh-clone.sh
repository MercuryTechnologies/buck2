#!/usr/bin/env bash
# Option B's resume on a fresh clone, all local (no RE, no remote cache): build at commit c0 and
# suspend; move the checkout aside and clone it into the same path, as a fresh runner would;
# commit c1 there (one source edited, one file added); put buck-out back, which piece 3 is to
# replace with re-declaring outputs from the CAS; resume and build. Then a cold build of c1 for
# comparison. Every file of the clone has a new inode, so only the content comparison and git
# can tell the resumed watcher what changed.
#   e2e-fresh-clone.sh <buck2 binary> <workdir> [watcher]
# NOGIT=1 moves the clone's .git away before the resume, so the watcher compares contents only.
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
printf 'buck-out/\n' > "$F/.gitignore"
G=(git -c user.name=e2e -c user.email=e2e@example.com -c commit.gpgsign=false)
cd "$F" && "${G[@]}" init -q -b main && "${G[@]}" add -A && "${G[@]}" commit -q -m c0
c0=$(git rev-parse HEAD)
# Sources older than buck2's racy window (2 s), as a checkout usually is.
sleep 3
step() { # label, cmd...
  local l=$1; shift; local t0=$(date +%s.%N); "$@" > "$R/$l.log" 2>&1; local rc=$?; local t1=$(date +%s.%N)
  local cmds=$(grep -o 'Commands: [0-9]*[^)]*)' "$R/$l.log" | tail -1)
  printf '%s\trc=%s\twall=%.3fs\t%s\n' "$l" $rc $(echo "$t1 $t0" | awk '{print $1-$2}') "${cmds:-}" | tee -a $R/summary.tsv
}
ran() { # label: what that step's command ran, from its own event log, saved beside its log
  local id=$(grep -o 'Build ID: [0-9a-f-]*' "$R/$1.log" | head -1 | cut -d' ' -f3)
  "$B" log what-ran "$(ls buck-out/v2/log/*"$id"*events* | head -1)" > "$R/$1.what-ran" 2>&1
  grep -o 'root//:g[0-9]*' "$R/$1.what-ran" | sort -u -t g -k2 -n | tr '\n' ' '; }
daemon_log() { echo "$HOME/.buck/buckd${F}/v2/buckd.stderr"; }
: > $R/summary.tsv
echo "buck2 $("$B" --version 2>&1 | head -1) watcher=$WATCHER c0=$c0 $(date -u +%FT%TZ)" | tee -a $R/summary.tsv

step cold-build-c0 "$B" build //:g39
step suspend "$B" suspend
grep -o 'suspended[^"]*' "$R/suspend.log" | head -1 | tee -a $R/summary.tsv

# The fresh runner: same absolute path, new clone, the PR's commit on top.
cd "$W" && mv "$F" "$W/c0" && git clone -q "$W/c0" "$F" && cd "$F" || exit 1
echo "edited on c1" >> src/20.txt
echo "a file c1 adds" > src/added.txt
"${G[@]}" add -A && "${G[@]}" commit -q -m c1
c1=$(git rev-parse HEAD)
mv "$W/c0/buck-out" "$F/buck-out"
echo "clone at c1=$c1: git diff --name-only c0 c1: $(git diff --name-only --no-renames "$c0" "$c1" | tr '\n' ' ')" | tee -a $R/summary.tsv
if [ "${NOGIT:-0}" = 1 ]; then mv "$F/.git" "$W/c1.git"; fi
sleep 3

step resume "$B" resume --require
grep -o '^\(resumed\|cold\)[^;]*' "$R/resume.log" | head -1 | tee -a $R/summary.tsv
echo "watcher: $(grep -o 'Resumed file watcher:.*' "$(daemon_log)" | tail -1)" | tee -a $R/summary.tsv
step resumed-build-c1 "$B" build //:g39
echo "resumed-build-c1 ran: $(ran resumed-build-c1)" | tee -a $R/summary.tsv
g39=$(find buck-out/v2 -path '*__g39__*' -name g39.txt -type f | head -1)
echo "g39 holds c1's edit: $(grep -c 'edited on c1' "$g39" 2>&1)" | tee -a $R/summary.tsv

# The comparison: a cold build of c1 with no image and no buck-out.
"$B" kill > /dev/null 2>&1
rm -rf buck-out
step cold-build-c1 "$B" build //:g39
echo "cold-build-c1 ran: $(ran cold-build-c1)" | tee -a $R/summary.tsv
"$B" kill > /dev/null 2>&1
echo "done $(date -u +%FT%TZ)" | tee -a $R/summary.tsv
