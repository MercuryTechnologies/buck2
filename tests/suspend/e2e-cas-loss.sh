#!/usr/bin/env bash
# What a daemon resumed on a fresh runner does when a blob its image declares has left the
# remote CAS. A main daemon builds remote producers, suspends --portable, and a fresh runner
# resumes it with BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE and
# BUCK2_TEST_FAIL_RE_DOWNLOAD_DIGESTS_ONCE naming every producer's output, so the first
# FindMissingBlobs reports each one missing and the first download of each fails NOT_FOUND, as
# an expired blob does. Each consumer then runs once, in its own command:
#   c_remote   buck2 build, remote genrule over p_remote
#   c_local    buck2 build, local_only action over p_local
#   p_top      buck2 build --materializations=all of a producer itself
#   p_default  buck2 build of a producer itself, under the default materialization
#   t_remote   buck2 test, remote test over p_test_remote
#   t_local    buck2 test, local test over p_test_local
#   t_build    buck2 test, remote test over a remote genrule over p_test_build
# Every consumer's output differs from its producer's, so the hooks name only the producers'.
# Pass for each: the command succeeds, its output or verdict equals a cold build's, and the
# producer ran again. A control resume without the hooks must pass every command too.
#   e2e-cas-loss.sh <buck2 binary> <workdir> <RE buckconfig>
# Writes <workdir>/results/summary.tsv, one log per step, and exits non-zero if a check fails.
set -uo pipefail
B=$1; W=$2; RE_CONFIG=$3
RE_PROPERTIES=${RE_PROPERTIES:-'{"OSFamily": "Linux", "Arch": "amd64", "Pool": "usw2", "container-image": "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"}'}
R=$W/results; F=$W/fixture; K=$W/control; C=$W/cold
rm -rf "$W"; mkdir -p "$R"
PRODUCERS="p_remote p_local p_top p_default p_test_remote p_test_local p_test_build"

make_fixture() { # dir
  local d=$1
  mkdir -p "$d/platforms" "$d/toolchains" "$d/.buckconfig.d"
  cp "$RE_CONFIG" "$d/.buckconfig.d/re.buckconfig"
  cat > "$d/.buckconfig" <<CFG
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
file_watcher = fs_hash_crawler
materializations = deferred
sqlite_materializer_state = true
[buck2_hydration]
enable_paging = true
suspend = true
suspend_env_ignore = ACTIONS_*, GIT_BASE_BRANCH, GIT_BRANCH, GIT_COMMIT, GIT_PR_NUMBER, ImageOS, ImageVersion, BUCK2_TEST_*
CFG
  cat > "$d/platforms/defs.bzl" <<BZL
def _impl(ctx):
    p = ExecutionPlatformInfo(label = ctx.label.raw_target(), configuration = ConfigurationInfo(constraints = {}, values = {}),
        executor_config = CommandExecutorConfig(local_enabled = True, remote_enabled = True, remote_cache_enabled = True,
            allow_cache_uploads = False, use_limited_hybrid = True, remote_execution_properties = $RE_PROPERTIES,
            remote_execution_use_case = "buck2-default", remote_output_paths = "output_paths"))
    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [p])]
hybrid_platform = rule(impl = _impl, attrs = {})
BZL
  printf 'load(":defs.bzl", "hybrid_platform")\nhybrid_platform(name = "hybrid")\n' > "$d/platforms/BUCK"
  printf 'load("@prelude//toolchains:demo.bzl", "system_demo_toolchains")\nsystem_demo_toolchains()\n' > "$d/toolchains/BUCK"
  cat > "$d/rules.bzl" <<'BZL'
def _local_cat(ctx):
    out = ctx.actions.declare_output(ctx.label.name + ".txt")
    ctx.actions.run(cmd_args("sh", "-c", '{ printf "local saw: "; cat "$1"; } > "$2"', "--", ctx.attrs.src, out.as_output()), category = "local_cat", local_only = True)
    return [DefaultInfo(default_output = out)]
local_cat = rule(impl = _local_cat, attrs = {"src": attrs.source()})

def _grep_test(ctx):
    kwargs = {}
    if ctx.attrs.local:
        kwargs["default_executor"] = CommandExecutorConfig(local_enabled = True, remote_enabled = False)
    return [DefaultInfo(), ExternalRunnerTestInfo(
        type = "custom",
        command = ["sh", "-c", 'grep -q "$1" "$2"', "--", ctx.attrs.want, ctx.attrs.src],
        run_from_project_root = True,
        **kwargs
    )]
grep_test = rule(impl = _grep_test, attrs = {"src": attrs.source(), "want": attrs.string(), "local": attrs.bool(default = False)})
BZL
  printf 'load(":rules.bzl", "local_cat", "grep_test")\n' > "$d/BUCK"
  # A salt keeps the producers out of the remote action cache of earlier runs.
  for p in $PRODUCERS; do
    printf 'genrule(name = "%s", out = "%s.txt", cmd = "echo %s %s > $OUT")\n' $p $p $p "$SALT" >> "$d/BUCK"
  done
  cat >> "$d/BUCK" <<'BUCK'
genrule(name = "c_remote", srcs = [":p_remote"], out = "c_remote.txt", cmd = "sed 's/^/remote saw: /' $SRCS > $OUT")
local_cat(name = "c_local", src = ":p_local")
grep_test(name = "t_remote", src = ":p_test_remote", want = "p_test_remote")
grep_test(name = "t_local", src = ":p_test_local", want = "p_test_local", local = True)
genrule(name = "c_test_build", srcs = [":p_test_build"], out = "c_test_build.txt", cmd = "sed 's/^/built saw: /' $SRCS > $OUT")
grep_test(name = "t_build", src = ":c_test_build", want = "p_test_build")
BUCK
}

# The control gets a salt of its own: sharing one would give the loss arm the control's action
# cache hits and locally cached blobs, and no hook would fire.
SALT=ctl$(date +%s%N)
make_fixture "$K"
CTL_SALT=$SALT
SALT=loss$(date +%s%N)
make_fixture "$F"
make_fixture "$C"
fail=0
step() { # label, cmd...
  local l=$1; shift; local t0=$(date +%s.%N); "$@" > "$R/$l.log" 2>&1; local rc=$?; local t1=$(date +%s.%N)
  local cmds=$(grep -o 'Commands: [0-9]*[^)]*)' "$R/$l.log" | tail -1)
  printf '%s\trc=%s\twall=%.3fs\t%s\n' "$l" $rc $(echo "$t1 $t0" | awk '{print $1-$2}') "${cmds:-}" | tee -a $R/summary.tsv
  return $rc
}
check() { # label, condition...
  if "${@:2}"; then echo "ok	$1" | tee -a $R/summary.tsv; else echo "FAIL	$1" | tee -a $R/summary.tsv; fail=1; fi
}
event_log() { # step label: that step's event log
  local id=$(grep -o 'Build ID: [0-9a-f-]*' "$R/$1.log" | tail -1 | cut -d' ' -f3)
  ls buck-out/v2/log/*"_${id}_events.pb.zst"
}
ran() { # label: what that step's command executed, as `buck2 log what-ran` reports it
  "$B" log what-ran "$(event_log "$1")" > "$R/$1.what-ran" 2>&1
}
executions() { # step label, target name: how many times that step's command executed an action
  # of the target, from its events; `what-ran` leaves out an execution the RE service answered
  # from its own cache (RemoteServedFromCache), which a rewound action may get.
  "$B" log show "$(event_log "$1")" | python3 -I -c '
import json, sys
n = 0
for line in sys.stdin:
    try:
        e = json.loads(line)
    except ValueError:
        continue
    a = e.get("Event", {}).get("data", {}).get("SpanEnd", {}).get("data", {}).get("ActionExecution")
    if a and a.get("key", {}).get("owner", {}).get("TargetLabel", {}).get("label", {}).get("name") == sys.argv[1]:
        n += 1
print(n)
' "$2"
}
digests() { # "<sha256>:<size>" of each producer's output, which is "<name> <salt>\n". The RE
  # config hashes with SHA-256: the empty directory's digest in every event log is e3b0c442...
  local out="" p
  for p in $PRODUCERS; do
    out="$out $(printf '%s %s\n' $p "$SALT" | python3 -I -c 'import hashlib,sys; b=sys.stdin.buffer.read(); print(f"{hashlib.sha256(b).hexdigest()}:{len(b)}")')"
  done
  echo $out
}
fresh_runner() {
  find buck-out/v2 -mindepth 1 -maxdepth 1 ! -name cache -exec rm -rf {} +
  find buck-out/v2/cache -mindepth 1 -maxdepth 1 ! -name suspend ! -name dice_state -exec rm -rf {} +
}
echo "buck2 $("$B" --version 2>&1 | head -1) salt=$SALT control_salt=$CTL_SALT $(date -u +%FT%TZ)" | tee $R/summary.tsv

main_then_resume() { # prefix: build the producers, suspend --portable, wipe, resume
  local x=$1
  step $x-build-main "$B" build $(for p in $PRODUCERS; do printf '//:%s ' $p; done) --materializations=none
  step $x-suspend "$B" suspend --portable
  check "$x: the suspend says it is portable" grep -q 'suspended (portable)' "$R/$x-suspend.log"
  [ $x = loss ] && digests > "$R/$x-digests.txt"
  cp -r buck-out/v2/log "$R/$x-main-logs"
  fresh_runner
}

consumers() { # prefix
  local x=$1
  step $x-c_remote "$B" build //:c_remote --show-full-output
  step $x-c_local "$B" build //:c_local --show-full-output
  step $x-p_top "$B" build //:p_top --show-full-output --materializations=all
  step $x-p_default "$B" build //:p_default --show-full-output
  step $x-t_remote "$B" test //:t_remote
  step $x-t_local "$B" test //:t_local
  step $x-t_build "$B" test //:t_build
  for s in c_remote c_local p_top p_default t_remote t_local t_build; do ran $x-$s; done
}

out_of() { # step label, target
  grep -E "^root//:$2 " "$R/$1.log" | awk '{print $2}'
}

# Control: the same resume without the hooks; every command must pass.
cd "$K"
main_then_resume ctl
step ctl-resume "$B" resume
check "ctl: the daemon resumed" grep -q '^resumed:' "$R/ctl-resume.log"
consumers ctl
for s in c_remote c_local p_top p_default t_remote t_local t_build; do
  check "ctl: $s succeeds without lost blobs" grep -q "^ctl-$s	rc=0" "$R/summary.tsv"
done
check "ctl: no producer ran again" test "$(( $(executions ctl-c_remote p_remote) + $(executions ctl-c_local p_local) + $(executions ctl-p_top p_top) + $(executions ctl-p_default p_default) + $(executions ctl-t_remote p_test_remote) + $(executions ctl-t_local p_test_local) + $(executions ctl-t_build p_test_build) ))" -eq 0
"$B" kill > /dev/null 2>&1

cd "$F"
main_then_resume loss
export BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE="$(cat "$R/loss-digests.txt")"
export BUCK2_TEST_FAIL_RE_DOWNLOAD_DIGESTS_ONCE="$BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE"
step loss-resume "$B" resume
check "loss: the daemon resumed despite the test hooks" grep -q '^resumed:' "$R/loss-resume.log"
consumers loss
for s in c_remote c_local p_top p_default t_remote t_local t_build; do
  check "loss: $s succeeds after its input left the CAS" grep -q "^loss-$s	rc=0" "$R/summary.tsv"
done
check "loss: c_remote's producer ran again" test "$(executions loss-c_remote p_remote)" -ge 1
check "loss: c_local's producer ran again" test "$(executions loss-c_local p_local)" -ge 1
check "loss: p_top ran again" test "$(executions loss-p_top p_top)" -ge 1
check "loss: p_default ran again" test "$(executions loss-p_default p_default)" -ge 1
check "loss: t_remote's producer ran again" test "$(executions loss-t_remote p_test_remote)" -ge 1
check "loss: t_local's producer ran again" test "$(executions loss-t_local p_test_local)" -ge 1
check "loss: t_build's producer ran again" test "$(executions loss-t_build p_test_build)" -ge 1
grep -h 'Rewinding\|rewind' buck-out/v2/log/buckd.stderr > "$R/loss-rewinds.txt" 2>/dev/null
"$B" kill > /dev/null 2>&1
unset BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE BUCK2_TEST_FAIL_RE_DOWNLOAD_DIGESTS_ONCE

# Cold: the same targets in a third checkout, compared with what the resumed daemon produced.
cd "$C"
step cold-build "$B" build //:c_remote //:c_local //:p_top //:p_default --show-full-output
for t in c_remote c_local p_top p_default; do
  r=$(out_of loss-$t $t); c=$(out_of cold-build $t)
  check "loss: $t equals a cold build's" sh -c "test -n '$r' && cmp -s '$r' '$c'"
done
"$B" kill > /dev/null 2>&1

echo "result fail=$fail" | tee -a $R/summary.tsv
exit $fail
