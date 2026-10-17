#!/usr/bin/env bash
# End-to-end check of `buck2 suspend --portable` on remote execution: a remote genrule chain,
# one `local_only` genrule that writes $GITHUB_SHA, and a remote genrule over both. The image is
# resumed over a buck-out that keeps only cache/suspend and cache/dice_state, as on a fresh
# runner, with another GITHUB_SHA.
#   e2e-re.sh <buck2 binary> <workdir> <RE buckconfig>
# The RE buckconfig holds [buck2_re_client] (the daemon reads it only from config files) and is
# copied into the fixture's .buckconfig.d/. RE_PROPERTIES overrides the executor properties
# (Starlark dict literal). Writes <workdir>/results/summary.tsv, one log per step, and exits
# non-zero if a check fails.
set -uo pipefail
B=$1; W=$2; RE_CONFIG=$3
RE_PROPERTIES=${RE_PROPERTIES:-'{"OSFamily": "Linux", "Arch": "amd64", "Pool": "usw2", "container-image": "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"}'}
R=$W/results; F=$W/fixture; C=$W/cold; N=$W/not-portable; L=$W/chain
rm -rf "$W"; mkdir -p "$R"

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
CFG
  # Lab's hybrid shape: remote by default, the local executor only for `local_only` actions,
  # and no cache upload from a local execution.
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
  printf 'load(":local.bzl", "local_sha")\n' > "$d/BUCK"
  # A salt keeps the chain's actions out of the remote action cache of earlier runs, so the
  # first build executes them.
  for i in $(seq 0 9); do
    if [ $i -eq 0 ]; then srcs=""; else srcs="srcs = [\":r$((i-1))\"], "; fi
    printf 'genrule(name = "r%d", %sout = "r%d.txt", cmd = "cat $SRCS > $OUT 2>/dev/null; echo r%d %s >> $OUT")\n' \
      $i "$srcs" $i $i "$SALT" >> "$d/BUCK"
  done
  # A local action reads the daemon's environment, as a nix realisation reads the machine.
  cat > "$d/local.bzl" <<'BZL'
def _impl(ctx):
    out = ctx.actions.declare_output(ctx.label.name + ".txt")
    ctx.actions.run(cmd_args("sh", "-c", 'echo "$GITHUB_SHA" > "$1"', "--", out.as_output()), category = "local_sha", local_only = True)
    return [DefaultInfo(default_output = out)]
local_sha = rule(impl = _impl, attrs = {})
BZL
  printf 'local_sha(name = "sha")\n' >> "$d/BUCK"
  printf 'genrule(name = "after", srcs = [":sha", ":r9"], out = "after.txt", cmd = "cat $SRCS > $OUT")\n' >> "$d/BUCK"
}

SALT=$(date +%s%N)
make_fixture "$F"
make_fixture "$C"
make_fixture "$N"
make_fixture "$L"
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
ran() { # label: what that step's command executed, as `buck2 log what-ran` reports it
  local id=$(grep -o 'Build ID: [0-9a-f-]*' "$R/$1.log" | tail -1 | cut -d' ' -f3)
  "$B" log what-ran buck-out/v2/log/*"_${id}_events.pb.zst" > "$R/$1.what-ran" 2>&1
}
ran_where() { # what-ran file, target: the executor column of the target's actions
  awk -F'\t' -v t="$2" 'index($2, t " ") == 1 { print $3 }' "$1"
}
echo "buck2 $("$B" --version 2>&1 | head -1) salt=$SALT $(date -u +%FT%TZ)" | tee $R/summary.tsv

cd "$F"
export GITHUB_SHA=sha-main
step build-main "$B" build //:after
ran build-main
check "the main build ran the chain" test -n "$(ran_where "$R/build-main.what-ran" root//:r9)"
step suspend-portable "$B" suspend --portable
check "the suspend says it is portable" grep -q 'suspended (portable)' "$R/suspend-portable.log"
check "the local output was uploaded: one locally executed action" grep -q ' 1 locally executed actions' "$R/suspend-portable.log"

# A fresh runner: the checkout at the same path, and of buck-out only the image and its paging
# store; no materializer state, no outputs.
fresh_runner() {
  find buck-out/v2 -mindepth 1 -maxdepth 1 ! -name cache -exec rm -rf {} +
  find buck-out/v2/cache -mindepth 1 -maxdepth 1 ! -name suspend ! -name dice_state -exec rm -rf {} +
}
fresh_runner
ls -R buck-out > "$R/buck-out-after-wipe.txt"

export GITHUB_SHA=sha-pr
step resume "$B" resume
check "the daemon resumed" grep -q '^resumed:' "$R/resume.log"
check "nothing was lost" sh -c "! grep -q 'not portable' '$R/resume.log'"
check "artifacts come back from the CAS" grep -qE '[1-9][0-9]* to fetch from the CAS' "$R/resume.log"
check "the local action is dirtied" grep -q '; 1 locally executed actions to run again' "$R/resume.log"

step build-pr "$B" build //:after //:r9 --show-full-output
ran build-pr
after=$(grep -E '^root//:after ' "$R/build-pr.log" | awk '{print $2}')
r9=$(grep -E '^root//:r9 ' "$R/build-pr.log" | awk '{print $2}')
check "the local action ran again with the PR's environment" grep -q '^sha-pr$' "$after"
check "what-ran lists the local action, run locally" test "$(ran_where "$R/build-pr.what-ran" root//:sha)" = local
check "the chain was not run again" test -z "$(for i in $(seq 0 9); do ran_where "$R/build-pr.what-ran" root//:r$i; done)"
check "a chain output fetched from the CAS has its content" grep -q "^r9 $SALT$" "$r9"
"$B" kill > /dev/null 2>&1

# The same targets cold, in another checkout, with the PR's environment: equal outputs.
cd "$C"
step build-cold "$B" build //:after //:r9 --show-full-output
cafter=$(grep -E '^root//:after ' "$R/build-cold.log" | awk '{print $2}')
cr9=$(grep -E '^root//:r9 ' "$R/build-cold.log" | awk '{print $2}')
check "after equals a cold build's" cmp -s "$after" "$cafter"
check "r9 equals a cold build's" cmp -s "$r9" "$cr9"
"$B" kill > /dev/null 2>&1
# Without --portable the local output has no way back, and the fresh runner starts cold.
cd "$N"
export GITHUB_SHA=sha-main
step np-build-main "$B" build //:after
step np-suspend "$B" suspend
fresh_runner
export GITHUB_SHA=sha-pr
step np-resume "$B" resume
check "a non-portable image is refused on a fresh runner" grep -qE '^cold: the image is not portable: [1-9][0-9]* materialized artifacts' "$R/np-resume.log"
"$B" kill > /dev/null 2>&1

# A chain: the main daemon resumes its own image on the same machine with the same environment,
# so nothing is dirtied, and is suspended again, portable, without a build in between. Its
# local action did not run in the resumed daemon, and must still be in the second image, or the
# fresh runner keeps main's output.
cd "$L"
export GITHUB_SHA=sha-main
step ch-build-main "$B" build //:after
step ch-suspend "$B" suspend
step ch-resume-same "$B" resume
check "chain: the same-host resume dirties nothing" grep -q '; 0 locally executed actions to run again' "$R/ch-resume-same.log"
step ch-suspend-portable "$B" suspend --portable
check "chain: the second image still lists the local action" grep -q ' 1 locally executed actions' "$R/ch-suspend-portable.log"
fresh_runner
export GITHUB_SHA=sha-pr
step ch-resume "$B" resume
check "chain: the fresh runner dirties the local action" grep -q '; 1 locally executed actions to run again' "$R/ch-resume.log"
step ch-build-pr "$B" build //:after --show-full-output
chafter=$(grep -E '^root//:after ' "$R/ch-build-pr.log" | awk '{print $2}')
check "chain: the local action ran again with the PR's environment" grep -q '^sha-pr$' "$chafter"
"$B" kill > /dev/null 2>&1

echo "result fail=$fail" | tee -a $R/summary.tsv
exit $fail
