#!/usr/bin/env bash
# What a fresh runner runs after `buck2 resume` of a portable image, when the suspended daemon ran
# local actions that feed remote ones.
#   e2e-local-cutoff.sh <buck2 binary> <workdir> <RE buckconfig>
# The fixture has lab's nix shape: `realise`, a local_only action that leaves a symlink to a path
# of an immutable store outside the project (a fake one under <workdir>/store, created if absent
# as nix realises a path); `read_link`, a local_only action that writes the link's target to a
# file; `stable`, a local_only action whose output never changes; `sha`, a local_only action that
# writes $GITHUB_SHA. A remote chain c0..c9 reads read_link and stable; `after` reads sha.
# STORE_KWARG=1 marks `realise` with `store_path_outputs = True` (needs a buck2 that has it);
# STORE_CHECK sets `buck2_hydration.suspend_store_check`; EXPECT_KEPT=1 says cycle 1 must keep
# realise's value (its store path is present and passes the check) rather than run it again.
# Writes <workdir>/results/summary.tsv: one line per step, then the actions each resumed build
# ran, and `ok`/`FAIL` per check. Exits non-zero if a check failed.
set -uo pipefail
B=$1; W=$2; RE_CONFIG=$3
RE_PROPERTIES=${RE_PROPERTIES:-'{"OSFamily": "Linux", "Arch": "amd64", "Pool": "usw2", "container-image": "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"}'}
STORE_KWARG=${STORE_KWARG:-0}
R=$W/results; F=$W/fixture; S=$W/store
rm -rf "$W"; mkdir -p "$R" "$S" "$F/platforms" "$F/toolchains" "$F/.buckconfig.d"
SALT=$(date +%s%N)
STORE_PATH=$S/0abc${SALT}-tool
fail=0

cp "$RE_CONFIG" "$F/.buckconfig.d/re.buckconfig"
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
execution_platforms = root//platforms:hybrid
[buck2]
file_watcher = fs_hash_crawler
materializations = deferred
sqlite_materializer_state = true
[buck2_hydration]
enable_paging = true
suspend = true
host_identity = e2e-local-cutoff
CFG
if [ -n "${STORE_CHECK:-}" ]; then printf 'suspend_store_check = %s\n' "$STORE_CHECK" >> "$F/.buckconfig"; fi
cat > "$F/platforms/defs.bzl" <<BZL
def _impl(ctx):
    p = ExecutionPlatformInfo(label = ctx.label.raw_target(), configuration = ConfigurationInfo(constraints = {}, values = {}),
        executor_config = CommandExecutorConfig(local_enabled = True, remote_enabled = True, remote_cache_enabled = True,
            allow_cache_uploads = False, use_limited_hybrid = True, remote_execution_properties = $RE_PROPERTIES,
            remote_execution_use_case = "buck2-default", remote_output_paths = "output_paths"))
    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [p])]
hybrid_platform = rule(impl = _impl, attrs = {})
BZL
printf 'load(":defs.bzl", "hybrid_platform")\nhybrid_platform(name = "hybrid")\n' > "$F/platforms/BUCK"
printf 'load("@prelude//toolchains:demo.bzl", "system_demo_toolchains")\nsystem_demo_toolchains()\n' > "$F/toolchains/BUCK"
if [ "$STORE_KWARG" = 1 ]; then kw=', store_path_outputs = True'; else kw=''; fi
cat > "$F/local.bzl" <<BZL
def _realise(ctx):
    out = ctx.actions.declare_output("out.link")
    ctx.actions.run(cmd_args("sh", "-c", '[ -e "\$1" ] || printf tool > "\$1"; ln -s "\$1" "\$2"', "--", "$STORE_PATH", out.as_output()),
        category = "fake_nix_build", local_only = True, allow_cache_upload = False$kw)
    return [DefaultInfo(default_output = out)]
realise = rule(impl = _realise, attrs = {})

def _read_link(ctx):
    out = ctx.actions.declare_output("path")
    ctx.actions.run(cmd_args("sh", "-c", 'readlink "\$1" | tr -d "\\\\n" > "\$2"', "--", ctx.attrs.link[DefaultInfo].default_outputs[0], out.as_output()),
        category = "fake_nix_path", local_only = True, allow_cache_upload = False)
    return [DefaultInfo(default_output = out)]
read_link = rule(impl = _read_link, attrs = {"link": attrs.dep()})

def _stable(ctx):
    out = ctx.actions.declare_output("stable.txt")
    ctx.actions.run(cmd_args("sh", "-c", 'echo stable > "\$1"', "--", out.as_output()), category = "stable", local_only = True)
    return [DefaultInfo(default_output = out)]
stable = rule(impl = _stable, attrs = {})

def _sha(ctx):
    out = ctx.actions.declare_output("sha.txt")
    ctx.actions.run(cmd_args("sh", "-c", 'echo "\$GITHUB_SHA" > "\$1"', "--", out.as_output()), category = "local_sha", local_only = True)
    return [DefaultInfo(default_output = out)]
sha = rule(impl = _sha, attrs = {})

# The shape of the go stdlib under lab's toolchain: a remote action over a local output, whose
# output path is content based, read by a dynamic_output that registers remote actions.
def _listing(ctx):
    out = ctx.actions.declare_output("listing.txt", has_content_based_path = True)
    ctx.actions.run(cmd_args("sh", "-c", 'cat "\$1" > "\$2"; echo >> "\$2"; echo "a b c $SALT" >> "\$2"', "--", ctx.attrs.src[DefaultInfo].default_outputs[0], out.as_output()),
        category = "listing")
    return [DefaultInfo(default_output = out)]
listing = rule(impl = _listing, attrs = {"src": attrs.dep()})

def _expand_impl(actions, listing, outs):
    names = listing.read_string().splitlines()[-1].split(" ")[:3]
    for name in names:
        actions.run(cmd_args("sh", "-c", 'echo "\$1" > "\$2"', "--", name, outs[name]), category = "expand", identifier = name)
    return []

_expand = dynamic_actions(impl = _expand_impl, attrs = {"listing": dynattrs.artifact_value(), "outs": dynattrs.dict(str, dynattrs.output())})

def _expanded(ctx):
    outs = {n: ctx.actions.declare_output(n + ".txt", has_content_based_path = True) for n in ["a", "b", "c"]}
    ctx.actions.dynamic_output_new(_expand(listing = ctx.attrs.listing[DefaultInfo].default_outputs[0], outs = {k: v.as_output() for k, v in outs.items()}))
    return [DefaultInfo(default_outputs = outs.values())]
expanded = rule(impl = _expanded, attrs = {"listing": attrs.dep()})

# The go stdlib again: one action with dep files built in two configurations. The second build
# is served from the identical-action cache (ActionExecutionKind::LocalActionCache) with the
# first one's outputs: remote ones for twin, local ones for twin_local, which a resume must run
# again and whose dependents its unchanged outputs must cut off.
def _twin(ctx):
    out = ctx.actions.declare_output("twin.txt", has_content_based_path = True)
    dep = ctx.actions.declare_output("twin.dep", has_content_based_path = True)
    tag = ctx.actions.artifact_tag()
    ctx.actions.run(cmd_args("sh", "-c", 'cat "\$1" > "\$2"; echo >> "\$2"; echo "a b c $SALT" >> "\$2"; echo "\$1" > "\$3"', "--",
        tag.tag_artifacts(ctx.attrs.src), out.as_output(), tag.tag_artifacts(dep.as_output())),
        category = "twin", dep_files = {"used": tag}, local_only = ctx.attrs.local)
    return [DefaultInfo(default_output = out)]
twin = rule(impl = _twin, attrs = {"src": attrs.source(), "local": attrs.bool(default = False)})
BZL
{
  printf 'load(":local.bzl", "realise", "read_link", "stable", "sha", "listing", "expanded")\n'
  printf 'listing(name = "listing", src = ":path")\nexpanded(name = "expanded", listing = ":listing")\n'
  printf 'load(":local.bzl", "twin")\ntwin(name = "twin", src = "twin.src")\nexpanded(name = "expanded_twin", listing = ":twin")\n'
  printf 'twin(name = "twin_local", src = "twin.src", local = True)\nexpanded(name = "expanded_twin_local", listing = ":twin_local")\n'
  printf 'constraint_setting(name = "flavour")\nconstraint_value(name = "red", constraint_setting = ":flavour")\n'
  printf 'platform(name = "red_platform", constraint_values = [":red"])\nconfigured_alias(name = "twin_red", actual = ":twin", platform = ":red_platform")\n'
  printf 'configured_alias(name = "twin_local_red", actual = ":twin_local", platform = ":red_platform")\n'
  printf 'realise(name = "realise")\nread_link(name = "path", link = ":realise")\nstable(name = "stable")\nsha(name = "sha")\n'
  for i in $(seq 0 9); do
    if [ $i -eq 0 ]; then srcs='":path", ":stable"'; else srcs="\":c$((i-1))\""; fi
    printf 'genrule(name = "c%d", srcs = [%s], out = "c%d.txt", cmd = "cat $SRCS > $OUT; echo c%d %s >> $OUT")\n' $i "$srcs" $i $i "$SALT"
  done
  printf 'genrule(name = "after", srcs = [":sha", ":c9"], out = "after.txt", cmd = "cat $SRCS > $OUT")\n'
} > "$F/BUCK"
echo twin > "$F/twin.src"

step() { # label, cmd...: runs under a trace id of its own, kept in $R/<label>.id
  local l=$1; shift; local id=$(cat /proc/sys/kernel/random/uuid); echo "$id" > "$R/$l.id"
  local t0=$(date +%s.%N); (export BUCK_WRAPPER_UUID=$id; "$@") > "$R/$l.log" 2>&1; local rc=$?; local t1=$(date +%s.%N)
  local cmds=$(grep -o 'Commands: [0-9]*[^)]*)' "$R/$l.log" | tail -1)
  printf '%s\trc=%s\twall=%.3fs\t%s\tid=%s\n' "$l" $rc $(echo "$t1 $t0" | awk '{print $1-$2}') "${cmds:-}" "$id" | tee -a $R/summary.tsv
  return $rc
}
check() { if "${@:2}"; then echo "ok	$1" | tee -a $R/summary.tsv; else echo "FAIL	$1" | tee -a $R/summary.tsv; fail=1; fi; }
evlog() { # a step's event log, by its trace id; waits for it to appear
  local id f i; id=$(cat "$R/$1.id" 2>/dev/null) || return
  for i in $(seq 1 60); do
    f=$(ls buck-out/v2/log/*_"$id"_events.pb.zst 2>/dev/null | head -1)
    [ -n "$f" ] && { echo "$f"; return; }
    sleep 0.5
  done
}
ran() { # writes <step>.what-ran; prints `?` when the log is missing, never a false 0
  local f; f=$(evlog "$1"); if [ -z "$f" ]; then echo '?'; return; fi
  "$B" log what-ran "$f" > "$R/$1.what-ran" 2>&1
  grep -c . "$R/$1.what-ran"
}
ran_targets() { awk -F'\t' '{split($2, a, " "); print a[1]}' "$R/$1.what-ran" | sort | uniq -c | awk '{printf "%s%s", sep, $2 (($1>1) ? "x" $1 : ""); sep=" "}'; }
chain_count() { local n=0 i; for i in $(seq 0 9); do n=$((n + $(ran_count "$1" root//:c$i))); done; echo $n; }
ran_count() { awk -F'\t' -v t="$2" 'index($2, t " ") == 1' "$R/$1.what-ran" | grep -c .; }
fresh_runner() { # the checkout at the same path, and of buck-out only the image and its paging store
  cp buck-out/v2/log/*.pb.zst "$R/" 2>/dev/null
  find buck-out/v2 -mindepth 1 -maxdepth 1 ! -name cache -exec rm -rf {} +
  find buck-out/v2/cache -mindepth 1 -maxdepth 1 ! -name suspend ! -name dice_state -exec rm -rf {} +
}
echo "buck2 $("$B" --version 2>&1 | head -1) salt=$SALT store_kwarg=$STORE_KWARG store_check=${STORE_CHECK:-} $(date -u +%FT%TZ)" | tee $R/summary.tsv

cd "$F" || exit 1
# Files written within buck2's racy window (2 s) of a file-watcher sync are reported changed again
# at the next one, which would dirty the packages at the suspend; start with older sources.
sleep 3
TARGETS=(//:after //:c9 //:realise //:path //:expanded //:expanded_twin //:expanded_twin_local)
export GITHUB_SHA=sha-main
# The other configuration first, so that the main build serves :twin from the identical-action cache.
step build-main-red "$B" build //:twin_red //:twin_local_red
step build-main "$B" build "${TARGETS[@]}"
kind_of() { # step, category: the execution kinds that step's actions of that category ended with
  "$B" log show "$(evlog "$1")" 2>/dev/null | grep SpanEnd | grep "\"category\":\"$2\"" | grep -o '"execution_kind":[0-9]*' | cut -d: -f2 | sort | uniq -c | xargs
}
echo "build-main twin kinds (count kind ...): $(kind_of build-main twin)" | tee -a $R/summary.tsv
check "main served twin from the identical-action cache (kind 10)" sh -c "kind_of() { :; }; echo '$(kind_of build-main twin)' | grep -qw 10"
echo "build-main ran=$(ran build-main): $(ran_targets build-main)" | tee -a $R/summary.tsv
check "main ran the chain" test "$(ran_count build-main root//:c9)" -ge 1
check "main realised the store path" test -f "$STORE_PATH"

# Cycle 1: a fresh runner whose store holds the path, as a runner image with the toolchain does.
step suspend-1 "$B" suspend --portable
# realise, path, stable, sha and twin_local in both configurations; not twin, whose outputs came
# from remote execution.
check "cycle 1: the suspend lists 6 local actions" grep -q ' 6 locally executed actions' "$R/suspend-1.log"
fresh_runner
export GITHUB_SHA=sha-pr
step resume-1 "$B" resume
grep -E '^(resumed|cold)|locally executed' "$R/resume-1.log" | sed 's/^/resume-1: /' | tee -a $R/summary.tsv
step build-1 "$B" build "${TARGETS[@]}" --show-full-output
echo "build-1 ran=$(ran build-1): $(ran_targets build-1)" | tee -a $R/summary.tsv
after=$(grep -E '^root//:after ' "$R/build-1.log" | awk '{print $2}')
path=$(grep -E '^root//:path ' "$R/build-1.log" | awk '{print $2}')
link=$(grep -E '^root//:realise ' "$R/build-1.log" | awk '{print $2}')
check "cycle 1: the chain was cut off: no c_i ran" test "$(chain_count build-1)" -eq 0
check "cycle 1: listing and expanded were cut off" test "$(( $(ran_count build-1 root//:listing) + $(ran_count build-1 root//:expanded) ))" -eq 0
check "cycle 1: expanded_twin was cut off" test "$(ran_count build-1 root//:expanded_twin)" -eq 0
check "cycle 1: twin, served with remote outputs, was not run again" test "$(ran_count build-1 root//:twin)" -eq 0
check "cycle 1: twin_local, served with local outputs, ran again" test "$(ran_count build-1 root//:twin_local)" -ge 1
check "cycle 1: expanded_twin_local was cut off" test "$(ran_count build-1 root//:expanded_twin_local)" -eq 0
check "cycle 1: sha ran again with the PR's environment" grep -q '^sha-pr$' "$after"
check "cycle 1: after ran again, since sha's output changed" test "$(ran_count build-1 root//:after)" -ge 1
check "cycle 1: the link names the store path" test "$(readlink "$link")" = "$STORE_PATH"
check "cycle 1: read_link's output names the store path" test "$(cat "$path")" = "$STORE_PATH"
if [ "${EXPECT_KEPT:-0}" = 1 ]; then
  check "cycle 1: realise kept: its store path is valid here" test "$(ran_count build-1 root//:realise)" -eq 0
else
  check "cycle 1: realise ran again" test "$(ran_count build-1 root//:realise)" -ge 1
fi

# Cycle 2: a fresh runner whose store lacks the path. realise must run and realise it again.
step suspend-2 "$B" suspend --portable
fresh_runner
rm -f "$STORE_PATH"
export GITHUB_SHA=sha-pr2
step resume-2 "$B" resume
grep -E '^(resumed|cold)|locally executed' "$R/resume-2.log" | sed 's/^/resume-2: /' | tee -a $R/summary.tsv
step build-2 "$B" build "${TARGETS[@]}" --show-full-output
echo "build-2 ran=$(ran build-2): $(ran_targets build-2)" | tee -a $R/summary.tsv
link=$(grep -E '^root//:realise ' "$R/build-2.log" | awk '{print $2}')
check "cycle 2: realise ran again: the store path was missing" test "$(ran_count build-2 root//:realise)" -ge 1
check "cycle 2: the store path exists again" test -f "$STORE_PATH"
check "cycle 2: the link names it" test "$(readlink "$link")" = "$STORE_PATH"
check "cycle 2: the chain was cut off" test "$(chain_count build-2)" -eq 0
check "cycle 2: listing and expanded were cut off" test "$(( $(ran_count build-2 root//:listing) + $(ran_count build-2 root//:expanded) ))" -eq 0
"$B" kill > /dev/null 2>&1

echo "result fail=$fail" | tee -a $R/summary.tsv
exit $fail
