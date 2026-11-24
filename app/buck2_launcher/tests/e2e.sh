#!/usr/bin/env bash
# End-to-end test of buck2-launcher with a real buck2.
#
#   tests/e2e.sh <buck2-launcher> <buck2>
#
# Builds a project with no prelude whose one rule writes `[e2e] msg`, under
# two channels that both run the given buck2 and two profiles whose configs
# set different values, so the built output says which config the daemon
# read. Checks zero config, a switch, the daemon restart it forces, the
# banner, a switch back, a person's own file, and the key refusal.
set -euo pipefail

launcher=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
real=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
work=$(mktemp -d)
trap 'cd /; (cd "$work/repo" 2>/dev/null && "$real" kill >/dev/null 2>&1) || true; rm -rf "$work"' EXIT
repo=$work/repo
mkdir -p "$repo/tools/buck2/buckconfig.d" "$repo/channels" "$work/home"

fail() { echo "FAIL: $*" >&2; exit 1; }
expect() { grep -qF -- "$2" <<<"$1" || fail "expected '$2' in: $1"; }
reject() { if grep -qF -- "$2" <<<"$1"; then fail "did not expect '$2' in: $1"; fi; }

# Two channels, one binary; the comment is the revision the launcher reads.
for c in current:1111111111aa next:2222222222bb; do
  name=${c%%:*}; rev=${c#*:}
  printf '#!/bin/sh\n# /nix/store/x-buck2-git-%s-wrapped\nexec "%s" "$@"\n' "$rev" "$real" \
    > "$repo/channels/buck2-$name"
  chmod +x "$repo/channels/buck2-$name"
done

# As in mwb, `.buckconfig` holds only what must be there and the defaults are
# in `.buckconfig.d`: buck2 reads that directory in name order, and
# `.buckconfig` itself takes precedence over it.
cat > "$repo/.buckconfig" <<'CFG'
[cells]
root = .
[buildfile]
name = BUCK
CFG
mkdir -p "$repo/.buckconfig.d"
printf '[e2e]\nmsg = zero\n' > "$repo/.buckconfig.d/00-defaults"
cat > "$repo/defs.bzl" <<'BZL'
def _impl(ctx):
    out = ctx.actions.write("msg.txt", ctx.attrs.msg)
    return [DefaultInfo(default_output = out)]

text = rule(impl = _impl, attrs = {"msg": attrs.string()})
BZL
cat > "$repo/BUCK" <<'BUCKFILE'
load(":defs.bzl", "text")
text(name = "msg", msg = read_config("e2e", "msg", "unset"))
BUCKFILE
printf '[e2e]\nmsg = next\n' > "$repo/tools/buck2/buckconfig.d/next"
printf '[e2e]\nmsg = keyed\n' > "$repo/tools/buck2/buckconfig.d/keyed"
cat > "$repo/tools/buck2/profiles.toml" <<'TOML'
default = "current"

[credentials]
file = "~/.e2e-credentials"

[channels.stable]
exec = "channels/buck2-current"
from = "stable"

[channels.candidate]
exec = "channels/buck2-next"
from = "candidate"

[profiles.current]
channel = "stable"
summary = "the checked-in config"

[profiles.next]
channel = "candidate"
config = "tools/buck2/buckconfig.d/next"
summary = "msg is next"

[profiles.keyed]
channel = "candidate"
config = "tools/buck2/buckconfig.d/keyed"
summary = "needs a key"
needs_key = true
TOML

cd "$repo"
export HOME=$work/home
b() { "$launcher" "$@" 2>"$work/err"; }
built() {
  b build //:msg --show-output >"$work/out"
  cat "$(awk '/msg.txt/{print $2}' "$work/out")"
}

echo "1. zero config: nothing extra, no files written"
[ "$(built)" = zero ] || fail "zero config built $(cat "$work/out")"
reject "$(cat "$work/err")" "buck2: "
[ ! -e .buck2-profile ] || fail ".buck2-profile written in zero config"
[ ! -e .buckconfig.d/50-local-profile ] || fail "profile file written in zero config"

echo "2. --profile shows the default"
out=$("$launcher" --profile 2>&1)
expect "$out" "buck2: profile current (default)"
expect "$out" "binary  stable 1111111111"

echo "3. switch to next: config written, daemon stopped, says so"
out=$("$launcher" --profile next 2>&1)
expect "$out" "buck2: profile next (was current)"
expect "$out" "wrote .buckconfig.d/50-local-profile"
expect "$out" "daemon  stopped (was stable 1111111111)"
head -1 .buckconfig.d/50-local-profile | grep -q '^# written by buck2 --profile:' \
  || fail "profile file has no header"

echo "4. a build under next reads next's config, with one banner line"
[ "$(built)" = next ] || fail "next built $(cat "$work/out")"
expect "$(cat "$work/err")" "buck2: next · buck2 2222222222 · msg is next"

echo "5. buck2's own flags before the subcommand pass through"
b --isolation-dir e2e2 build //:msg >/dev/null || fail "--isolation-dir call failed: $(cat "$work/err")"
"$real" --isolation-dir e2e2 kill >/dev/null 2>&1 || true

echo "6. back to current: file removed, daemon restarted, zero config again"
out=$("$launcher" --profile current 2>&1)
expect "$out" "removed .buckconfig.d/50-local-profile"
[ "$(built)" = zero ] || fail "current built $(cat "$work/out")"
reject "$(cat "$work/err")" "buck2: "
[ ! -e .buck2-profile ] || fail ".buck2-profile left behind"

echo "7. a file a person wrote is not overwritten"
mkdir -p .buckconfig.d && printf '[e2e]\nmsg = mine\n' > .buckconfig.d/50-local-profile
if out=$("$launcher" --profile next 2>&1); then fail "overwrote a person's file"; fi
expect "$out" "not written by buck2 --profile"
[ "$(built)" = mine ] || fail "person's file not in force"
rm .buckconfig.d/50-local-profile

echo "8. a profile that needs a key is refused without one, and changes nothing"
if out=$("$launcher" --profile keyed </dev/null 2>&1); then fail "keyed switched without a key"; fi
expect "$out" "needs a BuildBuddy key"
[ ! -e .buckconfig.d/50-local-profile ] || fail "refused switch wrote a file"

echo "9. with a key it switches, and the key is never printed"
printf 'SECRET-E2E-KEY\n' > "$HOME/.e2e-credentials"
out=$("$launcher" --profile keyed 2>&1)
expect "$out" "key     present"
reject "$out" "SECRET-E2E-KEY"
[ "$(built)" = keyed ] || fail "keyed built $(cat "$work/out")"
out=$("$launcher" --profile 2>&1)
reject "$out" "SECRET-E2E-KEY"
"$launcher" --profile current >/dev/null 2>&1

echo "PASS"
