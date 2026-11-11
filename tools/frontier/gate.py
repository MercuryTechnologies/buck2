#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

"""The frontier's determinism gate (phase 1), on the prelude-free fixture in
tests/frontier/fixture. Everything runs locally: no remote backend, no remote cache.

Each prime arm starts a fresh daemon, builds //..., primes a local store and is killed.
The arms differ from A in one thing each (design v2, section 9):

  A2  nothing (a repeat, same isolation dir, fresh buck-out)
  B   isolation dir
  C   project root path (a second checkout of the same commit)
  D   HOME, TMPDIR, LANG and one extra variable
  E   -j 1
  F   sentinel values in .buckconfig.local, ~/.buckconfig.d, -c and the environment

Then a shadow daemon on A's checkout checks A's store before and after each premise canary,
and after each revert. The gate fails on:

  1. a covered node (kind, key id) present in one arm and not another;
  2. a value-row difference for a covered node in any pair;
  3. an injected leaf whose digest differs in a pair where nothing it covers changed;
  4. a canary that leaves a record above the mutated leaf valid, invalidates a record whose
     closure does not hold a leaf of the mutation, or names another leaf;
  5. a verdict that does not return to hit after the revert;
  6. a sentinel's bytes, or its sha256 or blake3, anywhere in arm F's store;
  7. any `valid but different`.

Usage: python3 -I gate.py --buck2 <binary> --fixture <dir> --work <dir> [--keep]
"""

import argparse
import glob
import hashlib
import json
import os
import shutil
import subprocess
import sys

SENTINEL = "frontier-sentinel-7f3a9c"
SENTINEL_LOCAL = SENTINEL + "-local"
SENTINEL_HOME = SENTINEL + "-home"
SENTINEL_ARG = SENTINEL + "-arg"
SENTINEL_ENV = SENTINEL + "-env"

FAILURES = []


def fail(msg):
    FAILURES.append(msg)
    print("FAIL:", msg, flush=True)


def note(msg):
    print(msg, flush=True)


class Gate:
    def __init__(self, args):
        self.b = os.path.abspath(args.buck2)
        self.fixture = os.path.abspath(args.fixture)
        self.w = os.path.abspath(args.work)
        self.logs = os.path.join(self.w, "logs")
        os.makedirs(self.logs, exist_ok=True)
        self.base_env = {
            k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "USER", "LANG", "TMPDIR")
        }
        self.base_env.setdefault("HOME", os.path.join(self.w, "home"))
        self.base_env["HOME"] = os.path.join(self.w, "home")
        os.makedirs(self.base_env["HOME"], exist_ok=True)
        self.count = 0

    def checkout(self, name):
        d = os.path.join(self.w, name)
        if os.path.exists(d):
            shutil.rmtree(d)
        shutil.copytree(self.fixture, d, symlinks=True)
        os.rename(os.path.join(d, "gitignore.fixture"), os.path.join(d, ".gitignore"))
        env = dict(self.base_env)
        env.update(
            GIT_AUTHOR_NAME="gate",
            GIT_AUTHOR_EMAIL="gate@example.invalid",
            GIT_COMMITTER_NAME="gate",
            GIT_COMMITTER_EMAIL="gate@example.invalid",
            GIT_AUTHOR_DATE="2026-01-01T00:00:00Z",
            GIT_COMMITTER_DATE="2026-01-01T00:00:00Z",
        )
        for cmd in (
            ["git", "init", "-q", "-b", "main"],
            ["git", "add", "-A"],
            ["git", "commit", "-q", "-m", "fixture"],
        ):
            subprocess.run(cmd, cwd=d, env=env, check=True)
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=d, env=env, check=True, capture_output=True, text=True
        ).stdout.strip()
        return d, sha

    def buck2(self, cwd, iso, args, env=None, settings=(), check=True, name=None):
        self.count += 1
        cmd = [self.b, "--isolation-dir", iso, *args[:1]]
        for s in settings:
            cmd += ["--setting", s]
        cmd += args[1:]
        e = dict(env or self.base_env)
        name = name or f"{self.count:03d}-{args[0]}"
        with open(os.path.join(self.logs, name + ".out"), "w") as out, open(
            os.path.join(self.logs, name + ".err"), "w"
        ) as err:
            note(f"$ ({os.path.basename(cwd)}) {' '.join(cmd[1:])}")
            r = subprocess.run(cmd, cwd=cwd, env=e, stdout=out, stderr=err, timeout=900)
        text = open(os.path.join(self.logs, name + ".out")).read()
        if check and r.returncode != 0:
            fail(f"{' '.join(args)} in {cwd} exited {r.returncode}; see {name}.err")
        return r.returncode, text

    def kill(self, cwd, iso, env=None):
        subprocess.run(
            [self.b, "--isolation-dir", iso, "kill"],
            cwd=cwd,
            env=dict(env or self.base_env),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=300,
        )

    def prime_arm(self, arm, cwd, sha, iso, env=None, build_extra=(), config=()):
        settings = ["hydration.enable_paging=true", "frontier.mode=prime"]
        cfg = []
        for c in config:
            cfg += ["-c", c]
        store = os.path.join(self.w, "store-" + arm)
        if os.path.exists(store):
            shutil.rmtree(store)
        self.buck2(cwd, iso, ["build", *build_extra, *cfg, "//..."], env, settings, name=f"{arm}-build")
        _, out = self.buck2(
            cwd,
            iso,
            ["debug", "hydration", "frontier-prime", "--store", store, "--commit", sha],
            env,
            settings,
            name=f"{arm}-prime",
        )
        note(out.rstrip())
        self.kill(cwd, iso, env)
        bo = os.path.join(cwd, "buck-out", iso)
        if os.path.exists(bo):
            shutil.move(bo, os.path.join(self.w, f"buck-out-{arm}"))
        return store


def load_store(store):
    """(kind, key id) -> (value row, {leaf json: canon} over the record's closure)."""
    out = {}
    vdirs = [d for d in glob.glob(os.path.join(store, "*")) if os.path.basename(d) not in ("commits",)]
    bodies = {}
    for v in vdirs:
        for p in glob.glob(os.path.join(v, "inv", "*.json")):
            bodies[os.path.basename(p)[:-5]] = json.load(open(p))
    unshareable = set()
    for v in vdirs:
        for p in glob.glob(os.path.join(v, "unshareable", "*.tsv")):
            for line in open(p).read().splitlines()[1:]:
                kind, key, reason = line.split("\t", 2)
                unshareable.add((kind, key, reason))
        for p in glob.glob(os.path.join(v, "records", "*.json")):
            r = json.load(open(p))
            leaves = {}
            todo, seen = [r["root"]], set()
            while todo:
                e = todo.pop()
                if e in seen:
                    continue
                seen.add(e)
                b = bodies[e]
                for lid, canon in b["leaves"]:
                    leaves[json.dumps(lid, sort_keys=True)] = canon
                todo.extend(b["children"])
            out[(r["kind"], r["key_id"])] = (r["value_row"], leaves)
    return out, unshareable


def injected(leaves):
    return {k: v for k, v in leaves.items() if '"Injected"' in k}


def compare(base_name, base, other_name, other, expect_injected_diff=()):
    (ra, ua), (rb, ub) = base, other
    ka = set(ra) | {(k, i) for k, i, _ in ua}
    kb = set(rb) | {(k, i) for k, i, _ in ub}
    if ka != kb:
        only_a = sorted(ka - kb)[:5]
        only_b = sorted(kb - ka)[:5]
        fail(f"{base_name} vs {other_name}: covered keys differ: only {base_name} {only_a}, only {other_name} {only_b}")
    same = diff = 0
    by_kind = {}
    for k in sorted(set(ra) & set(rb)):
        ok = ra[k][0] == rb[k][0]
        s = by_kind.setdefault(k[0], [0, 0])
        s[0] += 1
        s[1] += ok
        if ok:
            same += 1
        else:
            diff += 1
    for kind, (n, ok) in sorted(by_kind.items()):
        if n != ok:
            fail(f"{base_name} vs {other_name}: {kind} value rows identical for {ok}/{n}")
    inj_a, inj_b = {}, {}
    for _, leaves in ra.values():
        inj_a.update(injected(leaves))
    for _, leaves in rb.values():
        inj_b.update(injected(leaves))
    inj_diff = sorted(k for k in set(inj_a) & set(inj_b) if inj_a[k] != inj_b[k])
    unexpected = [k for k in inj_diff if not any(e in k for e in expect_injected_diff)]
    if unexpected:
        fail(f"{base_name} vs {other_name}: injected digests differ: {unexpected}")
    note(
        f"{base_name} vs {other_name}: {len(ka)} vs {len(kb)} covered keys; records in both "
        f"{same + diff}, value rows same {same}; injected digests differing {inj_diff or 'none'}"
    )
    return by_kind


def shadow(g, cwd, sha, iso, store, label):
    settings = ["hydration.enable_paging=true", "frontier.mode=shadow"]
    g.buck2(cwd, iso, ["build", "//..."], None, settings, name=f"S-{label}-build")
    _, out = g.buck2(
        cwd,
        iso,
        ["debug", "hydration", "frontier-shadow", "--store", store, "--commit", sha],
        None,
        settings,
        name=f"S-{label}-shadow",
    )
    report = None
    for line in out.splitlines():
        if line.startswith("report: "):
            report = json.load(open(line[len("report: ") :]))
    if report is None:
        fail(f"shadow {label}: no report")
        return {}, out
    return {(e["kind"], e["key_id"]): e for e in report["entries"]}, out


def check_all_hit(entries, label):
    bad = {k: e["outcome"] for k, e in entries.items() if e["outcome"] != "hit"}
    if not entries:
        fail(f"{label}: no covered nodes")
    if bad:
        fail(f"{label}: {len(bad)} of {len(entries)} not hit: {list(bad.items())[:5]}")
    else:
        note(f"{label}: all {len(entries)} covered nodes hit")


def check_canary(entries, records, label, matches):
    """matches(leaf json) -> whether the leaf belongs to the mutation."""
    must = set()
    may = set()
    for k, (_, leaves) in records.items():
        if any(matches(l) for l in leaves):
            may.add(k)
    invalid = 0
    for k, e in entries.items():
        o = e["outcome"]
        if o == "valid_but_different":
            fail(f"{label}: valid but different: {k}")
        elif o == "hit":
            continue
        elif isinstance(o, dict) and ("invalid" in o or "unavailable" in o):
            invalid += 1
            leaf = json.dumps(e.get("leaf"), sort_keys=True)
            if k not in may:
                fail(f"{label}: {k} invalid, but its closure holds no leaf of the mutation")
            elif not matches(leaf):
                fail(f"{label}: {k} invalid on another leaf: {leaf}")
        else:
            fail(f"{label}: {k}: {o}")
    if invalid == 0:
        fail(f"{label}: nothing invalid; {len(may)} records hold a leaf of the mutation")
    note(f"{label}: {invalid} invalid, all on a leaf of the mutation; {len(may)} records hold one")
    return must


def leaf_match(kind, path=None, section=None, prop=None):
    def m(leaf_json):
        try:
            leaf = json.loads(leaf_json)
        except Exception:
            return False
        if not isinstance(leaf, dict) or kind not in leaf:
            return False
        v = leaf[kind]
        if path is not None and v.get("path") != path:
            return False
        if section is not None and section not in (v.get("section"), *(v.get("sections") or [])):
            return False
        if prop is not None and v.get("property") != prop:
            return False
        return True

    return m


def any_of(*ms):
    return lambda l: any(m(l) for m in ms)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--buck2", required=True)
    ap.add_argument("--fixture", required=True)
    ap.add_argument("--work", required=True)
    args = ap.parse_args()
    g = Gate(args)
    a, sha = g.checkout("a")
    c, sha_c = g.checkout("c")
    if sha != sha_c:
        fail("the two checkouts are not the same commit")
    main_iso = "fp-gate-main"

    # The mode off: no frontier command runs.
    rc, _ = g.buck2(a, "fp-gate-off", ["build", "//..."], None, ["hydration.enable_paging=true"], name="off-build")
    rc, _ = g.buck2(
        a,
        "fp-gate-off",
        ["debug", "hydration", "frontier-prime", "--store", os.path.join(g.w, "store-off"), "--commit", sha],
        None,
        ["hydration.enable_paging=true"],
        check=False,
        name="off-prime",
    )
    if rc == 0 or os.path.exists(os.path.join(g.w, "store-off")):
        fail("frontier-prime ran with frontier.mode off")
    g.kill(a, "fp-gate-off")
    if os.path.exists(os.path.join(a, "buck-out", "fp-gate-off")):
        shutil.move(os.path.join(a, "buck-out", "fp-gate-off"), os.path.join(g.w, "buck-out-off"))

    stores = {}
    stores["A"] = g.prime_arm("A", a, sha, main_iso)
    stores["A2"] = g.prime_arm("A2", a, sha, main_iso)
    stores["B"] = g.prime_arm("B", a, sha, "fp-gate-B")
    stores["C"] = g.prime_arm("C", c, sha, main_iso)
    env_d = dict(g.base_env)
    env_d.update(
        HOME=os.path.join(g.w, "home-d"),
        TMPDIR=os.path.join(g.w, "tmp-d"),
        LANG="C.UTF-8",
        FRONTIER_GATE_EXTRA="1",
    )
    for d in (env_d["HOME"], env_d["TMPDIR"]):
        os.makedirs(d, exist_ok=True)
    stores["D"] = g.prime_arm("D", a, sha, main_iso, env=env_d)
    stores["E"] = g.prime_arm("E", a, sha, main_iso, build_extra=("-j", "1"))
    env_f = dict(g.base_env)
    home_f = os.path.join(g.w, "home-f")
    os.makedirs(os.path.join(home_f, ".buckconfig.d"), exist_ok=True)
    with open(os.path.join(home_f, ".buckconfig.d", "sentinel"), "w") as f:
        f.write(f"[fixture]\n  home_secret = {SENTINEL_HOME}\n")
    env_f.update(HOME=home_f, FRONTIER_GATE_TOKEN=SENTINEL_ENV)
    with open(os.path.join(a, ".buckconfig.local"), "w") as f:
        f.write(f"[fixture]\n  greeting = {SENTINEL_LOCAL}\n")
    stores["F"] = g.prime_arm("F", a, sha, main_iso, env=env_f, config=(f"fixture.arg_secret={SENTINEL_ARG}",))
    os.remove(os.path.join(a, ".buckconfig.local"))

    loaded = {k: load_store(v) for k, v in stores.items()}
    if not loaded["A"][0]:
        fail("arm A wrote no records")
    by_kind = compare("A", loaded["A"], "A2", loaded["A2"])
    # Each arm's own difference may move only the injected leaves it names.
    compare("A", loaded["A"], "B", loaded["B"], expect_injected_diff=("BuildDataKey",))
    compare("A", loaded["A"], "C", loaded["C"])
    compare("A", loaded["A"], "D", loaded["D"])
    compare("A", loaded["A"], "E", loaded["E"])
    note("records by kind in A: " + ", ".join(f"{k} {n}" for k, (n, _) in sorted(by_kind.items())))

    # 6: no sentinel reaches arm F's store, by bytes or digest.
    needles = []
    for s in (SENTINEL_LOCAL, SENTINEL_HOME, SENTINEL_ARG, SENTINEL_ENV):
        needles += [s.encode(), hashlib.sha256(s.encode()).hexdigest().encode()]
        try:
            b3 = subprocess.run(["b3sum", "--no-names"], input=s.encode(), capture_output=True, check=True)
            needles.append(b3.stdout.strip())
        except Exception:
            pass
    leaked = []
    for root, _, files in os.walk(stores["F"]):
        for fn in files:
            data = open(os.path.join(root, fn), "rb").read()
            for n in needles:
                if n in data:
                    leaked.append((fn, n[:24]))
    if leaked:
        fail(f"sentinels in arm F's store: {leaked[:5]}")
    unsh_f = {r for _, _, r in loaded["F"][1]}
    if not any("external config premise: fixture.greeting" in r for r in unsh_f):
        fail("arm F: the record reading the .buckconfig.local greeting was not unshareable")
    note(f"arm F: no sentinel in the store ({len(needles)} needles); unshareable reasons {sorted(unsh_f)}")

    # Shadow: a fresh daemon on A's checkout against A's store.
    s_iso = "fp-gate-shadow"
    records_a = loaded["A"][0]
    entries, out = shadow(g, a, sha, s_iso, stores["A"], "clean")
    note(out.rstrip())
    check_all_hit(entries, "shadow clean")

    def mutate(label, do, undo, matches):
        do()
        entries, out = shadow(g, a, sha, s_iso, stores["A"], label)
        check_canary(entries, records_a, f"canary {label}", matches)
        undo()
        entries, _ = shadow(g, a, sha, s_iso, stores["A"], label + "-revert")
        check_all_hit(entries, f"revert {label}")

    p = lambda *x: os.path.join(a, *x)

    def write(path, text):
        with open(path, "w") as f:
            f.write(text)

    orig = {n: open(p(n)).read() for n in ("srcs/a.txt", "ignored.txt", ".buckconfig")}
    mutate(
        "content",
        lambda: write(p("srcs/a.txt"), "a changed\n"),
        lambda: write(p("srcs/a.txt"), orig["srcs/a.txt"]),
        leaf_match("Path", path="srcs/a.txt"),
    )
    mutate(
        "chmod",
        lambda: os.chmod(p("srcs/b.txt"), 0o755),
        lambda: os.chmod(p("srcs/b.txt"), 0o644),
        leaf_match("Path", path="srcs/b.txt"),
    )

    def to_symlink():
        shutil.copy(p("data.txt"), p("data-copy.txt"))
        os.remove(p("data.txt"))
        os.symlink("data-copy.txt", p("data.txt"))

    def from_symlink():
        os.remove(p("data.txt"))
        shutil.move(p("data-copy.txt"), p("data.txt"))

    mutate(
        "symlink",
        to_symlink,
        from_symlink,
        any_of(leaf_match("Path", path="data.txt"), leaf_match("Listing", path=""), leaf_match("Path", path="data-copy.txt")),
    )
    mutate(
        "untracked",
        lambda: write(p("srcs/untracked.txt"), "new\n"),
        lambda: os.remove(p("srcs/untracked.txt")),
        any_of(leaf_match("Listing", path="srcs"), leaf_match("Path", path="srcs/untracked.txt")),
    )
    mutate(
        "ignored",
        lambda: write(p("ignored.txt"), "edited\n"),
        lambda: write(p("ignored.txt"), orig["ignored.txt"]),
        leaf_match("Path", path="ignored.txt"),
    )
    mutate(
        "buckconfig",
        lambda: write(p(".buckconfig"), orig[".buckconfig"].replace("greeting = hello", "greeting = changed")),
        lambda: write(p(".buckconfig"), orig[".buckconfig"]),
        any_of(leaf_match("Config", section="fixture", prop="greeting"), leaf_match("Path", path=".buckconfig")),
    )
    mutate(
        "cell_aliases",
        lambda: write(p(".buckconfig"), orig[".buckconfig"].replace("  fx = root\n", "  fx = root\n  fx2 = root\n")),
        lambda: write(p(".buckconfig"), orig[".buckconfig"]),
        any_of(leaf_match("Sections", section="cell_aliases"), leaf_match("Path", path=".buckconfig")),
    )
    mutate(
        "buckconfig_local",
        lambda: write(p(".buckconfig.local"), "[fixture]\n  greeting = local\n"),
        lambda: os.remove(p(".buckconfig.local")),
        any_of(leaf_match("Config", section="fixture", prop="greeting"), leaf_match("Path", path=".buckconfig.local")),
    )
    g.kill(a, s_iso)

    note("")
    if FAILURES:
        note(f"GATE FAILED: {len(FAILURES)} failure(s)")
        for f in FAILURES:
            note("  - " + f)
        sys.exit(1)
    note("GATE PASSED")


if __name__ == "__main__":
    main()
