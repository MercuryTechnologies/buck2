# buck2_launcher

A `buck2` that picks, per worktree, which buck2 binary runs (a channel) and
which `.buckconfig.d` file is in force (a config), together called a profile.
A project lists its channels and profiles in a manifest and points its
`bin/buck2` at `buck2-launcher`. With nothing selected the project's default
profile runs and nothing extra is printed.

The library knows no project's names; `src/bin/buck2_launcher.rs` reads the
manifest at `BUCK2_PROFILES_MANIFEST`, relative to the project root, or
`tools/buck2/profiles.toml`.

## The manifest

```toml
default = "current"

[credentials]
file = "~/.buckconfig.d/mercury-credentials"   # only its presence is read
login_url = "https://<buildbuddy app>"          # serves /cli-login
org = "<org slug>"
template = "[credentials]\nbuildbuddy_api_key = {key}\n"

[channels.mercury-head]
exec = "nix/bin/buck2-current"   # a dotslash file or any executable
from = "mercury-head"

[channels.mercury-head-next]
exec = "nix/bin/buck2-next"
from = "mercury-head-next"

[profiles.current]
channel = "mercury-head"
summary = "builds local, cache read-only"

[profiles.next]
channel = "mercury-head-next"
config = "tools/buck2/buckconfig.d/next"   # copied into .buckconfig.d
summary = "tests on hub RE"
needs_key = true

[profiles.next.warn_on_os]
macos = "shell tests can run on hub; specs cannot (their binary is built here)"
```

Optional keys: `state_file` (`.buck2-profile`), `config_dest`
(`.buckconfig.d/50-local-profile`), `name` (`buck2`, the prefix of every line).

## Rules it keeps

- Only flags before buck2's subcommand are its own (`--profile`, `--channel`,
  `--login`, `--logout`); buck2's flags there (`--isolation-dir`, `-v`, …)
  and everything from the subcommand on reach buck2 unchanged.
- It writes one config file and one state file, never `.buckconfig.local`,
  never a checked-in file, and never a config file it did not write (each
  starts with `# written by buck2 --profile:`).
- A switch, or a call whose binary or config differs from the one the
  worktree's daemon was started under, stops that daemon with the binary that
  started it.
- A key file is only ever tested for presence; no output contains any part of
  it.
- `BUCK2_ROLLOUT_BIN` (CI) chooses the binary when set, and says so.
- A channel whose program is the launcher itself is refused.

## Commands

    buck2 --profile                 show the profile in force
    buck2 --profile next            switch this worktree to next
    buck2 --profile next build //x  switch, then run the command
    buck2 --profile next --channel <commit>   one half overridden
    buck2 --profile current         back to zero config
    buck2 --login | --logout        BuildBuddy key for this machine

Your own `.buckconfig.local` still wins over any profile.

## What it prints

A fresh clone, zero config: nothing extra.

    $ buck2 build //app:mwb
    (buck2's own output only)

Showing the profile:

    $ buck2 --profile
    buck2: profile current (default)
      binary  mercury-head 9a95ed57f0 (bin/buck2's pin)
      config  none written; checked-in .buckconfig.d only
              builds local, cache read-only
      key     absent (`buck2 --login` adds one)
      daemon  running, same binary

Switching to next on a machine with no key yet (login is opt-out):

    $ buck2 --profile next
    buck2: profile next needs a BuildBuddy key; none on
      this machine.
      sign in: https://<hub>/cli-login?…   (opening browser)
      or paste a key, or Ctrl-C and set MWB_BUCK2_LOGIN=never
    buck2: key saved to ~/.buckconfig.d/mercury-credentials
    buck2: profile next (was current)
      binary  mercury-head-next 2b5f3188d9
      config  wrote .buckconfig.d/50-local-profile
                from tools/buck2/buckconfig.d/next
              tests run on hub RE; builds stay local
      key     present
      daemon  stopped (was 9a95ed57f0); restarts on next call

Any later call under next, one line on stderr:

    $ buck2 test //src/Mercury/Foo:spec
    buck2: next · buck2 2b5f3188d9 · tests on hub RE
    (buck2's own output)

Switching to experimental, key present:

    $ buck2 --profile experimental
    buck2: profile experimental (was next)
      binary  mwb-carry/resilience-v7 48cf950ae3
      config  wrote .buckconfig.d/50-local-profile
                from tools/buck2/buckconfig.d/experimental
              tests and builds run on hub RE
      key     present
      daemon  stopped (was 2b5f3188d9); restarts on next call

    $ buck2 build //app:mwb
    buck2: experimental · buck2 48cf950ae3 · tests and
      builds on hub RE

A channel overridden with a commit:

    $ buck2 --profile next --channel 7288c87a83
    buck2: profile next* (channel 7288c87a83, CI-built)
      ...
    $ buck2 test //…
    buck2: next* · buck2 7288c87a83 · tests on hub RE

A commit Mercury's cache does not hold:

    $ buck2 --profile next --channel 0123abcd
    buck2: channel 0123abcd is not built; nothing changed.
      build it: gh workflow run buck2-channel.yml -f rev=0123abcd

A Mac with a profile that builds on hub:

    $ buck2 --profile experimental
    buck2: warning: experimental on macOS: shell tests can
      run on hub; specs cannot (their binary is built here).
    buck2: profile experimental (was current)
      ...

No terminal (agents, CI) and no key:

    $ buck2 --profile next
    buck2: profile next needs a BuildBuddy key; none on
      this machine, and no terminal to sign in from.
      Run `buck2 --login` in a terminal. Nothing changed.

Back to zero config:

    $ buck2 --profile current
    buck2: profile current (was experimental)
      binary  mercury-head 9a95ed57f0
      config  removed .buckconfig.d/50-local-profile
      daemon  stopped (was 48cf950ae3); restarts on next call

## Files

| File | Holds |
|---|---|
| `.buckconfig.d/50-local-profile` | the profile's config, copied |
| `.buck2-profile` | the profile and any override |
| `~/.buckconfig.d/mercury-credentials` | the key, mode 600 |

The key is never printed, on error paths included; `--profile` says
only whether one is present.

## Login

`buck2 --login` uses BuildBuddy's CLI login: it listens on 127.0.0.1, opens
`<login_url>/cli-login?cli_url=http://localhost:<port>&workspace=…&org=…`,
and the page mints a personal API key and redirects to the listener with it.
A key pasted on stdin is taken too, for a browser on another machine. The key
is written into `template` at `file`, mode 600 (its directory 700), through a
rename, and never printed. Switching to a profile with `needs_key` and no key
offers the login when stdin and stderr are a terminal, unless
`BUCK2_LAUNCHER_LOGIN=never`; otherwise it refuses and changes nothing.

Personal keys need the server flag `app.user_owned_keys_enabled` and the
org's own toggle; a personal key can carry at most cache and CAS writes, and
an authenticated key may execute.

## Not built yet

Checking a new key against the server (buck2's first remote call rejects a
bad one), channels from any commit, and the published dotslash file. A
commit channel prints a message and changes nothing.
