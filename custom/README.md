Custom build script
===================

This directory is intentionally isolated from buck2 main source.
Especially, the buck2 upstream does not allow to put the Cargo.lock file onto a top-level location.
So we put all the necessary additional files to this "custom" directory for enabling `nix build .#buck2`.
