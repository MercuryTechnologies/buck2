#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

"""Takes apart a suspend bundle (format 2) for e2e-portable.sh, without the checks of
`buck2 resume --import`, so the tests can hand the daemon an image no import has checked.

  unpack BUNDLE DIR        write every member under DIR; nothing is verified
  pad BUNDLE OUT BYTES     copy BUNDLE to OUT with BYTES of random bytes right after the
                           MANIFEST's records: an import that reads past the MANIFEST fails on them

Each frame is decoded by the zstd command, so the script needs nothing beyond the standard
library. Run it with `python3 -I`.
"""

import json
import os
import struct
import subprocess
import sys

PROLOGUE = b"BUCK2SUSBUNDLE" + struct.pack("<H", 2)
HEAD, DATA, END = 1, 2, 3


def records(f):
    """Yields (offset, kind, member, chunk, raw_len, frame) for every record."""
    if f.read(len(PROLOGUE)) != PROLOGUE:
        sys.exit("not a format 2 suspend bundle")
    while True:
        at = f.tell()
        head = f.read(24)
        if not head:
            return
        if len(head) != 24:
            sys.exit("the bundle ends inside a record head")
        kind, member, chunk, raw_len, zlen = struct.unpack("<B3xIQII", head)
        frame = f.read(zlen)
        if len(frame) != zlen:
            sys.exit("the bundle ends inside a record")
        yield at, kind, member, chunk, raw_len, frame


def decode(frame):
    return subprocess.run(
        ["zstd", "-dcq"], input=frame, stdout=subprocess.PIPE, check=True
    ).stdout


def unpack(bundle, out):
    header = None
    with open(bundle, "rb") as f:
        for _, kind, member, chunk, _, frame in records(f):
            if kind == HEAD:
                header = json.loads(decode(frame))
                for m in header["members"]:
                    path = os.path.join(out, m["path"])
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "wb") as w:
                        w.truncate(m["len"])
            elif kind == DATA:
                m = header["members"][member]
                with open(os.path.join(out, m["path"]), "r+b") as w:
                    w.seek(chunk * header["chunk_size"])
                    w.write(decode(frame))


def pad(bundle, out, nbytes):
    with open(bundle, "rb") as f:
        rest_at = None
        manifest = None
        for at, kind, member, _, _, frame in records(f):
            if kind == HEAD:
                paths = [m["path"] for m in json.loads(decode(frame))["members"]]
                manifest = paths.index("suspend/MANIFEST")
            elif kind == DATA and member != manifest:
                rest_at = at
                break
        if rest_at is None:
            sys.exit("no record follows the MANIFEST's")
        f.seek(0)
        with open(out, "wb") as w:
            w.write(f.read(rest_at))
            left = nbytes
            while left > 0:
                n = min(left, 64 << 20)
                w.write(os.urandom(n))
                left -= n
            while block := f.read(64 << 20):
                w.write(block)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "unpack" and len(sys.argv) == 4:
        unpack(sys.argv[2], sys.argv[3])
    elif cmd == "pad" and len(sys.argv) == 5:
        pad(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    else:
        sys.exit(__doc__)
