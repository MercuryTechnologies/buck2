#!/bin/sh
# Lay out the musl headers for one architecture as a sysroot, the way
# `make install-headers` in musl's Makefile does, without make: copy
# include/, then arch/generic/bits/, then arch/<arch>/bits/ over it, and
# generate bits/alltypes.h and bits/syscall.h with the same sed rules.
# Only headers: the C runtime objects and libc.a come from the Rust
# toolchain's self-contained musl target.
#
#   musl_sysroot.sh <musl source dir> <arch> <out dir>
set -eu

src="$1"
arch="$2"
out="$3"
inc="$out/usr/include"

mkdir -p "$inc/bits"
cp -R "$src/include/." "$inc/"
rm -f "$inc/alltypes.h.in"
cp "$src"/arch/generic/bits/*.h "$inc/bits/"
cp "$src/arch/$arch"/bits/*.h "$inc/bits/"
sed -f "$src/tools/mkalltypes.sed" \
    "$src/arch/$arch/bits/alltypes.h.in" \
    "$src/include/alltypes.h.in" >"$inc/bits/alltypes.h"
cp "$src/arch/$arch/bits/syscall.h.in" "$inc/bits/syscall.h"
sed -n -e s/__NR_/SYS_/p <"$src/arch/$arch/bits/syscall.h.in" >>"$inc/bits/syscall.h"
