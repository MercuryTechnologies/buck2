#!/bin/sh
# Lay out the gnu link sysroot for lab's c1 execution platform from four nix
# store paths, and check it against the committed manifest.
#
#   gnu_exec_sysroot.sh <out> <manifest>|--print <glibc> <gcc> <libgcc> <zlib>
#
# <out>/sysroot is a sysroot clang's Linux driver searches: the C runtime
# objects, glibc's link libraries and linker scripts in usr/lib, and gcc's
# crtbegin/crtend and libgcc in usr/lib/gcc/<triple>/<version>, where clang
# looks for a GCC installation. <out>/rustc-libs holds libz.so.1 for the gnu
# host rustc. With --print, the manifest of what was laid out goes to
# stdout instead of being checked.
#
# It runs in lab's thin worker image, so it uses only the image's /bin and
# the executor's nix daemon.
set -eu

out=$1
manifest=$2
glibc=$3
gcc=$4
libgcc=$5
zlib=$6
triple=x86_64-unknown-linux-gnu
gcc_version=14.3.0

# Every exec-side program this sysroot links runs under the image's loader,
# so link-time glibc must be the loader's glibc. The image digest is part of
# the action key, so an image bump runs this check again.
loader=${GNU_EXEC_SYSROOT_LOADER:-/lib64/ld-linux-x86-64.so.2}
if ! "$loader" --help 2>/dev/null | grep -qF "  $glibc/lib (system search path)"; then
    echo "gnu_exec_sysroot: $loader does not run $glibc; the image and these store paths moved apart. Bump the paths in lab_buildbuddy/BUCK with the image digest of lab_buildbuddy//:usw2_c1." >&2
    exit 1
fi

# lab's NIX_SUBSTITUTE_FN (infra/toolchains/nix/nix_build.bzl): an empty
# client configuration, so the daemon substitutes from its own substituters,
# checked against its own keys.
socket=/nix/var/nix/daemon-socket/socket
if [ -e "$socket" ]; then
    conf=$(mktemp -d)
    NIX_CONF_DIR=$conf NIX_USER_CONF_FILES=$conf/nix.conf NIX_REMOTE=daemon \
        nix-store --realise "$glibc" "$gcc" "$libgcc" "$zlib" >/dev/null
    rmdir "$conf"
else
    for p in "$glibc" "$gcc" "$libgcc" "$zlib"; do
        if [ ! -d "$p" ]; then
            echo "gnu_exec_sysroot: no nix daemon at $socket and no $p; this action needs an execution platform with lab_buildbuddy//:nix_daemon" >&2
            exit 1
        fi
    done
fi

rm -rf "$out"
lib=$out/sysroot/usr/lib
gcc_dir=$lib/gcc/$triple/$gcc_version
mkdir -p "$gcc_dir" "$out/rustc-libs"

for f in Scrt1.o crti.o crtn.o libdl.so libpthread.so librt.so libutil.so; do
    cp -L "$glibc/lib/$f" "$lib/"
done
for f in crtbegin.o crtbeginS.o crtend.o crtendS.o libgcc.a libgcc_eh.a; do
    cp -L "$gcc/lib/gcc/$triple/$gcc_version/$f" "$gcc_dir/"
done
cp -L "$libgcc/lib/libgcc_s.so.1" "$lib/"

# glibc's libc.so and libm.so are linker scripts that name their libraries by
# store path. Copy each file a script names, and rewrite the name to a bare
# one, which lld looks up on the -L path, where the sysroot's usr/lib is.
for script in "$glibc/lib/libc.so" "$glibc/lib/libm.so" "$libgcc/lib/libgcc_s.so"; do
    for f in $(grep -o '/nix/store/[^ )]*' "$script"); do
        cp -L "$f" "$lib/"
    done
    sed 's|/nix/store/[^ )]*/||g' "$script" >"$lib/${script##*/}"
    if grep -q /nix/store "$lib/${script##*/}"; then
        echo "gnu_exec_sysroot: ${script##*/} still names /nix/store after the rewrite" >&2
        exit 1
    fi
done

cp -L "$zlib/lib/libz.so.1" "$out/rustc-libs/"

chmod -R u=rwX,go=rX "$out"

list() {
    (
        cd "$out"
        for f in sysroot/usr/lib/* sysroot/usr/lib/gcc/$triple/$gcc_version/* rustc-libs/*; do
            if [ -f "$f" ]; then
                sha256sum "$f"
            fi
        done
    ) | LC_ALL=C sort -k 2
}

if [ "$manifest" = --print ]; then
    list
    exit 0
fi
actual=$(list)
expected=$(cat "$manifest")
if [ "$actual" != "$expected" ]; then
    echo "gnu_exec_sysroot: the sysroot differs from $manifest. Laid out:" >&2
    echo "$actual" >&2
    exit 1
fi
