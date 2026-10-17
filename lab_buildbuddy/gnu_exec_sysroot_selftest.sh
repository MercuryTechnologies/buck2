#!/bin/sh
# Link and run a C program against the gnu exec sysroot, the way the cxx
# toolchain links build scripts and proc macros, and check that the linker
# read nothing from outside the sysroot, the LLVM tree and this action.
#
#   gnu_exec_sysroot_selftest.sh <llvm> <sysroot> <out>
#
# <out> gets the linker's --trace, the list of every file it opened.
set -eu

llvm=$(realpath "$1")
sysroot=$(realpath "$2")
out=$3
work=$(mktemp -d)

# No C headers in the sysroot (no exec-side C compile needs them), so declare
# puts rather than include stdio.h.
printf 'int puts(const char *);\nint main(void) { return puts("ok") < 0; }\n' >"$work/m.c"
"$llvm/bin/clang" -target x86_64-unknown-linux-gnu "--sysroot=$sysroot" -c "$work/m.c" -o "$work/m.o"
# rustc's link line for a gnu target: -nodefaultlibs and these libraries.
"$llvm/bin/clang++" -target x86_64-unknown-linux-gnu "--sysroot=$sysroot" -fuse-ld=lld \
    -nodefaultlibs -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc \
    -Wl,--trace "$work/m.o" -o "$work/m" >"$work/trace"
cp "$work/trace" "$out"

n=0
while IFS= read -r f; do
    n=$((n + 1))
    case $(realpath "$f") in
    "$sysroot"/* | "$llvm"/* | "$work"/*) ;;
    *)
        echo "gnu_exec_sysroot_selftest: the link read $f, outside the sysroot" >&2
        exit 1
        ;;
    esac
done <"$work/trace"
if [ "$n" -eq 0 ]; then
    echo "gnu_exec_sysroot_selftest: the linker traced no files" >&2
    exit 1
fi
[ "$("$work/m")" = ok ]
rm -rf "$work"
