#!/bin/sh
# Regenerate the jemalloc headers that configure writes, one set per static
# musl target the release builds. tikv-jemalloc-sys's build script runs
# jemalloc's configure and make on the build machine; under buck the script
# does not run (buildscript.run = false in fixups.toml) and the cxx_library
# there compiles the C sources against these headers instead, so they are
# produced here, once per target, and checked in.
#
#   shim/third-party/rust/fixups/tikv-jemalloc-sys/regen.sh
#
# Every input is a buck2 target built through .buckconfig.namespace, so this
# needs the same remote execution credentials as any build with that config
# and materialises the Rust toolchain of each target locally. BUCK2 names the
# buck2 to run (default: buck2 on PATH); make, sed and sh must exist here.
#
# Only jemalloc_internal_defs.h depends on the target (page size, spin-wait
# instruction); the script checks that the other three headers came out the
# same for both targets before it writes the shared copy.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../../.." && pwd)
crate="tikv-jemalloc-sys-0.6.1+5.3.0-1-ge13ca993e8ccb9ba9847cc330696e02839f328f7"
# The build script passes the part after `+` as --with-version.
je_version="${crate#*+}"
BUCK2="${BUCK2:-buck2}"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

output_of() {
    # $1: file written by --show-full-output, $2: target name
    awk -v t="$2" 'index($1, t) == length($1) - length(t) + 1 { print $2 }' "$1"
}

for arch in x86_64 aarch64; do
    triple="$arch-unknown-linux-musl"
    case $arch in
        # 4 KiB pages. 16 on aarch64 so one binary serves the 4 KiB and
        # 64 KiB page kernels both, as distributions build it; jemalloc
        # aborts when the compiled-in page size is smaller than the
        # kernel's.
        x86_64) lg_page=12 ;;
        aarch64) lg_page=16 ;;
    esac

    outputs="$work/outputs-$arch"
    (cd "$root" && $BUCK2 build --config-file .buckconfig.namespace \
        --target-platforms "namespace//:linux-$arch-musl" \
        --show-full-output \
        toolchains//:llvm_dist \
        toolchains//:rust \
        "toolchains//:musl-sysroot-$arch" \
        "shim//third-party/rust:$crate.crate" >"$outputs")
    llvm=$(output_of "$outputs" ":llvm_dist")
    rust=$(output_of "$outputs" ":rust")
    sysroot=$(output_of "$outputs" ":musl-sysroot-$arch")
    cratedir=$(output_of "$outputs" ":$crate.crate")

    rustlib="$rust/lib/rustlib/$triple/lib"
    self_contained="$rustlib/self-contained"
    cc="$llvm/bin/clang -target $triple --sysroot=$sysroot"

    # configure links test programs. The sysroot has headers only, so the
    # C runtime and libc.a are the Rust toolchain's self-contained musl
    # objects, the way rustc links the release; compiler_builtins stands in
    # for libgcc (aarch64 long double arithmetic) and references
    # rust_eh_personality, hence the stub. musl ships libm, libpthread,
    # libdl and librt as empty archives, which the Rust toolchain omits.
    libs="$work/libs-$arch"
    mkdir -p "$libs"
    for lib in m pthread dl rt; do
        "$llvm/bin/llvm-ar" rcs "$libs/lib$lib.a"
    done
    printf 'void rust_eh_personality(void) {}\n' >"$libs/eh.c"
    $cc -c "$libs/eh.c" -o "$libs/eh.o"

    build="$work/build-$arch"
    mkdir -p "$build"
    cp -R "$cratedir/jemalloc/." "$build/"
    cp "$cratedir/configure/configure" "$build/configure"
    (
        cd "$build"
        CC="$cc" \
        LDFLAGS="-fuse-ld=lld -static -nostdlib -nostartfiles $self_contained/crt1.o $self_contained/crti.o $self_contained/crtn.o -L$self_contained -L$libs" \
        LIBS="-lc $(ls "$rustlib"/libcompiler_builtins-*.rlib) $libs/eh.o" \
        NM="$llvm/bin/llvm-nm" \
        AR="$llvm/bin/llvm-ar" \
        RANLIB="$llvm/bin/llvm-ranlib" \
        sh ./configure \
            --host="$triple" \
            --build=x86_64-unknown-linux-gnu \
            --with-version="$je_version" \
            --disable-cxx \
            --enable-doc=no \
            --enable-shared=no \
            --enable-stats \
            --with-lg-page="$lg_page" \
            --with-lg-hugepage=21 \
            --with-lg-vaddr=48 \
            --with-private-namespace=_rjem_ >configure.out
        # private_namespace.h is generated from the symbols of every
        # compiled object, so this compiles the library once.
        make include/jemalloc/internal/private_namespace.h >make.out
    )

    mkdir -p "$here/include-$arch/jemalloc/internal" "$work/shared-$arch/jemalloc/internal"
    cp "$build/include/jemalloc/internal/jemalloc_internal_defs.h" "$here/include-$arch/jemalloc/internal/"
    cp "$build/include/jemalloc/jemalloc.h" "$work/shared-$arch/jemalloc/"
    cp "$build/include/jemalloc/internal/jemalloc_preamble.h" \
        "$build/include/jemalloc/internal/private_namespace.h" \
        "$work/shared-$arch/jemalloc/internal/"
done

diff -r "$work/shared-x86_64" "$work/shared-aarch64"
rm -rf "$here/include"
cp -R "$work/shared-x86_64" "$here/include"
