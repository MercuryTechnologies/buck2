{ lib,
  darwin,
  stdenv,
  makeRustPlatform,
  openssl,
  sqlite,
  installShellFiles,
  protobuf,
  pkg-config,
  rust-bin,
}:

let
  # based on Buck2's `rust-toolchain` file.
  rust-nightly = rust-bin.nightly."2024-10-13".default.override {
    extensions = [ "rust-analyzer" "rust-src" ];
  };
  rustPlatform = makeRustPlatform {
    cargo = rust-nightly;
    rustc = rust-nightly;
  };
in

rustPlatform.buildRustPackage rec {
  pname = "buck2";
  version = "2025-02-15-mercury001";

  src = ../.;

  cargoLock = {
    lockFile = ./Cargo.lock;
    allowBuiltinFetchGit = true;
  };

  postPatch = ''
    ln -s ${./Cargo.lock} Cargo.lock
  '';

  nativeBuildInputs = [
    installShellFiles
    protobuf
    pkg-config
  ];
  buildInputs =
    [
      openssl
      sqlite
    ]
    ++ lib.optionals stdenv.isDarwin [
      darwin.apple_sdk.frameworks.CoreFoundation
      darwin.apple_sdk.frameworks.CoreServices
      darwin.apple_sdk.frameworks.IOKit
      darwin.apple_sdk.frameworks.Security
    ];

  BUCK2_BUILD_PROTOC = "${protobuf}/bin/protoc";
  BUCK2_BUILD_PROTOC_INCLUDE = "${protobuf}/include";
  doCheck = false;
  dontStrip = true; # XXX (aseipp): cargo will delete dwarf info but leave symbols for backtraces

  postInstall = ''
    mv $out/bin/buck2     $out/bin/buck
    ln -sfv $out/bin/buck $out/bin/buck2
    mv $out/bin/starlark  $out/bin/buck2-starlark
    mv $out/bin/read_dump $out/bin/buck2-read_dump

    installShellCompletion --cmd buck2 \
      --bash <( $out/bin/buck2 completion bash ) \
      --fish <( $out/bin/buck2 completion fish ) \
      --zsh <( $out/bin/buck2 completion zsh )
  '';
}
