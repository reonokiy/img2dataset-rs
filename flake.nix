{
  description = "img2dataset-rs Rust package flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustToolchain = pkgs.rust-bin.stable.latest.default;
        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        version = cargoToml.package.version;
      in
      {
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "img2dataset-rs";
          version = version;
          src = ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
          };
          nativeBuildInputs = [ ];
          buildInputs = [
          ];
          RUSTC_BOOTSTRAP = 1; # 2024 edition workaround, remove if not needed
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [
            rustToolchain
          ];
        };
      }
    );
}
