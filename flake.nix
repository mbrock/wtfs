{
  description = "Zig bindings for getattrlistbulk syscall on macOS";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages = {
          default = pkgs.stdenv.mkDerivation {
            pname = "wtfs";
            version = "0.6.0";

            src = ./.;

            nativeBuildInputs = [ pkgs.zig ];

            buildPhase = ''
              runHook preBuild

              export HOME=$TMPDIR
              zig build -Doptimize=ReleaseSafe -Dcpu=baseline --prefix $out

              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall

              # The zig build --prefix already installs to $out
              # but we need to ensure the install phase completes

              runHook postInstall
            '';

            meta = with pkgs.lib; {
              description = "Efficient bulk retrieval of file attributes using getattrlistbulk";
              homepage = "https://github.com/mbrock/wtfs";
              license = licenses.mit;
              maintainers = [ ];
              platforms = platforms.unix;
              mainProgram = "wtfs";
            };
          };
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.zig
          ];

          shellHook = ''
            echo "wtfs development environment"
            echo "Zig version: $(zig version)"
          '';
        };

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/wtfs";
        };
      }
    );
}
