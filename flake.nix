{
  description = "Fast directory scanning utility with beautiful Python analysis tools";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Build the Zig core binary
        wtfs-core = pkgs.stdenv.mkDerivation {
          pname = "wtfs-core";
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
            description = "Efficient bulk retrieval of file attributes (core Zig binary)";
            homepage = "https://github.com/mbrock/wtfs";
            license = licenses.mit;
            maintainers = [ ];
            platforms = platforms.unix;
            mainProgram = "wtfs";
          };
        };

        # Build tagflow (not yet in nixpkgs)
        tagflow = pkgs.python3Packages.buildPythonPackage rec {
          pname = "tagflow";
          version = "0.12.0";
          format = "pyproject";

          src = pkgs.fetchPypi {
            inherit pname version;
            hash = "sha256-+Ooje+SUwKG55A44AdY5wjQo3ly/mAQxYHiHo065J5A=";
          };

          nativeBuildInputs = with pkgs.python3Packages; [
            hatchling
          ];

          propagatedBuildInputs = with pkgs.python3Packages; [
            fastapi
            pydantic
            beautifulsoup4
            anyio
            hypercorn
            rich
            trio
          ];

          # Disable tests if they require network or special setup
          doCheck = false;

          meta = with pkgs.lib; {
            description = "Block-oriented HTML generation for Python";
            homepage = "https://github.com/lessrest/tagflow";
            license = licenses.mit;
          };
        };

        # Build the Python package with bundled Zig binaries
        wtfs-python = pkgs.python3Packages.buildPythonApplication {
          pname = "wtfs";
          version = "0.1.0";

          src = ./.;

          format = "pyproject";

          nativeBuildInputs = [
            pkgs.python3Packages.hatchling
            pkgs.zig
          ];

          propagatedBuildInputs = with pkgs.python3Packages; [
            rich
            textual
            tagflow
            fastapi
            uvicorn
            pydantic
            starlette
          ];

          # Build the Zig binaries before building the Python package
          preBuild = ''
            export HOME=$TMPDIR
            # Build all platform binaries using the python step
            # This cross-compiles for: linux-{x86_64,aarch64}, macos-{x86_64,aarch64}
            zig build python -Doptimize=ReleaseFast
          '';

          meta = with pkgs.lib; {
            description = "Fast directory scanning utility with beautiful Python analysis tools";
            homepage = "https://github.com/mbrock/wtfs";
            license = licenses.mit;
            maintainers = [ ];
            platforms = platforms.unix;
            mainProgram = "wtfs";
          };
        };
      in
      {
        packages = {
          # Python tool as the default (more useful)
          default = wtfs-python;

          # Also provide the core Zig binary separately
          wtfs-core = wtfs-core;

          # Aliases for clarity
          wtfs = wtfs-python;
          wtfs-python = wtfs-python;
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.zig
            pkgs.python3
            pkgs.python3Packages.rich
            pkgs.python3Packages.textual
            pkgs.python3Packages.fastapi
            pkgs.python3Packages.uvicorn
            pkgs.python3Packages.hatchling
            pkgs.python3Packages.pip
            pkgs.uv
          ];

          shellHook = ''
            echo "wtfs development environment"
            echo "Zig version: $(zig version)"
            echo "Python version: $(python --version)"
            echo ""
            echo "Available commands:"
            echo "  - Build Zig core: zig build"
            echo "  - Run Python tool: python -m wtfs.cli"
            echo "  - Run TUI: python -m wtfs.tui"
            echo "  - Run web UI: python -m wtfs.cli --webui"
            echo ""
            echo "Note: Install additional Python deps with: uv sync"
          '';
        };

        apps = {
          # Default app runs the Python tool
          default = {
            type = "app";
            program = "${wtfs-python}/bin/wtfs";
          };

          # Also provide the core binary as an app
          wtfs-core = {
            type = "app";
            program = "${wtfs-core}/bin/wtfs";
          };

          # Additional Python tools
          wtfs-tui = {
            type = "app";
            program = "${wtfs-python}/bin/wtfs-tui";
          };

          wtfsdump = {
            type = "app";
            program = "${wtfs-python}/bin/wtfsdump";
          };
        };
      }
    );
}
