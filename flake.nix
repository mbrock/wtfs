{
  description = "wtfs: find out where disk space went; scans a tree into a Parquet/Arrow table";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      version = builtins.head (
        builtins.match ''.*[.]version = "([^"]+)".*'' (builtins.readFile ./build.zig.zon)
      );

      src = nixpkgs.lib.fileset.toSource {
        root = ./.;
        fileset = nixpkgs.lib.fileset.unions [
          ./build.zig
          ./build.zig.zon
          ./src
        ];
      };

      wtfsFor =
        pkgs:
        {
          static ? false,
        }:
        pkgs.stdenv.mkDerivation {
          pname = if static then "wtfs-static" else "wtfs";
          inherit version src;
          nativeBuildInputs = [ pkgs.zig_0_17.hook ];
          # The zig hook builds with --release=safe -Dcpu=baseline. The Linux
          # build makes raw syscalls and links no libc, so it is static
          # either way; `static` asserts that.
          doCheck = true;
          postInstall = pkgs.lib.optionalString static ''
            if ${pkgs.file}/bin/file $out/bin/wtfs | grep -q dynamic; then
              echo "wtfs is not statically linked" >&2; exit 1
            fi
          '';
          meta = {
            description = "Find out where disk space went: fast parallel scan to a Parquet/Arrow table";
            homepage = "https://github.com/mbrock/wtfs";
            license = pkgs.lib.licenses.mit;
            platforms = systems;
            mainProgram = "wtfs";
          };
        };
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          wtfs = wtfsFor pkgs { };
        in
        {
          default = wtfs;
          inherit wtfs;
        }
        // pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          static = wtfsFor pkgs { static = true; };
        }
      );

      apps = forAllSystems (pkgs: {
        default = {
          type = "app";
          program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/wtfs";
          meta.description = "Scan a directory tree and summarize disk usage";
        };
      });

      checks = forAllSystems (
        pkgs:
        let
          wtfs = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
        in
        {
          inherit wtfs;
          # The total wtfs reports and the table's sum(du) both equal du -sB1,
          # including for hardlinks, sparse files and symlinks.
          matches-du =
            pkgs.runCommand "wtfs-matches-du"
              {
                nativeBuildInputs = [
                  wtfs
                  pkgs.duckdb
                  pkgs.coreutils
                ];
              }
              ''
                mkdir -p t/a/b t/c
                head -c 100000 /dev/zero > t/a/b/big
                ln t/a/b/big t/c/big-link
                truncate -s 1G t/sparse
                ln -s a t/link
                for i in $(seq 1 300); do echo $i > t/a/f$i; done

                expected=$(du -sB1 t | cut -f1)
                got=$(wtfs -q -n 0 -o scan.parquet t | sed -n 's/^# du \([0-9]*\) bytes.*/\1/p')
                summed=$(duckdb -csv -noheader -c "SELECT sum(du) FROM 'scan.parquet'")
                echo "du=$expected wtfs=$got sum(du)=$summed"
                [ "$expected" = "$got" ] && [ "$expected" = "$summed" ]
                touch $out
              '';
        }
      );

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.zig_0_17
            pkgs.duckdb
            pkgs.uv
            pkgs.python3
          ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
