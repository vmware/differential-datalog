{ ghc ? null
, pkgs ? import <nixpkgs> { }
} @ args:

let

  ghc = args.ghc or pkgs.haskell.compiler.ghc865;
  java = pkgs.adoptopenjdk-bin;
  rust-toolchain = "1.39.0";

  flatbuffers-java = pkgs.runCommand "flatbuffers-java" {
    buildInputs = [ java pkgs.findutils ];
  } ''
    mkdir -p $out
    cp -r ${pkgs.flatbuffers.src}/java $out/
    chmod -R +w $out/java
    javac $out/java/com/google/flatbuffers/*.java
    rm -rf $out/java/com/google/flatbuffers/*.java
  '';

  stack-shell = pkgs.haskell.lib.buildStackProject {
    inherit ghc;
    name = "stack-shell";

    CLASSPATH = "${flatbuffers-java}/java";

    shellHook = ''
      export PATH="$HOME/.local/bin:$PATH"
    '';

    buildInputs = [
      pkgs.flatbuffers
      pkgs.zlib
      pkgs.adoptopenjdk-bin
      pkgs.gitMinimal
      pkgs.rustup
      pkgs.cacert
    ];
  };

  dev-shell = stack-shell.overrideAttrs (old: {
    buildInputs = old.buildInputs ++ [
      pkgs.stack
    ];

    shellHook = (old.shellHook or "") + ''

      unset STACK_IN_NIX_SHELL

      function initial-setup {
        rustup default ${rust-toolchain}
        stack build
        stack install
      }

      function ddlog-run {
        local program="$1"
        local program_noext=''${program%.*}

        if [ ! -f "$program" ]; then
          >&2 echo "You are trying to run $program, but it does not exist"
          return
        fi

        local input_file="$2"
        if [ -z "$input_file" ]; then
          local dat_name="$program_noext.dat"
          if [ -f "$dat_name" ]; then
            >&2 echo "Using $dat_name as an input file"
            input_file="$dat_name"
          fi
        fi
        ddlog -i "$program" -L '${builtins.toPath ./lib}' || return
        (cd "''${program_noext}_ddlog" && cargo build)
        if [ -z "$input_file" ]; then
          "''${program_noext}_ddlog/target/release/$(basename "$program_noext")_cli"
        else
          "''${program_noext}_ddlog/target/release/$(basename "$program_noext")_cli" --no-print < "$input_file"
        fi
      }
    '';
  });

in dev-shell // {

  inherit 
    stack-shell
  ;

}
