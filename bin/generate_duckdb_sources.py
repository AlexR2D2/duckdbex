#!/usr/bin/env python3
"""Generate the vendored DuckDB sources for duckdbex.

Must be run with CWD set to a duckdb/duckdb checkout. Writes the
package_build.py output tree (unity builds + directly referenced
sources + extension loader) to <target_dir>, plus two manifest files:

  .sources       - the compile list (fed to the Makefile)
  .include_dirs  - include roots (fed to the Makefile)

Usage:
  cd duckdb && python3 /path/to/generate_duckdb_sources.py <target_dir>
"""

import os
import sys


def main() -> None:
    target = os.path.abspath(sys.argv[1])
    sys.path.insert(0, os.path.join(os.getcwd(), "scripts"))
    from package_build import build_package

    sources, include_dirs, _originals = build_package(
        target_dir=target,
        extensions=["core_functions", "parquet", "json", "icu"],
        linenumbers=False,
        unity_count=32,
        folder_name="",
    )

    # build_package returns absolute paths for the unity files it writes
    # into target_dir; normalize everything to target-relative for .sources.
    target_prefix = os.path.abspath(target) + os.sep
    sources = [
        s[len(target_prefix):] if s.startswith(target_prefix) else s for s in sources
    ]

    # package_build always ships third_party/jemalloc, but upstream only enables
    # it on 64-bit Linux and duckdbex never enables it. jemalloc_cpp.cpp also
    # requires DUCKDB_OVERRIDE_NEW_DELETE, which we do not want in a NIF.
    def is_jemalloc(path):
        return path.replace(os.sep, "/").startswith("third_party/jemalloc/")

    sources = [s for s in sources if not is_jemalloc(s)]
    include_dirs = [d for d in include_dirs if not is_jemalloc(d)]

    with open(os.path.join(target, ".sources"), "w") as f:
        f.write("\n".join(sources) + "\n")
    with open(os.path.join(target, ".include_dirs"), "w") as f:
        f.write("\n".join(include_dirs) + "\n")

    print(f"[duckdbex] generated {len(sources)} sources into {target}")


if __name__ == "__main__":
    main()