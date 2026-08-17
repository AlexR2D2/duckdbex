#!/usr/bin/env python3
"""Create a deterministic archive of generated DuckDB sources."""

import gzip
import os
from pathlib import Path
import stat
import sys
import tarfile


def archive_entries(source: Path):
    yield source

    for root, directories, files in os.walk(source):
        directories[:] = sorted(name for name in directories if name != ".DS_Store")

        for name in directories:
            yield Path(root, name)

        for name in sorted(name for name in files if name != ".DS_Store"):
            yield Path(root, name)


def normalize(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0

    if info.isdir():
        info.mode = 0o755
    elif info.isfile():
        info.mode = 0o755 if info.mode & stat.S_IXUSR else 0o644

    return info


def create_archive(source: Path, destination: Path) -> None:
    with destination.open("wb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.GNU_FORMAT,
            ) as archive:
                for path in archive_entries(source):
                    relative_path = path.relative_to(source)
                    archive_path = Path("duckdb", relative_path)
                    info = normalize(archive.gettarinfo(path, archive_path.as_posix()))

                    if info.isfile():
                        with path.open("rb") as contents:
                            archive.addfile(info, contents)
                    else:
                        archive.addfile(info)


def main() -> None:
    source = Path(sys.argv[1]).resolve()
    destination = Path(sys.argv[2]).resolve()

    create_archive(source, destination)
    print(f"[duckdbex] archived {source} into {destination}")


if __name__ == "__main__":
    main()
