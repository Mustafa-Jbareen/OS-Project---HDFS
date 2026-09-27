#!/usr/bin/env python3
"""Page-cache step of the per-job protocol. Runs on a DataNode host.

Usage: cache-step.py cold|warm MOUNT_BASE IMAGE_DIR [FILE_GLOB]

  Both modes first run `sync` twice (the first flushes the loopback
  filesystems into their image files, the second flushes the images).
  cold  evict the data files and the loopback images from the page cache
        with posix_fadvise(DONTNEED) -- both cached copies of the data
  warm  read every data file once, so the next reader gets it from RAM

FILE_GLOB is relative to MOUNT_BASE; default: the HDFS block files
("dn*/hdfs_data/**/blk_*"). storage-bench.sh passes its own files.

Needs no root (Tapuz grants no sudo for drop_caches), so the step is the same
on every cluster. The OS, the Hadoop jars and filesystem metadata stay cached
in both modes: cold and warm differ only in the data itself.
"""
import glob
import os
import subprocess
import sys


def main(argv):
    if len(argv) < 3 or argv[0] not in ("cold", "warm"):
        print(__doc__)
        return 1
    mode, mount_base, image_dir = argv[:3]
    pattern = argv[3] if len(argv) > 3 else os.path.join("dn*", "hdfs_data", "**", "blk_*")

    subprocess.run(["sync"], check=False)
    subprocess.run(["sync"], check=False)
    files = glob.glob(os.path.join(mount_base, pattern), recursive=True)

    if mode == "cold":
        evicted = 0
        for path in files + glob.glob(os.path.join(image_dir, "hdfs_dn*.img")):
            try:
                fd = os.open(path, os.O_RDONLY)
            except OSError:
                continue
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                evicted += 1
            finally:
                os.close(fd)
        print(f"evicted {evicted} files")
    else:
        total = 0
        buf = bytearray(4 << 20)
        for path in files:
            try:
                with open(path, "rb", buffering=0) as f:
                    while True:
                        n = f.readinto(buf)
                        if not n:
                            break
                        total += n
            except OSError:
                pass
        print(f"read {total >> 20} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
