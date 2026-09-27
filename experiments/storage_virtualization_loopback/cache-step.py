#!/usr/bin/env python3
"""Page-cache step of the per-job protocol. Runs on a DataNode host.

Usage: cache-step.py cold|warm MOUNT_BASE IMAGE_DIR [FILE_GLOB]
       cache-step.py measure MOUNT_BASE [FILE_GLOB]

  cold and warm first run `sync` twice (the first flushes the loopback
  filesystems into their image files, the second flushes the images).
  cold     evict the data files and the loopback images from the page cache
           with posix_fadvise(DONTNEED) -- both cached copies of the data
  warm     read every data file once, so the next reader gets it from RAM
  measure  print how many MB of the data files are in the page cache now
           (mincore(2), like fincore; -1 if it cannot be measured). It only
           looks: nothing is read or evicted.

FILE_GLOB is relative to MOUNT_BASE; default: the HDFS block files
("dn*/hdfs_data/**/blk_*"). storage-bench.sh passes its own files.

Needs no root (Tapuz grants no sudo for drop_caches) and no fincore (not
installed on Tapuz), so the step is the same on every cluster. The OS, the
Hadoop jars and filesystem metadata stay cached in both modes: cold and warm
differ only in the data itself.
"""
import ctypes
import ctypes.util
import glob
import mmap
import os
import subprocess
import sys

# mincore() sets the lowest bit of a page's byte when the page is in RAM.
LOWEST_BIT = bytes(i & 1 for i in range(256))


def cached_bytes(paths):
    """Bytes of these files that are in the page cache (mmap + mincore)."""
    libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long)
    libc.munmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t)
    libc.mincore.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte))
    page = os.sysconf("SC_PAGE_SIZE")
    map_failed = ctypes.c_void_p(-1).value
    total = 0
    for path in paths:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            continue
        try:
            size = os.fstat(fd).st_size
            if size == 0:
                continue
            # Mapping a file does not read it; mincore only reports.
            addr = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
            if addr is None or addr == map_failed:
                raise OSError(ctypes.get_errno(), f"mmap failed for {path}")
            try:
                vec = (ctypes.c_ubyte * ((size + page - 1) // page))()
                if libc.mincore(addr, size, vec) != 0:
                    raise OSError(ctypes.get_errno(), f"mincore failed for {path}")
                total += min(bytes(vec).translate(LOWEST_BIT).count(1) * page, size)
            finally:
                libc.munmap(addr, size)
        finally:
            os.close(fd)
    return total


def main(argv):
    default_glob = os.path.join("dn*", "hdfs_data", "**", "blk_*")
    if len(argv) >= 2 and argv[0] == "measure":
        files = glob.glob(os.path.join(argv[1], argv[2] if len(argv) > 2 else default_glob), recursive=True)
        try:
            print(cached_bytes(files) >> 20)
        except Exception as e:  # noqa: BLE001 -- any failure means "not measured"
            print(f"cache-step.py measure: {e}", file=sys.stderr)
            print(-1)
        return 0
    if len(argv) < 3 or argv[0] not in ("cold", "warm"):
        print(__doc__)
        return 1
    mode, mount_base, image_dir = argv[:3]
    pattern = argv[3] if len(argv) > 3 else default_glob

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
