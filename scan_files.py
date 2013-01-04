
import argparse
from datetime import datetime
from disk_catalogue import (
    dealer,
    Content,
    ContentInfo,
    Filename,
    Inode,
    Volume,
)
import hashlib
import magic
import re
import sys
import os
from sqlalchemy.orm.exc import NoResultFound
import sqlalchemy.sql.functions as func
from stat import S_IFREG, S_ISREG


class ScanFiles(object):
    def __init__(self):
        self.paths = []
        self.min = 20
        self.max = 2 * (2 ** 20)
        self.max_errors = 3
        self.scan_limit = float("inf")
        self.sesh = dealer()

    def ready(self):
        return len(self.paths) > 0

    def scan(self):
        print "Scanning: {p}".format(
            p=" ".join(self.paths),
        )
        for x in self.paths:
            self.scan_one(x)

    def get_volume(self, path):
        mount_path = path
        while mount_path != "/":
            try:
                volume = self.sesh.query(Volume).filter(
                    Volume.last_mount == mount_path
                ).one()
                return volume
            except NoResultFound, e:
                mount_path, junk = os.path.split(mount_path)
        raise Exception(
            "Can't find a catalogued volume matching {path}; run "
            "catalog.py first".format(path=path)
        )

    def scan_one(self, path):
        vol = self.get_volume(path)
        query = self.sesh.query(Inode, Filename).filter(
            # FIXME: this seems a little wrong :)
            Inode.volume_id == Filename.volume_id,
            Inode.inode_num == Filename.inode_num,
        ).outerjoin(Content).filter(
            Content.inode_num.__eq__(None),
        )
        scan_expr = (
            (Filename.volume_id == vol.id) &
            (Inode.unix_mode.op("&")(S_IFREG) == S_IFREG)
        )
        info = [
            "everything on {mount}".format(mount=vol.last_mount)
        ]
        if path != vol.last_mount:
            prefix = os.path.relpath(path, vol.last_mount)
            scan_expr &= Filename.filename.startswith(prefix)
            info.append("under {prefix}".format(prefix=prefix))

        if self.min != 0 or self.max != 0:
            if int(self.min) != 0:
                scan_expr &= (Inode.size >= self.min)
                info.append(
                    "larger than {minsize} bytes".format(
                        minsize=self.min,
                    )
                )
            if int(self.max) != 0:
                scan_expr &= (Inode.size <= self.max)
                info.append(
                    "smaller than {maxsize} bytes".format(
                        maxsize=self.max,
                    )
                )

        query = query.filter(scan_expr)
        print "Scanning {what}".format(what=", ".join(info))
        print "That matches: ",
        count = query.count()
        print "{x} filenames".format(x=count)
        last_inode_num = 0
        errors = 0
        added = 0
        try:
            for inode, filename in query.order_by(
                Inode.alloc, Inode.volume_id, Inode.inode_num,
                Filename.filename,
            ).yield_per(100):
                if filename.inode_num == last_inode_num:
                    print u"skipping {fn} (hard link)".format(
                        fn=filename.filename,
                    )
                elif not S_ISREG(inode.unix_mode):
                    print u"skipping {fn} (not a regular file)".format(
                        fn=filename.filename,
                    )
                else:
                    if not self.scan_file(vol, inode, filename):
                        errors += 1
                    else:
                        added += 1
                    if errors > self.max_errors:
                        self.sesh.commit()
                        raise Exception("too many errors, exiting")
                    if added >= self.scan_limit:
                        break
                last_inode_num = filename.inode_num
        except KeyboardInterrupt:
            print "Interrupted, shutting down cleanly."
        print "Added {added} entries; {e} error(s)".format(
            added=added,
            e=errors,
        )
        self.sesh.commit()

    def scan_file(self, vol, inode, filename):
        # find inode
        if filename.filename_raw is not None:
            fn = os.path.join(
                vol.last_mount.encode("utf8"),
                filename.filename_raw
            )
        else:
            fn = os.path.join(vol.last_mount, filename.filename)
        stat = os.stat(fn)
        errs = []
        if not stat:
            errs.append("could not stat")
        else:
            if stat.st_ino != inode.inode_num:
                errs.append("inode number changed")
            if stat.st_size != inode.size:
                errs.append("size changed")
        if len(errs):
            print u"Error scanning {fn}; {errs}".format(
                fn=fn,
                errs=", ".join(errs),
            )
        sha1 = hashlib.sha1()
        md5 = hashlib.md5()
        try:
            f = open(fn, 'rb')
        except IOError, e:
            print u"Failed to open {fn} for reading; {e}".format(
                fn=fn,
                e=e,
            )
            return False
        expected = stat.st_size
        gitsha1 = hashlib.sha1()
        gitsha1.update("blob %d\0" % expected)
        read = 0
        first_block = None
        while read < expected:
            remaining = expected - read
            bsize = 32768 if remaining > 32768 else remaining
            try:
                block = f.read(bsize)
            except IOError, e:
                print u"Failure while reading from {fn}; {e}".format(
                    fn=fn,
                    e=e,
                )
                return False
            if first_block is None:
                first_block = block
            if len(block) != bsize:
                print u"Short read from {fn}; skipping".format(
                    fn=fn,
                )
                f.close()
                return False
            # TODO: threads could speed up this part
            sha1.update(block)
            md5.update(block)
            gitsha1.update(block)
            read += len(block)
        f.close()

        sha1sum = sha1.hexdigest()
        content = Content(
            volume_id=vol.id,
            inode_num=stat.st_ino,
            size=stat.st_size,
            mtime=datetime.utcfromtimestamp(stat.st_mtime),
            ctime=datetime.utcfromtimestamp(stat.st_ctime),
            sha1=sha1sum,
        )
        self.sesh.add(content)
        try:
            content_info = self.sesh.query(ContentInfo).filter(
                ContentInfo.sha1 == sha1sum
            ).one()
        except NoResultFound:
            content_info = ContentInfo(
                sha1=sha1sum,
                md5=md5.hexdigest(),
                gitblobid=gitsha1.hexdigest(),
                magic_info=magic.from_buffer(first_block),
                mime_type=magic.from_buffer(first_block, mime=True),
            )
            self.sesh.add(content_info)
        print u"{sz}k inum {num}: {sha} {mime} {fn}".format(
            sz=inode.alloc / 1024,
            num=stat.st_ino,
            sha=sha1sum[0:11],
            mime=content_info.mime_type,
            fn=filename.filename,
        )
        return True
        

parser = argparse.ArgumentParser(
    description="scan files on a catalogued filesystem"
)
parser.add_argument(
    'paths', metavar="PATH", type=str, nargs="+",
    help="paths under which to scan"
)
parser.add_argument(
    "-m", "--min", metavar="SIZE", type=str,
    help="minimum file size to consider, eg 20b or 10k",
)
parser.add_argument(
    "-M", "--max", metavar="SIZE", type=str,
    help="maximum file size to consider, eg 2M",
)
parser.add_argument(
    "-n", "--scan-limit", metavar="NUM", type=int,
    help="only scan this many files in this run",
)
parser.add_argument(
    "-e", "--max-errors", metavar="NUM", type=int,
    help="maximum number of errors to tolerate before aborting",
)


def main(argv):
    scanfiles = ScanFiles()
    parser.parse_args(argv[1:], scanfiles)
    if not scanfiles.ready():
        parser.print_help()
    else:
        scanfiles.scan()


if __name__ == "__main__":
    import sys
    main(sys.argv)
