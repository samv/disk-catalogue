
import argparse
from datetime import datetime
from disk_catalogue import (
    Filename,
    Inode,
    sesh,
    Volume,
)
from fix_unicode import fix_bad_unicode
import re
import sys
import os
from sqlalchemy.orm.exc import NoResultFound
import sqlalchemy.sql.functions as func


class MountInfo(object):
    device = None
    uuid = None
    path = None
    volume = None
    

class CatalogueFS(object):
    def __init__(self):
        self.mount_point = []
        self.v = False
        self.parse_fail = 0
        self.read_mounts()

    def ready(self):
        if len(self.mount_point) == 0:
            return False
        else:
            return True
    
    def catalogue(self):
        print "Catalogueing: {mp}".format(
            mp=" ".join(self.mount_point),
        )
        for x in self.mount_point:
            self.catalogue_one(x)

    def read_mounts(self):
        mounts = os.popen("mount", "r")
        self.mi = {}
        for mount in mounts:
            # works with linux & macos x
            m = re.match(
                r"^(\S.*?) on (/.*?) (?:type (\S+) )?\((?:(\S+),)?",
                mount
            ) 
            if m:
                device, mount_point = m.group(1), m.group(2)
                fstype = m.group(3) or m.group(4)
                device_num = os.stat(mount_point).st_dev
                if device_num not in self.mi:
                    mi = self.mi[device_num] = MountInfo()
                    mi.device = device
                    mi.path = mount_point
                    mi.fstype = fstype
                else:
                    print(
                        "Device {dev} mounted at {path} and {path2}, "
                        "and that's BAD :-)".format(
                            dev=device,
                            path=mount_mount,
                            path2=mi.path
                        )
                    )
            else:
                self.parse_fail += 1

    def get_uuid(self, device, fstype):
        needs_root = True
        if re.match(r"ext\d+$", fstype):
            cmd = ["dumpe2fs", device]
            pattern = r"Filesystem UUID:\s+([a-f0-9\-]+)"
        else:
            raise Exception(
                "Don't know how to collect UUID from {fstype} ({dev})"
                .format(
                    fstype=fstype,
                    dev=device
                )
            )
        if needs_root:
            if os.getuid() == 0:
                pass
            elif os.geteuid() == 0:
                os.setuid(0)
            else:
                cmd = ["sudo"] + cmd
        pipe = os.popen(" ".join(cmd), "r")  # FIXME - use subprocess
        for l in pipe:
            m = re.search(pattern, l)
            if m:
                return m.group(1)
        raise Exception(
            "Failed to get UUID of {dev} using {cmd}".format(
                dev=device,
                cmd=" ".join(cmd),
            )
        )

    def mount_info(self, mount):
        devnum = os.stat(mount).st_dev
        mi = self.mi.get(devnum, None)
        if mi is None or not mount.startswith(mi.path):
            import pdb; pdb.set_trace()
            raise Exception(
                "my clever mount/device code failed, or you're expecting "
                "too much of me.  sorry."
            )
        if not hasattr(mi, "vfss"):
            # note: this can take a _long_ time on big FAT filesystems
            print "Checking size of {mount} ({dev}): ".format(
                mount=mi.path,
                dev=mi.device,
            ),
            vfss = mi.vfss = os.statvfs(mount)
            print "{files} files, {mb} MiB".format(
                files=str("(?)" if vfss.f_files is None else vfss.f_files),
                mb=str((vfss.f_bsize * (vfss.f_blocks - vfss.f_bfree)) >> 20),
            )
            # FIXME - call this earlier, to avoid sudo timeout
            mi.uuid = self.get_uuid(mi.device, mi.fstype)
            mi.volume = self.get_volume(mi)
        return mi

    def get_volume(self, mi):
        try:
            volume = sesh.query(Volume).filter(Volume.id == mi.uuid).one()
            volume.last_mount = mi.path
        except NoResultFound, e:
            volume = Volume(id=mi.uuid, last_mount=mi.path)
            sesh.add(volume)
        sesh.commit()
        return volume

    def get_file(self, mi, stat):
        inc = 0
        try:
            fobj = sesh.query(Inode).filter(
                Inode.volume_id == mi.uuid,
                Inode.inode_num == stat.st_ino,
            ).one()
            # TODO: update fields & invalidate checksums if critica
            # fields change
        except NoResultFound, e:
            fobj = Inode(
                volume_id=mi.uuid,
                inode_num=stat.st_ino,
                size=stat.st_size,
                unix_mode=stat.st_mode,
                mtime=datetime.utcfromtimestamp(stat.st_mtime),
                atime=datetime.utcfromtimestamp(stat.st_atime),
                ctime=datetime.utcfromtimestamp(stat.st_ctime),
                alloc=(stat.st_blocks * mi.vfss.f_bsize),
                rdev=stat.st_rdev,
                uid=stat.st_uid,
                gid=stat.st_gid,
                nlink=stat.st_nlink,
            ) 
            sesh.add(fobj)
            inc += 1
        return fobj, inc

    def get_filename(self, fobj, relname):
        """Returns a Filename object, given the Inode object and the filesystem
        relative name."""
        try:
            relname_utf8 = relname.decode("utf8")
        except UnicodeDecodeError:
            latin1 = relname.decode("latin1")
            relname_utf8 = fix_bad_unicode(latin1)
            print u"Tidied {rn} to '{new}'".format(
                rn=repr(relname),
                new=relname_utf8,
            )
        relname_bin = None
        if relname_utf8.encode("utf8") != relname:
            relname_bin = relname
        try:
            fnobj = sesh.query(Filename).filter(
                Filename.volume_id == fobj.volume_id,
                Filename.inode_num == fobj.inode_num,
                Filename.filename == relname_utf8,
            ).one()
            #TODO: check other names & remove if necc.
            #FIXME: multiple links to same file differing only by unicode!
        except NoResultFound:
            fnobj = Filename(
                volume_id=fobj.volume_id,
                inode_num=fobj.inode_num,
                filename=relname_utf8,
                filename_raw=relname_bin,
            )
            sesh.add(fnobj)
        return fnobj

    def catalogue_one(self, mount):
        mi = self.mount_info(mount)
        volume = self.get_volume(mi)
        print "Checking database: ",
        have_files = sesh.query(Inode).filter(
            Inode.volume_id == volume.id
        ).count()
        print "{0} file(s),".format(have_files),
        try:
            have_size = sesh.query(
                func.sum(Inode.alloc),
            ).filter(
                Inode.volume_id == volume.id
            ).group_by(Inode.volume_id).one()[0]
        except NoResultFound:
            have_size = 0
        print "{0} MiB".format(int(have_size / (2 ** 20)))
        scanned = new = 0
        def show_progress():
            print "found {x}/{tot} file(s), ~{m}/{totm} MiB".format(
                x=(have_files + new),
                tot=(mi.vfss.f_files - mi.vfss.f_ffree),
                m=int(have_size / (2 ** 20)),
                totm=int((
                    (mi.vfss.f_bsize * (mi.vfss.f_blocks - mi.vfss.f_bfree))
                ) / (2 ** 20)),
            )

        for dirpath, dirnames, filenames in os.walk(mount):
            reldir = os.path.relpath(dirpath, mi.path)
            for filename in filenames:
                stat = os.lstat(os.path.join(dirpath, filename))
                fobj, inc = self.get_file(mi, stat)
                relfile = (
                    os.path.join(reldir, filename) if reldir != "." else
                    filename
                )
                fnobj = self.get_filename(fobj, relfile)
                scanned += 1
                new += inc
                if inc > 0:
                    have_size += mi.vfss.f_bsize * stat.st_blocks
                if scanned % 1000 == 0:
                    show_progress()
                    sesh.commit()
        show_progress()
        sesh.commit()


parser = argparse.ArgumentParser(description="catalog a filesystem")
parser.add_argument(
    'mount_point', metavar="PATH", type=str, nargs="+",
    help="the mounted root of the filesystem to scan"
)
parser.add_argument(
    "-v", "--verbose",
    help="show directories being added etc",
)


def main(argv):
    cat = CatalogueFS()
    parser.parse_args(argv[1:], cat)
    if not cat.ready():
        parser.print_help()
    else:
        cat.catalogue()


if __name__ == "__main__":
    import sys
    main(sys.argv)
