
create table volumes (
    id uuid not null,
    label text,
    last_mount text,
    primary key (id)
);

create table inodes (
    volume_id uuid not null references volumes (id),
    inode_num int not null,
    unix_mode int2 not null,
    nlink int2 not null,
    uid int not null,
    gid int not null,
    rdev int,
    size int8 not null,
    alloc int8 not null,
    atime timestamptz,
    mtime timestamptz,
    ctime timestamptz,
    md5 text,
    sha1 text,
    gitsha1 text,
    primary key (volume_id, inode_num)
);

create table filenames (
    volume_id uuid not null references volumes (id),
    inode_num int not null,
    foreign key (volume_id, inode_num) references inodes (volume_id, inode_num),
    filename text not null,
    primary key (volume_id, inode_num, filename)
);
