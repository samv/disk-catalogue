
create table volumes (
    id uuid not null,
    last_mount text,
    primary key (id)
);

create table inodes (
    volume_id uuid not null references volumes (id),
    inode_num int not null,
    unix_mode int4 not null,
    nlink int2 not null,
    uid int not null,
    gid int not null,
    rdev int,
    size int8 not null,
    alloc int8 not null,
    atime timestamptz,
    mtime timestamptz,
    ctime timestamptz,
    primary key (volume_id, inode_num)
);

create table filenames (
    volume_id uuid not null references volumes (id),
    inode_num int not null,
    foreign key (volume_id, inode_num) references inodes (volume_id, inode_num),
    filename text not null,
    filename_raw bytea,
    primary key (volume_id, inode_num, filename)
);

create table content (
    volume_id uuid not null references volumes (id),
    inode_num int not null,
    foreign key (volume_id, inode_num) references inodes (volume_id, inode_num),
    size int8 not null,
    ctime timestamptz,
    primary key (volume_id, inode_num, size, ctime),
    mtime timestamptz,
    sha1 char(40)
);

create table content_info (
    sha1 text not null,
    primary key (sha1),
    md5 char(32),
    gitblobid char(40),
    magic_info text,
    mime_type text
);
