CHUNKSIZE = 1<<26

GETDIR_FLAG_WITHATTR   = 0x01
GETDIR_FLAG_ADDTOCACHE = 0x02

#type for readdir command
TYPE_FILE      = 'f'
TYPE_SYMLINK   = 'l'
TYPE_DIRECTORY = 'd'
TYPE_FIFO      = 'q'
TYPE_BLOCKDEV  = 'b'
TYPE_CHARDEV   = 'c'
TYPE_SOCKET    = 's'
TYPE_TRASH     = 't'
TYPE_SUSTAINED  = 'r'
TYPE_UNKNOWN   = '?'

ERROR_MAX            = 38

# CHUNKSERVER <-> CLIENT/CHUNKSERVER
CLTOCS_READ = 200
# chunkid:64 version:32 offset:32 size:32
CSTOCL_READ_STATUS = 201
# chunkid:64 status:8
CSTOCL_READ_DATA = 202
# chunkid:64 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]

CLTOMA_FUSE_GETATTR = 408
# msgid:32 inode:32
# msgid:32 inode:32 uid:32 gid:32
MATOCL_FUSE_GETATTR = 409
# msgid:32 status:8
# msgid:32 attr:35B
CLTOMA_FUSE_READLINK = 412
# msgid:32 inode:32
MATOCL_FUSE_READLINK = 413
# msgid:32 status:8
# msgid:32 length:32 path:lengthB
CLTOMA_FUSE_READDIR = 428
# msgid:32 inode:32 uid:32 gid:32 - old version (works like new version with flags==0
# msgid:32 inode:32 uid:32 gid:32 flags:8
MATOCL_FUSE_READDIR = 429
# msgid:32 status:8
# msgid:32 N*[ name:NAME inode:32 type:8 ] - when GETDIR_FLAG_WITHATTR in flags is not set
# msgid:32 N*[ name:NAME inode:32 type:35B ]   - when GETDIR_FLAG_WITHATTR in flags is set

CLTOMA_FUSE_READ_CHUNK = 432
# msgid:32 inode:32 chunkindx:32
MATOCL_FUSE_READ_CHUNK = 433
# msgid:32 status:8
# msgid:32 length:64 chunkid:64 version:32 N*[ip:32 port:16]
# msgid:32 length:64 srcs:8 srcs*[chunkid:64 version:32 ip:32 port:16] - not implemented

errtab = [
    "OK",
    "Operation not permitted",
    "Not a directory",
    "No such file or directory",
    "Permission denied",
    "File exists",
    "Invalid argument",
    "Directory not empty",
    "Chunk lost",
    "Out of memory",
    "Index too big",
    "Chunk locked",
    "No chunk servers",
    "No such chunk",
    "Chunk is busy",
    "Incorrect register BLOB",
    "None of chunk servers performed requested operation",
    "File not opened",
    "Write not started",
    "Wrong chunk version",
    "Chunk already exists",
    "No space left",
    "IO error",
    "Incorrect block number",
    "Incorrect size",
    "Incorrect offset",
    "Can't connect",
    "Incorrect chunk id",
    "Disconnected",
    "CRC error",
    "Operation delayed",
    "Can't create path",
    "Data mismatch",
    "Read-only file system",
    "Quota exceeded",
    "Bad session id",
    "Password is needed",
    "Incorrect password",
    "Unknown MFS error",
]

def strerror(code):
    if code > ERROR_MAX:
        code = ERROR_MAX
    return errtab[code]

S_IFDIR  = 0o040000 # directory */
S_IFREG  = 0o100000 # regular */
S_IFLNK  = 0o120000 # symbolic link */
