
VERSION_ANY       = 0
CRC_POLY          = 0xEDB88320
MFS_ROOT_ID       = 1
MFS_NAME_MAX      = 255
MFS_MAX_FILE_SIZE = 0x20000000000

# 1.6.27
VERSION = 0x01061B

CHUNKSIZE = 1<<26

GETDIR_FLAG_WITHATTR   = 0x01
GETDIR_FLAG_ADDTOCACHE = 0x02
GETDIR_FLAG_DIRCACHE   = 0x04

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

# status code
STATUS_OK            = 0 # OK

ERROR_EPERM          = 1 # Operation not permitted
ERROR_ENOTDIR        = 2 # Not a directory
ERROR_ENOENT         = 3 # No such file or directory
ERROR_EACCES         = 4 # Permission denied
ERROR_EEXIST         = 5 # File exists
ERROR_EINVAL         = 6 # Invalid argument
ERROR_ENOTEMPTY      = 7 # Directory not empty
ERROR_CHUNKLOST      = 8 # Chunk lost
ERROR_OUTOFMEMORY    = 9 # Out of memory

ERROR_INDEXTOOBIG    = 10 # Index too big
ERROR_LOCKED         = 11 # Chunk locked
ERROR_NOCHUNKSERVERS = 12 # No chunk servers
ERROR_NOCHUNK        = 13 # No such chunk
ERROR_CHUNKBUSY      = 14 # Chunk is busy
ERROR_REGISTER       = 15 # Incorrect register BLOB
ERROR_NOTDONE        = 16 # None of chunk servers performed requested operation
ERROR_NOTOPENED      = 17 # File not opened
ERROR_NOTSTARTED     = 18 # Write not started

ERROR_WRONGVERSION   = 19 # Wrong chunk version
ERROR_CHUNKEXIST     = 20 # Chunk already exists
ERROR_NOSPACE        = 21 # No space left
ERROR_IO             = 22 # IO error
ERROR_BNUMTOOBIG     = 23 # Incorrect block number
ERROR_WRONGSIZE      = 24 # Incorrect size
ERROR_WRONGOFFSET    = 25 # Incorrect offset
ERROR_CANTCONNECT    = 26 # Can't connect
ERROR_WRONGCHUNKID   = 27 # Incorrect chunk id
ERROR_DISCONNECTED   = 28 # Disconnected
ERROR_CRC            = 29 # CRC error
ERROR_DELAYED        = 30 # Operation delayed
ERROR_CANTCREATEPATH = 31 # Can't create path

ERROR_MISMATCH       = 32 # Data mismatch
ERROR_EROFS          = 33 # Read-only file system
ERROR_QUOTA          = 34 # Quota exceeded
ERROR_BADSESSIONID   = 35 # Bad session id
ERROR_NOPASSWORD     = 36 # Password is needed
ERROR_BADPASSWORD    = 37 # Incorrect password
ERROR_MAX            = 38

# flags: "flags" fileld in "CLTOMA_FUSE_AQUIRE"
WANT_READ    = 1
WANT_WRITE   = 2
AFTER_CREATE = 4

# flags: "setmask" field in "CLTOMA_FUSE_SETATTR"
# SET_GOAL_FLAG,SET_DELETE_FLAG are no longer supported
# SET_LENGTH_FLAG,SET_OPENED_FLAG are deprecated
# instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETGOAL command
# instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETTRASH_TIMEOUT command
# instead of using FUSE_SETATTR with SET_LENGTH_FLAG/SET_OPENED_FLAG use FUSE_TRUNCATE command
SET_GOAL_FLAG     = 1 << 0
SET_MODE_FLAG     = 1 << 1
SET_UID_FLAG      = 1 << 2
SET_GID_FLAG      = 1 << 3
SET_LENGTH_FLAG   = 1 << 4
SET_MTIME_FLAG    = 1 << 5
SET_ATIME_FLAG    = 1 << 6
SET_OPENED_FLAG   = 1 << 7
SET_DELETE_FLAG   = 1 << 8
ANTOAN_NOP        = 0

# CHUNKSERVER <-> CLIENT/CHUNKSERVER
CLTOCS_READ = 200
# chunkid:64 version:32 offset:32 size:32
CSTOCL_READ_STATUS = 201
# chunkid:64 status:8
CSTOCL_READ_DATA = 202
# chunkid:64 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]

CLTOCS_WRITE = 210
# chunkid:64 version:32 N*[ ip:32 port:16 ]
CSTOCL_WRITE_STATUS = 211
# chunkid:64 writeid:32 status:8
CLTOCS_WRITE_DATA = 212
# chunkid:64 writeid:32 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]
CLTOCS_WRITE_FINISH = 213
# chunkid:64 version:32

#ANY <-> CHUNKSERVER
ANTOCS_CHUNK_CHECKSUM = 300
# chunkid:64 version:32
CSTOAN_CHUNK_CHECKSUM = 301
# chunkid:64 version:32 checksum:32
# chunkid:64 version:32 status:8

ANTOCS_CHUNK_CHECKSUM_TAB = 302
# chunkid:64 version:32
CSTOAN_CHUNK_CHECKSUM_TAB = 303
# chunkid:64 version:32 1024*[checksum:32]
# chunkid:64 version:32 status:8

# CLIENT <-> MASTER

# old attr record:
#   type:8 flags:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 length:64
#   total: 32B (1+1+2+4+4+4+4+4+8
#
#   flags: ---DGGGG
#             |\--/
#             |  \------ goal
#             \--------- delete imediatelly

# new attr record:
#   type:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 nlink:32 length:64
#   total: 35B
#
#   mode: FFFFMMMMMMMMMMMM
#         \--/\----------/
#           \       \------- mode
#            \-------------- flags
#
#   in case of BLOCKDEV and CHARDEV instead of 'length:64' on the end there is 'mojor:16 minor:16 empty:32'

# NAME type:
# ( leng:8 data:lengB


FUSE_REGISTER_BLOB_NOACL = "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx64"
# CLTOMA:
#  clientid:32 [ version:32 ]
# MATOCL:
#  clientid:32
#  status:8

FUSE_REGISTER_BLOB_TOOLS_NOACL = "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx63"
# CLTOMA:
#  -
# MATOCL:
#  status:8

FUSE_REGISTER_BLOB_ACL = "DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0"

REGISTER_GETRANDOM = '\1'
# rcode==1: generate random blob
# CLTOMA:
#  rcode:8
# MATOCL:
#  randomblob:32B

REGISTER_NEWSESSION = '\2'
# rcode==2: first register
# CLTOMA:
#  rcode:8 version:32 ileng:32 info:ilengB pleng:32 path:plengB [ passcode:16B ]
# MATOCL:
#  sessionid:32 sesflags:8 rootuid:32 rootgid:32
#  status:8

REGISTER_RECONNECT = '\3'
# rcode==3: mount reconnect
# CLTOMA:
#  rcode:8 sessionid:32 version:32
# MATOCL:
#  status:8

REGISTER_TOOLS = '\4'
# rcode==4: tools connect
# CLTOMA:
#  rcode:8 sessionid:32 version:32
# MATOCL:
#  status:8

REGISTER_NEWMETASESSION = '\5'
# rcode==5: first register
# CLTOMA:
#  rcode:8 version:32 ileng:32 info:ilengB [ passcode:16B ]
# MATOCL:
#  sessionid:32 sesflags:8
#  status:8

REGISTER_CLOSESESSION = '\6'
# rcode==6: close session
# CLTOMA:
#  rcode:8 sessionid:32 [ metaid:64 ]
# MATOCL:
#  status:8

CLTOMA_FUSE_REGISTER = 400
# blob:64B ... (depends on blob - see blob descriptions above
MATOCL_FUSE_REGISTER = 401
# depends on blob - see blob descriptions above
CLTOMA_FUSE_STATFS = 402
# msgid:32 -
MATOCL_FUSE_STATFS = 403
# msgid:32 totalspace:64 availspace:64 trashspace:64 inodes:32
CLTOMA_FUSE_ACCESS = 404
# msgid:32 inode:32 uid:32 gid:32 modemask:8
MATOCL_FUSE_ACCESS = 405
# msgid:32 status:8
CLTOMA_FUSE_LOOKUP = 406
# msgid:32 inode:32 name:NAME uid:32 gid:32
MATOCL_FUSE_LOOKUP = 407
# msgid:32 status:8
# msgid:32 inode:32 attr:35B
CLTOMA_FUSE_GETATTR = 408
# msgid:32 inode:32
# msgid:32 inode:32 uid:32 gid:32
MATOCL_FUSE_GETATTR = 409
# msgid:32 status:8
# msgid:32 attr:35B
CLTOMA_FUSE_SETATTR = 410
# msgid:32 inode:32 uid:32 gid:32 setmask:8 attr:32B   - compatibility with very old version
# msgid:32 inode:32 uid:32 gid:32 setmask:16 attr:32B  - compatibility with old version
# msgid:32 inode:32 uid:32 gid:32 setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32
MATOCL_FUSE_SETATTR = 411
# msgid:32 status:8
# msgid:32 attr:35B
CLTOMA_FUSE_READLINK = 412
# msgid:32 inode:32
MATOCL_FUSE_READLINK = 413
# msgid:32 status:8
# msgid:32 length:32 path:lengthB
CLTOMA_FUSE_SYMLINK = 414
# msgid:32 inode:32 name:NAME length:32 path:lengthB uid:32 gid:32
MATOCL_FUSE_SYMLINK = 415
# msgid:32 status:8
# msgid:32 inode:32 attr:35B
CLTOMA_FUSE_MKNOD = 416
# msgid:32 inode:32 name:NAME type:8 mode:16 uid:32 gid:32 rdev:32
MATOCL_FUSE_MKNOD = 417
# msgid:32 status:8
# msgid:32 inode:32 attr:35B
CLTOMA_FUSE_MKDIR = 418
# msgid:32 inode:32 name:NAME mode:16 uid:32 gid:32
MATOCL_FUSE_MKDIR = 419
# msgid:32 status:8
# msgid:32 inode:32 attr:35B
CLTOMA_FUSE_UNLINK = 420
# msgid:32 inode:32 name:NAME uid:32 gid:32
MATOCL_FUSE_UNLINK = 421
# msgid:32 status:8
CLTOMA_FUSE_RMDIR = 422
# msgid:32 inode:32 name:NAME uid:32 gid:32
MATOCL_FUSE_RMDIR = 423
# msgid:32 status:8
CLTOMA_FUSE_RENAME = 424
# msgid:32 inode_src:32 name_src:NAME inode_dst:32 name_dst:NAME uid:32 gid:32
MATOCL_FUSE_RENAME = 425
# msgid:32 status:8
CLTOMA_FUSE_LINK = 426
# msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32
MATOCL_FUSE_LINK = 427
# msgid:32 status:8
# msgid:32 inode:32 attr:35B
CLTOMA_FUSE_GETDIR = 428
# msgid:32 inode:32 uid:32 gid:32 - old version (works like new version with flags==0
# msgid:32 inode:32 uid:32 gid:32 flags:8
MATOCL_FUSE_GETDIR = 429
# msgid:32 status:8
# msgid:32 N*[ name:NAME inode:32 type:8 ] - when GETDIR_FLAG_WITHATTR in flags is not set
# msgid:32 N*[ name:NAME inode:32 type:35B ]   - when GETDIR_FLAG_WITHATTR in flags is set
CLTOMA_FUSE_OPEN = 430
# msgid:32 inode:32 uid:32 gid:32 flags:8
MATOCL_FUSE_OPEN = 431
# msgid:32 status:8
# since 1.6.9 if no error:
# msgid:32 attr:35B

CLTOMA_FUSE_READ_CHUNK = 432
# msgid:32 inode:32 chunkindx:32
MATOCL_FUSE_READ_CHUNK = 433
# msgid:32 status:8
# msgid:32 length:64 chunkid:64 version:32 N*[ip:32 port:16]
# msgid:32 length:64 srcs:8 srcs*[chunkid:64 version:32 ip:32 port:16] - not implemented
CLTOMA_FUSE_WRITE_CHUNK = 434 # it creates, duplicates or sets new version of chunk if necessary */
# msgid:32 inode:32 chunkindx:32
MATOCL_FUSE_WRITE_CHUNK = 435
# msgid:32 status:8
# msgid:32 length:64 chunkid:64 version:32 N*[ip:32 port:16]
CLTOMA_FUSE_WRITE_CHUNK_END = 436
# msgid:32 chunkid:64 inode:32 length:64
MATOCL_FUSE_WRITE_CHUNK_END = 437
# msgid:32 status:8


CLTOMA_FUSE_APPEND = 438
# msgid:32 inode:32 srcinode:32 uid:32 gid:32 - append to existing element
MATOCL_FUSE_APPEND = 439
# msgid:32 status:8


CLTOMA_FUSE_CHECK = 440
# msgid:32 inode:32
MATOCL_FUSE_CHECK = 441
# msgid:32 status:8
# msgid:32 N*[ copies:8 chunks:16 ]

CLTOMA_FUSE_GETTRASHTIME = 442
# msgid:32 inode:32 gmode:8
MATOCL_FUSE_GETTRASHTIME = 443
# msgid:32 status:8
# msgid:32 tdirs:32 tfiles:32 tdirs*[ trashtime:32 dirs:32 ] tfiles*[ trashtime:32 files:32 ]
CLTOMA_FUSE_SETTRASHTIME = 444
# msgid:32 inode:32 uid:32 trashtimeout:32 smode:8
MATOCL_FUSE_SETTRASHTIME = 445
# msgid:32 status:8
# msgid:32 changed:32 notchanged:32 notpermitted:32

CLTOMA_FUSE_GETGOAL = 446
# msgid:32 inode:32 gmode:8
MATOCL_FUSE_GETGOAL = 447
# msgid:32 status:8
# msgid:32 gdirs:8 gfiles:8 gdirs*[ goal:8 dirs:32 ] gfiles*[ goal:8 files:32 ]

CLTOMA_FUSE_SETGOAL = 448
# msgid:32 inode:32 uid:32 goal:8 smode:8
MATOCL_FUSE_SETGOAL = 449
# msgid:32 status:8
# msgid:32 changed:32 notchanged:32 notpermitted:32

CLTOMA_FUSE_GETTRASH = 450
# msgid:32
MATOCL_FUSE_GETTRASH = 451
# msgid:32 status:8
# msgid:32 N*[ name:NAME inode:32 ]

CLTOMA_FUSE_GETDETACHEDATTR = 452
# msgid:32 inode:32 dtype:8
MATOCL_FUSE_GETDETACHEDATTR = 453
# msgid:32 status:8
# msgid:32 attr:35B

CLTOMA_FUSE_GETTRASHPATH = 454
# msgid:32 inode:32
MATOCL_FUSE_GETTRASHPATH = 455
# msgid:32 status:8
# msgid:32 length:32 path:lengthB

CLTOMA_FUSE_SETTRASHPATH = 456
# msgid:32 inode:32 length:32 path:lengthB
MATOCL_FUSE_SETTRASHPATH = 457
# msgid:32 status:8

CLTOMA_FUSE_UNDEL = 458
# msgid:32 inode:32
MATOCL_FUSE_UNDEL = 459
# msgid:32 status:8
CLTOMA_FUSE_PURGE = 460
# msgid:32 inode:32
MATOCL_FUSE_PURGE = 461
# msgid:32 status:8

CLTOMA_FUSE_GETDIRSTATS = 462
# msgid:32 inode:32
MATOCL_FUSE_GETDIRSTATS = 463
# msgid:32 status:8
# msgid:32 inodes:32 dirs:32 files:32 ugfiles:32 mfiles:32 chunks:32 ugchunks:32 mchunks32 length:64 size:64 gsize:64

CLTOMA_FUSE_TRUNCATE = 464
# msgid:32 inode:32 [opened:8] uid:32 gid:32 opened:8 length:64
MATOCL_FUSE_TRUNCATE = 465
# msgid:32 status:8
# msgid:32 attr:35B

CLTOMA_FUSE_REPAIR = 466
# msgid:32 inode:32 uid:32 gid:32
MATOCL_FUSE_REPAIR = 467
# msgid:32 status:8
# msgid:32 notchanged:32 erased:32 repaired:32

CLTOMA_FUSE_SNAPSHOT = 468
# msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32 canoverwrite:8
MATOCL_FUSE_SNAPSHOT = 469
# msgid:32 status:8

CLTOMA_FUSE_GETSUSTAINED = 470
# msgid:32
MATOCL_FUSE_GETSUSTAINED = 471
# msgid:32 status:8
# msgid:32 N*[ name:NAME inode:32 ]

CLTOMA_FUSE_GETEATTR = 472
# msgid:32 inode:32 gmode:8
MATOCL_FUSE_GETEATTR = 473
# msgid:32 status:8
# msgid:32 eattrdirs:8 eattrfiles:8 eattrdirs*[ eattr:8 dirs:32 ] eattrfiles*[ eattr:8 files:32 ]

CLTOMA_FUSE_SETEATTR = 474
# msgid:32 inode:32 uid:32 eattr:8 smode:8
MATOCL_FUSE_SETEATTR = 475
# msgid:32 status:8
# msgid:32 changed:32 notchanged:32 notpermitted:32

CLTOMA_FUSE_QUOTACONTROL = 476
# msgid:32 inode:32 qflags:8 - delete quota
# msgid:32 inode:32 qflags:8 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 - set quota
MATOCL_FUSE_QUOTACONTROL = 477
# msgid:32 status:8
# msgid:32 qflags:8 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 curinodes:32 curlength:64 cursize:64 currealsize:64

CLTOMA_FUSE_DIR_REMOVED = 490
# msgid:32 N*[ inode:32 ]

# special - sustained (opened) inodes - keep opened files.
CLTOMA_FUSE_SUSTAINED_INODES = 499
# N*[inode:32]

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

def mfs_strerror(code):
    if code > ERROR_MAX:
        code = ERROR_MAX
    return errtab[code]

S_IFMT   = 0170000 # type of file */
S_IFIFO  = 0010000 # named pipe (fifo) */
S_IFCHR  = 0020000 # character special */
S_IFDIR  = 0040000 # directory */
S_IFBLK  = 0060000 # block special */
S_IFREG  = 0100000 # regular */
S_IFLNK  = 0120000 # symbolic link */
S_IFSOCK = 0140000 # socket */
S_IFWHT  = 0160000 # whiteout */
S_ISUID  = 0004000 # set user id on execution */
S_ISGID  = 0002000 # set group id on execution */
S_ISVTX  = 0001000 # save swapped text even after use */
S_IRUSR  = 0000400 # read permission, owner */
S_IWUSR  = 0000200 # write permission, owner */
S_IXUSR  = 0000100 # execute/search permission, owner */
