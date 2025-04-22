# multipart-backup

A Python script for macOS and Linux that utilizes `dd` to create incremental backups of entire partitions, regardless of their contents or filesystem.

For example, after backing up a 1 GB partition into 100 MB chunks, the folder containing the backup may look like this:

    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000000
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000001
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000002
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000004
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000005
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000006
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000007
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000008
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000009
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000010
    -rw-r--r--  4 root  staff   25165824 Feb 21 23:15 part_00000011

Then, when the script is run again and the backup is updated, only the parts containing data that has changed will be updated. The other files will be left as-is.

The script can also optionally be used to create snapshots, where each backup is contained within its own timestamped folder. Each time a new snapshot is made, the script hard links the contents of the previous snapshot folder into the new snapshot folder, and then the multi-part files are updated. (Similar to macOS's Time Machine feature.) That way, multiple snapshots of the partition may be kept around while still utilizing space efficiently.

### Requirements:

- Python 3.x (tested with 3.12.3)
- `dd` is installed and in your PATH.

### Usage for backing up:

    backup-to-parts.py [-h] [-bs BLOCK_SIZE] [-ps PART_SIZE] [-k]
                       [-s SNAPSHOTS] [-u] [-l] [-f PASSPHRASE]
                       [-o PASSPHRASE] [-H LEVEL] source backup-root

* source: the file or device to backup, e.g. `/dev/rdisk1s2` or `/dev/sda2`. Can also be a partition UUID when `-u` is specified.

* backup-root: the path to the folder that will contain the backup

* `-bs SIZE` `--block-size SIZE`
Block size used with `dd` and when comparing files. Defaults to 1 MB.

* `-ps SIZE` `--part-size SIZE`:
The size of the parts the source file or device is split into. Defaults to 100 MB.

* `-k` `--keep-null-parts`:
The default behavior is any part of the backup that contains no data other than null bytes (zero) are represented by 0 bytes files. When this is used, all parts are kept at full size.  Note that this option is required when creating a one-off encrypted or obfuscated backup.

* `-s COUNT` `--snapshots COUNT`
Specifies how many snapshots are kept in the backup root. When set to 1 or higher, the script will create the snapshot folders in the backup root named with a timestamp. When set to 0, no snapshots are made and the backup root just contains all of the parts. The default is 4.

* `-f PASSPHRASE` `--fernet PASSPHRASE`
This option will create a backup of the source that is encrypted using the Fernet algorithm (from the [cryptography](https://pypi.org/search/?q=cryptography) package).  This backup will have each of its parts encrypted using the provided passphrase.<br><br>
Encrypted snapshots are consdiered "one-offs", and do not participate in incremental backups.  The program will detect these snapshots during incremental backups, and simply bypass them.

* `-o PASSPHRASE` `--obfuscate PASSPHRASE`
Backups can be obfuscated using a custom algorithm instead of encrypted with something more formal.  Unlike backups encrypted with traditional algorithms, obfuscation guarantees a 1:1 byte pattern and size footprint with the source partition.  Potential advantages to this are documented in the code.<br><br>
As with encrypted snapshots, obfuscated snapshots are also considered "one-offs", and do not participate in incremental backups.  The program will detect these snapshots during incremental backups, and ignore them.

* `-H` `--hash`
The SHA hash level the program should use if you enable `Fernet` or `Obfusctation`.  The default is 256 (i.e., SHA256), but you can explicitly specify 1, 384 or 512 depending upon your desired hardening amount. (SHA1 should be avoided--it has been deprecated since Google announced the first collision in 2017--but is included here for your discretionary use.)

* `-u` `--uuid`
Specifies that source is a partition UUID rather than a file or device identifier.

* `-l` `--symlink`
Use soft links (i.e. symlinks) instead of hard links for incremental backups.

* `-h` `--help`
Displays usage information

##### Example:

    backup-to-parts.py -ps 50m -bs 1m -s 10 /dev/rdisk4s1 /Volumes/Backups/external-drive-backup/

### Usage for restoring:

    restore-from-parts.py [-h] [-bs BLOCK_SIZE] [-s START] [-v] [-u]
                       [-f PASSPHRASE] [-o PASSPHRASE] [-H LEVEL]
                       snapshot-path destination

* snapshot-path: path to a folder containing all of the parts of a backup. When `-s` is non-zero when creating the backup, this is the path to a particular snapshot, otherwise it's the path to the backup root itself.

* destination: the file or device to restore onto, e.g. `/dev/rdisk1s2` or `/dev/sda2`

* `-bs SIZE` `--block-size SIZE`
Block size used with `dd`. Defaults to 1 MB.

* `-s START` `--start START`
Index of the part to start with when writing to the destination. The part is still written to the correct point on the drive as though restoration started with the first part. Useful to resume a restoration that has been stopped partway through.

* `-f PASSPHRASE` `--fernet PASSPHRASE`
Restore a snapshot previously encrypted using Fernet.  This option is required if you point the program at a snapshot folder that has detectable encrypted parts.

* `-c PASSPHRASE` `--clarify PASSPHRASE`
Restore a snapshot previously obfuscated.<br><br>
This option is _not_ required if you point the program at a snapshot folder that has detectable obfuscated parts.  Providing the passphrase will restore the partition to a state identical to a regular backup.  However, if you point the program at an obfuscated folder _without_ providing a passphrase, the program will "restore" the obfuscated binary data to the partition as-is without warning or complaint.  Your partition will not be directly usable (or even perhaps accessible).  See the code for the `Recast.obfuscate()` method for a more detailed description about this tactic.

* `-H` `--hash`
The SHA hash level the program should use if you enable `Fernet` or `Clarify`.  This should exactly match the level used to create the original backup, or your data will not be correctly transformed.

* `-u` `--uuid`
Specifies that destination is a partition UUID rather than a file or device identifier.

* `-h` `--help`
Displays usage information

##### Example:

    restore-from-parts.py -bs 1m /Volumes/Backups/external-drive-backup/snapshot-2018-04-20-001337 /dev/rdisk4s1

### Sizes

Similar to `dd`, where sizes are specified, a decimal, octal, or hexadecimal number of bytes is expected.  If the number ends with a `b`, `k`, `m`, `g`, or `w`, the number is multiplied by 512, 1024 (1K), 1048576 (1M), 1073741824 (1G) or the number of bytes in an integer, respectively.

### Some macOS notes:

It's much faster to specify a disk using `/dev/rdisk` rather than `/dev/disk`. The explanation can be found in the `hdiutil` manpage:

> `/dev/rdisk` nodes are character-special devices, but are "raw" in the BSD sense and force block-aligned I/O. They are closer to the physical disk than the buffer cache. `/dev/disk` nodes, on the other hand, are buffered block-special devices and are used primarily by the kernel's filesystem code.

### Why this exists

(see the [README in the original repository](https://github.com/briankendall/multipart-backup) for a detailed narrative as to why Brian created this.)

### Issues and future work

- Selecting symlinks instead of hard links will break snapshot rolling at the moment.

- There's no progress indicator other than how many parts have been copied

- No option for compression: I experimented early on with compressing each part of a backup using gzip, but it causes it to take a lot longer to perform the backup, and since my partitions are encrypted gzip didn't really save me any disk space anyway. So I settled instead of excluding parts that are totally blank. I may add an option later that allows using gzip.
