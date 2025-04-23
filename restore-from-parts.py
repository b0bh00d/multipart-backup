#!/usr/bin/env python3

import os
import sys
import argparse
import subprocess

import shared
from recaster import Recaster

from typing import List

def checkPartsAndGetPartSize(backupPath: str, parts: List[str], blockSize: int) -> None:
    """Checks to make sure all the parts in the given backup are a consistent size, and returns that size."""
    backupPartSize = None

    for i in range(len(parts)-1):
        part = parts[i]
        partPath = os.path.join(backupPath, part)
        partSize = os.stat(partPath).st_size

        if partSize == 0:
            continue

        if backupPartSize is None:
            backupPartSize = partSize
        else:
            if partSize != backupPartSize:
                raise shared.BackupDataError('Parts in backup have inconsistent sizes. Backup may be corrupted!')

            if partSize % blockSize != 0:
                print(partSize, blockSize)
                raise shared.BackupDataError('Parts in backup have a size that is not an integer multiple of the block size. '
                                      'Please specify a compatible block size.')

    return backupPartSize

def restore(args: argparse.Namespace) -> None:
    dest = shared.deviceIdentifierForSourceString(args.dest, args.uuid)
    recaster = Recaster(hashlvl=args.hash, passphrase=args.clarify if args.clarify else args.fernet)

    reconstitute = None
    parts = shared.partsInSnapshot(args.backup)
    if len(parts) == 0:
        parts = shared.partsInSnapshot(args.backup, variant="obfuscated")
        if len(parts):
            sys.stdout.write(f"Found {len(parts)} obfuscation files.\n")
            if args.clarify:
                reconstitute = recaster.clarify
        else:
            parts = shared.partsInSnapshot(args.backup, variant="encrypted")
            if len(parts):
                if args.fernet:
                    sys.stdout.write(f"Found {len(parts)} encrypted files.\n")
                    reconstitute = recaster.decrypt
                else:
                    # we cannot simply write this data to the partition
                    raise Exception('Encrypted files found in snapshot without invoking "fernet" decryption.')
    else:
        sys.stdout.write(f"Found {len(parts)} backup files.\n")

    backupPartSize = checkPartsAndGetPartSize(args.backup, parts, args.blockSize)

    if backupPartSize is None:
        raise shared.BackupDataError('Could not deduce part size... are all of your parts 0 bytes in size?')

    partBlockCount = backupPartSize // args.blockSize
    speedCalculator = shared.AverageSpeedCalculator(5)

    for i in range(args.startPartIndex, len(parts)):
        speedCalculator.startOfCycle()

        partPath = os.path.join(args.backup, parts[i])
        partSize = os.stat(partPath).st_size

        if speedCalculator.averageSpeed() is not None:
            shared.outputStatus(f"Restoring part index {i} ... speed: {shared.humanReadableSize(speedCalculator.averageSpeed())}/sec")
        else:
            shared.outputStatus(f"Restoring part index {i} ...")

        if partSize == 0:
            # If the file size is 0, that indicates that it was a full size part that contained only zeros, so we
            # can pull data from /dev/zero for this part.
            partPathToUse = '/dev/zero'
        else:
            if reconstitute:
                data = reconstitute(partPath)
                if data:
                    # write it out to a temporary file
                    path, filename = os.path.split(partPath)
                    basename, ext = os.path.splitext(filename)
                    partPath = os.path.join(path, f"__{basename}__")
                    with open(partPath, 'wb') as f:
                        f.write(data)
                    data = None

            partPathToUse = partPath

        if args.verbose:
            p = subprocess.Popen(['dd', f'if={partPathToUse}', f'of={dest}', f'bs={args.blockSize}', f'count={partBlockCount}',
                      f'oseek={i * partBlockCount}'])
            p.communicate()
        else:
            p = subprocess.Popen(['dd', f'if={partPathToUse}', f'of={dest}', f'bs={args.blockSize}', f'count={partBlockCount}',
                      f'oseek={i * partBlockCount}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()

        if reconstitute:
            # remove the temporary file
            os.remove(partPathToUse)

        if p.returncode != 0:
            sys.stderr.write(f'dd failed! Output:\n  "{err.decode('utf-8').strip()}"\n')
            raise shared.DDError(f'dd failed on index {i} with status {p.returncode}')

        speedCalculator.endOfCycle(partSize)

    sys.stdout.write("\nRestore completed\n")

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Iteratively backup file or device to multi-part file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('backup', help="Folder containing multi-part backup")
    parser.add_argument('dest', help="Destination file or device")
    parser.add_argument('-f', '--fernet', help='Passphrase for decrypting backups using Fernet.', type=str, default=None)
    parser.add_argument('-c', '--clarify', help='Passphrase for clarifying backups using an obfuscation algorithm.', type=str, default=None)
    parser.add_argument('-bs', '--block-size', help='Block size for dd and comparing files. Uses same format for sizes '
                        'as dd. Defaults to 1MB.', type=str, default=str(1024*1024))
    parser.add_argument('-H', '--hash', type=int, default=256, help='SHA hash level to use (1, 256, 384, 512).')
    parser.add_argument('-s', '--start', help='Index of starting part', type=str, default=str(0))
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-u', '--uuid', help='Indicates destination is a partition UUID', action='store_true')
    args = parser.parse_args()

    if args.hash not in [1, 256, 384, 512]:
        sys.stderr.write("Error: Hash level must be one of 1, 256, 384, or 512.\n")
        sys.exit(1)

    try:
        shared._outputStatusDontReplaceLine = args.verbose

        d = vars(args)
        d['startPartIndex'] = int(args.start)
        d['blockSize'] = shared.humanReadableSizeToBytes(args.block_size)

        restore(args)

        return 0
    except (shared.DDError, shared.BackupDataError) as e:
        sys.stderr.write(f'Error: {e}\n')
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)
