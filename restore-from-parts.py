#!/usr/bin/env python3

import os
import sys
import argparse
import subprocess

import shared

verbose = False

def checkPartsAndGetPartSize(backupPath: str, parts, blockSize: int) -> None:
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

def restore(backupPath: str, dest: str, blockSize: int, startPartIndex: int) -> None:
    parts = shared.partsInSnapshot(backupPath)
    backupPartSize = checkPartsAndGetPartSize(backupPath, parts, blockSize)

    if backupPartSize is None:
        raise shared.BackupDataError('Could not deduce part size... are all of your parts 0 bytes in size?')

    partBlockCount = backupPartSize // blockSize
    speedCalculator = shared.AverageSpeedCalculator(5)

    for i in range(startPartIndex, len(parts)):
        speedCalculator.startOfCycle()

        partPath = os.path.join(backupPath, parts[i])
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
            partPathToUse = partPath

        if verbose:
            p = subprocess.Popen(['dd', 'if=%s' % partPathToUse, 'of=%s' % dest, 'bs=%s' % blockSize, 'count=%s' % partBlockCount,
                      'oseek=%s' % (i*partBlockCount)])
            p.communicate()
        else:
            p = subprocess.Popen(['dd', 'if=%s' % partPathToUse, 'of=%s' % dest, 'bs=%s' % blockSize, 'count=%s' % partBlockCount,
                      'oseek=%s' % (i*partBlockCount)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()

        if p.returncode != 0:
            sys.stderr.write(f'dd failed! Output:\n{err}\n')
            raise shared.DDError(f'dd failed on index {i} with status {p.returncode}')

        speedCalculator.endOfCycle(partSize)

    sys.stdout.write("\nRestore completed\n")

def main() -> int:
    global verbose
    parser = argparse.ArgumentParser(description="Iteratively backup file or device to multi-part file")
    parser.add_argument('backup', help="Folder containing multi-part backup")
    parser.add_argument('dest', help="Destination file or device")
    parser.add_argument('-bs', '--block-size', help='Block size for dd and comparing files. Uses same format for sizes '
                        'as dd. Defaults to 1MB.', type=str, default=str(1024*1024))
    parser.add_argument('-s', '--start', help='Index of starting part', type=str, default=str(0))
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    try:
        verbose = args.verbose
        shared._outputStatusDontReplaceLine = verbose
        startPartIndex = int(args.start)

        blockSize = shared.humanReadableSizeToBytes(args.block_size)
        restore(args.backup, args.dest, blockSize, startPartIndex)
        return 0
    except (shared.DDError, shared.BackupDataError) as e:
        sys.stderr.write('Error: %s\n' % e.message)
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)
