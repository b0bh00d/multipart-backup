#!/usr/bin/env python3

import os
import re
import sys
import datetime
import argparse
import subprocess

import shared

from typing import Tuple, List

_nullBlock = '\0'

def isFileAllZeros(path: str, blockSize: int) -> bool:
    """Returns true if the file at path contains no data other than 0. Will check data in increments of blockSize."""
    global _nullBlock

    # Quick optimization so that we don't have to recreate _nullBlock more than necessary
    if len(_nullBlock) != blockSize:
        _nullBlock = '\0' * blockSize

    result = False

    with open(path, 'rb') as f:
        while True:
            block = f.read(blockSize)

            if len(block) == 0:
                break

            result = True

            if (len(block) == blockSize) and (block != _nullBlock):
                return False
            elif block != ('\0' * len(block)):
                return False

    return result

def areFilesIdentical(path1: str, path2: str, blockSize: int) -> bool:
    """Returns true if both files contain identical data. Will check data in increments of blockSize."""
    with open(path1, 'rb') as f1:
        with open(path2, 'rb') as f2:
            while True:
                block1 = f1.read(blockSize)
                block2 = f2.read(blockSize)

                if block1 != block2:
                    return False

                if len(block1) == 0:
                    break

    return True

def partPathAtIndex(dest: str, index: int) -> str:
    """Returns the path of a backup part for the given backup destination and index"""
    return os.path.join(dest, f'part_{index:08d}')

def newPartPathAtIndex(dest: str, index: int) -> str:
    """Returns the path of a newly created backup part for the given backup destination and index. A new part has not
    yet been compared to an existing part to see if they're identical or if the new part contains all zeros"""
    return os.path.join(dest, f'part_{index:08d}.new')

def copyPartToDisk(source: str, dest: str, partSize: int, blockSize: int, index: int, speedCalculator) -> Tuple[str | None, int]:
    """Copies source into dest in partSize chunks. Returns the path of the newly created part, or None if the part
    was within partSize-1 bytes of the end of source and there are no more parts to copy."""
    partBlockCount = partSize // blockSize
    partPath = newPartPathAtIndex(dest, index)

    if speedCalculator.averageSpeed() is not None:
        shared.outputStatus(f"Copying part {index+1} ... speed: {shared.humanReadableSize(speedCalculator.averageSpeed())}/sec")
    else:
        shared.outputStatus(f"Copying part {index+1} ...")

    p = subprocess.Popen(['dd', 'if=%s' % source, 'of=%s' % partPath, 'bs=%s' % blockSize,
               'count=%s' % partBlockCount, 'skip=%s' % (index*partBlockCount)],
              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    if p.returncode != 0:
        sys.stderr.write(f'dd failed! Output:\n{err}\n')
        raise shared.DDError(f'dd failed on index {index} with status {p.returncode}')

    newPartSize = os.stat(partPath).st_size

    # If the part size is zero, that means we've gone past the end of the file or device we're copying and
    # we need to stop
    if newPartSize == 0:
        os.remove(partPath)
        return (None, 0)
    else:
        return (partPath, newPartSize)

def compareNewPart(newPartPath: str, partSize: int, blockSize: int, keepNullParts: bool) -> bool:
    """Compares a freshly completed part to the previously existing part (if one exists) as well as checking
    if the part is all zeros"""
    def areOldAndNewPartsIdentical(prevPartPath, newPartPath, newPartIsAllZeros):
        newPartSize = os.stat(newPartPath).st_size
        prevPartSize = os.stat(prevPartPath).st_size

        if not keepNullParts and prevPartSize == 0 and newPartIsAllZeros:
            return True
        else:
            result = areFilesIdentical(prevPartPath, newPartPath, blockSize)
            return result

    newPartIsAllZeros = isFileAllZeros(newPartPath, blockSize)
    prevPartPath = os.path.splitext(newPartPath)[0]

    if os.path.exists(prevPartPath):
        if areOldAndNewPartsIdentical(prevPartPath, newPartPath, newPartIsAllZeros):
            os.remove(newPartPath)
            return False
        else:
            os.remove(prevPartPath)

    os.rename(newPartPath, prevPartPath)

    # Only want to consider files that are of size partSize
    if os.stat(prevPartPath).st_size == partSize and not keepNullParts and newPartIsAllZeros:
        # Blank out file, signaling that its size is blockSize and it is all zeros
        with open(prevPartPath, 'wb') as _:
            pass

    return True

def removeExcessPartsInDestStartingAtIndex(dest: str, index: int) -> int:
    """Used to remove parts that are no longer needed for the given backup destination."""
    deletedFiles = 0

    while os.path.exists(partPathAtIndex(dest, index)):
        os.remove(partPathAtIndex(dest, index))
        index += 1
        deletedFiles += 1

    return deletedFiles

def snapshotTimestamp() -> str:
    return f"snapshot-{datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")}"

def inProgressSnapshotName() -> str:
    return 'snapshot-inprogress'

def isSnapshotDir(dirName: str) -> bool:
    return (dirName == inProgressSnapshotName() or
            re.search(r"^snapshot-\d{4}-\d{2}-\d{2}-\d{6}", dirName) is not None)

def previousSnapshots(destRoot: str) -> List[str]:
    return list(map(lambda x: os.path.join(destRoot, x),
               sorted(list(filter(isSnapshotDir,os.listdir(destRoot))))))

def findIncompleteSnapshot(snapshots):
    incompletes = list(filter(lambda x: os.path.basename(x) == inProgressSnapshotName(), snapshots))

    if len(incompletes) > 0:
        return incompletes[0]
    else:
        return None

def createNewSnapshot(destRoot: str) -> str:
    dest = os.path.join(destRoot, inProgressSnapshotName())
    os.mkdir(dest)
    return dest

def createNewSnapshotWithLinksToOld(destRoot: str, lastSnapshot) -> str:
    dest = createNewSnapshot(destRoot)

    for part in shared.partsInSnapshot(lastSnapshot):
        os.link(os.path.join(lastSnapshot, part), os.path.join(dest, part))

    return dest

def setupAndReturnDestination(destRoot: str, snapshotCount: int) -> str:
    """If snapshotCount > 0, either returns a new snapshot containing hard links to the previous snapshot's parts, or
    returns an existing in-progress snapshot. If snapshotCount is 0, then returns destRoot."""
    if not os.path.exists(destRoot):
        os.makedirs(destRoot, exist_ok=True)

    if snapshotCount > 0:
        prevs = previousSnapshots(destRoot)
        incompleteSnapshot = findIncompleteSnapshot(prevs)

        if incompleteSnapshot is not None:
            sys.stdout.write("NOTE: last snapshot is complete! Will attempt to "
                             "finish it...\n")
            dest = incompleteSnapshot
        elif len(prevs) > 0:
            sys.stdout.write("Setting up new snapshot...\n")
            dest = createNewSnapshotWithLinksToOld(destRoot, prevs[-1])
        else:
            dest = createNewSnapshot(destRoot)
    else:
        dest = destRoot

    return dest

def removeEmptyDirectoryEvenIfItHasAnAnnoyingDSStoreFileInIt(dir: str) -> None:
    try:
        os.rmdir(dir)
        return
    except OSError:
        if os.path.exists(os.path.join(dir, '.DS_Store')):
            os.remove(os.path.join(dir, '.DS_Store'))

            try:
                os.rmdir(dir)
            except OSError:
                pass

def removeOldSnapshots(destRoot: str, snapshotCount: int) -> None:
    """If the backup at the given root folder contains more snapshots than snapshotCount, removes the oldest extra
    snapshots."""
    prevs = previousSnapshots(destRoot)
    snapshotsToRemove = prevs[:-snapshotCount]

    if len(snapshotsToRemove) > 0:
        sys.stdout.write("Removing old snapshots...\n")

        for oldSnapshot in snapshotsToRemove:
            for part in shared.partsInSnapshot(oldSnapshot):
                os.remove(os.path.join(oldSnapshot, part))

            removeEmptyDirectoryEvenIfItHasAnAnnoyingDSStoreFileInIt(oldSnapshot)

def renameSnapshotToFinalName(dest: str) -> None:
    os.rename(dest, os.path.join(os.path.dirname(dest), snapshotTimestamp()))

def deviceIdentifierForSourceString(source: str, sourceIsUUID: bool) -> str | None:
    if sourceIsUUID:
        result = shared.findDiskDeviceIdentifierByUUID(source)

        if result is None:
            raise ValueError(f'Could not find a partition with UUID: {source}')

        return result

    elif os.path.exists(source):
        return source
    else:
        raise ValueError(f'"{source}" is not a valid device identifier or file')

def backup(sourceString: str, sourceIsUUID: bool, destRoot: str, partSize: int, blockSize: int, keepNullParts: bool, snapshotCount: int) -> None:
    if partSize % blockSize != 0:
        raise ValueError('Part size must be integer multiple of block size')

    source = deviceIdentifierForSourceString(sourceString, sourceIsUUID)
    dest = setupAndReturnDestination(destRoot, snapshotCount)
    speedCalculator = shared.AverageSpeedCalculator(5)

    partIndex = 0
    changedFiles = 0

    while True:
        speedCalculator.startOfCycle()
        newPartPath, newPartSize = copyPartToDisk(source, dest, partSize, blockSize, partIndex, speedCalculator)

        if newPartPath is None:
            break

        fileChanged = compareNewPart(newPartPath, partSize, blockSize, keepNullParts)

        if fileChanged:
            changedFiles += 1

        partIndex += 1
        speedCalculator.endOfCycle(partSize)

        if newPartSize != partSize:
            # We've hit the final part
            break

    deletedFiles = removeExcessPartsInDestStartingAtIndex(dest, partIndex)
    renameSnapshotToFinalName(dest)

    if snapshotCount > 0:
        removeOldSnapshots(destRoot, snapshotCount)

    sys.stdout.write("\n")
    sys.stdout.write(f"Finished! Changed files: {changedFiles + deletedFiles}\n")

def main() -> int:
    parser = argparse.ArgumentParser(description="Iteratively backup file or device to multi-part file")
    parser.add_argument('source', help="Source file, device identifier, or partition UUID")
    parser.add_argument('dest', help="Destination folder for multi-part backup")
    parser.add_argument('-bs', '--block-size', help='Block size for dd and comparing files. Uses same format for sizes '
                        'as dd. Defaults to 1 MB.', type=str, default=str(1024*1024))
    parser.add_argument('-ps', '--part-size', help='Size of each part of the backup. Uses same format for sizes as dd. '
                        'Defaults to 100 MB', type=str, default=str(100*1024*1024))
    parser.add_argument('-k', '--keep-null-parts', help='Keep parts that contain all zeros at full size',
                        action='store_true')
    parser.add_argument('-s', '--snapshots', type=int, default=4, help='Number of snapshots to maintain. Default is 4.')
    parser.add_argument('-u', '--uuid', help='Indicates source is a partition UUID', action='store_true')
    args = parser.parse_args()

    try:
        partSize = shared.humanReadableSizeToBytes(args.part_size)
        blockSize = shared.humanReadableSizeToBytes(args.block_size)
        backup(args.source, args.uuid, args.dest, partSize, blockSize, args.keep_null_parts, args.snapshots)
        return 0
    except (shared.DDError, ValueError, shared.BackupError) as e:
        sys.stderr.write(f'Error: {e}\n')
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)
