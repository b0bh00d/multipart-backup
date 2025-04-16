import os
import sys
import time
import uuid
import ctypes
import platform
import subprocess

from typing import List

_outputStatusLastSize = 0
_outputStatusDontReplaceLine = False

class BackupDataError(Exception):
    pass

class DDError(Exception):
    pass

class BackupError(Exception):
    pass

class UnimplementedPlatformError(Exception):
    pass

class AverageSpeedCalculator():
    """Class for calculating average copy speed of several copy operations"""
    def __init__(self, maxSamples: int) -> None:
        self.startTime = None
        self.currentAverageSpeed = None
        self.maxSamples = maxSamples
        self.timingList = []
        self.bytesCopiedList = []

    def startOfCycle(self) -> None:
        self.startTime = time.time()

    def endOfCycle(self, bytesCopied: int) -> None:
        self.timingList.append(time.time()-self.startTime)
        self.bytesCopiedList.append(bytesCopied)
        self.timingList = self.timingList[-self.maxSamples:]
        self.bytesCopiedList = self.bytesCopiedList[-self.maxSamples:]
        self.currentAverageSpeed = sum(self.bytesCopiedList) / sum(self.timingList)

    def averageSpeed(self) -> float:
        return self.currentAverageSpeed

def outputStatus(value: str) -> None:
    """Prints a line to the console that overwrites the previous line, allowing for status updates."""
    if _outputStatusDontReplaceLine:
        sys.stdout.write(f'{value}\n')
        return

    global _outputStatusLastSize

    if len(value) < _outputStatusLastSize:
        value = value + (' ' * (_outputStatusLastSize-len(str)))

    sys.stdout.write(f'{value}\r')
    sys.stdout.flush()
    _outputStatusLastSize = len(value)

def humanReadableSize(bytes: int) -> str:
    """Returns a nicer human readable representation of the given size in bytes"""
    if bytes < 1024:
        return f'{bytes}b'
    elif bytes < (1024*1024):
        return f'{bytes / 1024:.1f}K'
    elif bytes < (1024*1024*1024):
        return f'{bytes / (1024*1024):.1f}M'
    else:
        return f'{bytes / (1024*1024*1024):.1f}G'

def humanReadableSizeToBytes(value: str) -> int:
    """Converts a human readable size value into an exact number of bytes. Uses
    the same format as dd."""
    validSuffixes = {'b':512, 'k':1024, 'm':1048576, 'g':1073741824, 'w':ctypes.sizeof(ctypes.c_int)}
    value = value.lower().strip()

    if value[-1] in validSuffixes:
        numberPart = value[:-1]
        suffix = value[-1]
    else:
        numberPart = value
        suffix = None

    if numberPart.startswith('0x'):
        number = int(numberPart, 16)
    elif numberPart.startswith('0'):
        number = int(numberPart, 8)
    else:
        number = int(numberPart, 10)

    if suffix is None:
        return number
    else:
        return number * validSuffixes[suffix]

def isPartFile(filename: str) -> bool:
    return len(filename) == 13 and filename.startswith('part_') and filename[-8:].isdigit()

def isEncryptedFile(filename: str) -> bool:
    return filename.endswith('.enc') and filename[-12:-8].isdigit()

def isObfuscatedFile(filename: str) -> bool:
    return filename.endswith('.obf') and filename[-12:-8].isdigit()

def partsInSnapshot(dest: str, variant: str = None) -> List[str]:
    match variant:
        case None:
            return sorted(list(filter(isPartFile, os.listdir(dest))))
        case 'encrypted':
            return sorted(list(filter(isEncryptedFile, os.listdir(dest))))
        case 'obfuscated':
            return sorted(list(filter(isObfuscatedFile, os.listdir(dest))))

    return []

def normalizeUUID(uuidString: str) -> str:
    return str(uuid.UUID(uuidString)).lower()

def deviceIdentifierForSourceString(source: str, sourceIsUUID: bool) -> str | None:
    if sourceIsUUID:
        result = findDiskDeviceIdentifierByUUID(source)

        if result is None:
            raise ValueError(f'Could not find a partition with UUID: {source}')

        return result

    elif os.path.exists(source):
        return source
    else:
        raise ValueError(f'"{source}" is not a valid device identifier or file')

def findDiskDeviceIdentifierByUUIDLinux(uuidString: str) -> str | None:
    # https://stackoverflow.com/questions/5080402/python-subprocess-module-how-do-i-give-input-to-the-first-of-series-of-piped-co
    import threading

    first = subprocess.Popen(['blkid'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    second = subprocess.Popen(['grep', uuidString], stdin=first.stdout, stdout=subprocess.PIPE)
    first.stdout.close()
    first.stdout = None

    threading.Thread(target=first.communicate).start()

    # get output from the second command at the same time
    output = second.communicate()[0].decode('utf-8').strip()
    if len(output):
        device, artrib = output.split(':')
        return device
        # options = artrib.split()
        # print(device)
        # print(options)

    return None

def findDiskDeviceIdentifierByUUIDMacOS(uuidString: str) -> str | None:
    import plistlib

    diskUtilPlistData = subprocess.check_output(['diskutil', 'list', '-plist'])
    diskUtilData = plistlib.readPlistFromString(diskUtilPlistData)
    allDisksAndPartitions = diskUtilData['AllDisksAndPartitions']

    def findDiskUUIDInList(partitionList, targetUUIDString):
        for partition in partitionList:
            matches = (('DiskUUID' in partition and partition['DiskUUID'].lower() == targetUUIDString) or
                       ('VolumeUUID' in partition and partition['VolumeUUID'].lower() == targetUUIDString))
            if matches:
                # Want to provide the unbuffered device identifier for better performance, hence the r
                return f'/dev/r{partition['DeviceIdentifier']}'

        return None

    for data in allDisksAndPartitions:
        if 'Partitions' in data:
            result = findDiskUUIDInList(data['Partitions'], uuidString)

            if result is not None:
                return result

        if 'APFSVolumes' in data:
            result = findDiskUUIDInList(data['APFSVolumes'], uuidString)

            if result is not None:
                return result

    return None

def findDiskDeviceIdentifierByUUID(uuidString: str) -> str | None:
    uuidString = normalizeUUID(uuidString)

    match platform.system():
        case 'Darwin':
            return findDiskDeviceIdentifierByUUIDMacOS(uuidString)
        case 'Linux':
            return findDiskDeviceIdentifierByUUIDLinux(uuidString)
        case _:
            raise UnimplementedPlatformError(f'Finding a device by UUID is not implemented for platform: {platform.system()}')

def isUUID(uuidString: str) -> bool:
    try:
        uuid.UUID(uuidString)
        return True
    except ValueError:
        return False
