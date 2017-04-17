#!/usr/bin/env python

import sys
from pathlib import Path
import time
from multiprocessing import Pool
from functools import partial

import transfer


def get_new_files(folder, init_filelist=None):
    folder = Path(folder)
    if init_filelist is None:
        init_filelist = []
    return [f for f in folder.glob('**/*.dat')
            if (f.with_suffix('.yml').is_file() and f not in init_filelist)]


def complete_task(fname, dry_run=False):
    print('Completed processing for "%s" (callback)' % fname, flush=True)
    #dest = transfer.replace_basedir(fname, transfer.temp_basedir,
    #                                transfer.local_archive_basedir)
    #transfer.filecopy(fname, dest) # filecopy does not have a dry_run arg


def start_monitoring(folder, dry_run=False):
    title_msg = 'Monitoring %s' % folder.name
    print('\n\n%s' % title_msg)

    init_filelist = get_new_files(folder)

    print('- The following files are present at startup and will be skipped:')
    for f in init_filelist:
        print('  %s' % f)
    print()

    with Pool(processes=4) as pool:
        try:
            while True:
                transfer.timestamp()
                for i in range(20):
                    time.sleep(3)
                    newfiles = get_new_files(folder, init_filelist)
                    for newfile in newfiles:
                        pool.apply_async(transfer.process_int,
                                         (newfile, dry_run),
                                         callback=complete_task)
                    init_filelist += newfiles
        except KeyboardInterrupt:
            print('\n>>> Got keyboard interrupt.\n', flush=True)
    print('Closing subprocess pool.', flush=True)


def batch_process(folder, dry_run=False):
    assert folder.is_dir(), 'Path not found: %s' % folder

    title_msg = 'Monitoring %s' % folder.name
    print('\n\n%s' % title_msg)

    filelist = get_new_files(folder)

    print('- The following files will be processed in batch:')
    for f in filelist:
        print('  %s' % f)
    print()

    with Pool(processes=4) as pool:
        try:
            pool.starmap(transfer.process_int, [(f, dry_run) for f in filelist])
        except KeyboardInterrupt:
            print('\n>>> Got keyboard interrupt.\n', flush=True)
    print('Closing subprocess pool.', flush=True)


def help():
    msg = """
    monitor.py

    This script monitors a folder and converts DAT files to Photon-HDF5
    if a metadata YAML file with the same name (except extension) is found
    in the same folder.

    USAGE
    -----

    python monitor.py <folder> [--batch] [--dry-run]

    Optional Arguments:
        --batch
            Process all the DAT/YML files in the folder (batch-mode). Without
            this option only new files created after the monitor started are
            processed.
        --dry-run
            No processing (copy, conversion, analysis) is perfomed.
            Used for debugging.

    """
    print(msg)


if __name__ == '__main__':
    args = sys.argv[1:].copy()
    if len(args) == 0 or '-h' in args or '--help' in args:
        help()
        sys.exit(0)
    msg = '1 to 3 command-line arguments expected. Received %d instead.'
    assert 1 <= len(args) <= 3, msg % len(args)


    dry_run = False
    if '--dry-run' in args:
        dry_run = True
        complete_task = partial(complete_task, dry_run=dry_run)
        args.pop(args.index('--dry-run'))
    batch = False
    if '--batch' in args:
        batch = True
        args.pop(args.index('--batch'))
    assert len(args) == 1

    folder = Path(args[0])
    assert folder.is_dir(), 'Path not found: %s' % folder
    if batch:
        batch_process(folder, dry_run)
    else:
        start_monitoring(folder, dry_run)
    print('Monitor execution end.', flush=True)
