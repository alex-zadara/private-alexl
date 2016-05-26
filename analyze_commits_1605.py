#! /usr/bin/env python

from __future__ import print_function

import sys
import argparse
import re
import csv

def print_error(msg):
    print('ERROR: {0}'.format(msg), file=sys.stderr)

# ANALYZE #####################################################
def get_fs_stats(res, dev):
    fs_stats = res.get(dev)
    if fs_stats is None:
        print('Initializing stats for FS[{0}]'.format(dev))
        curr_values = { 'curr_txn' : None,
                        'curr_txn_vals' : {},
                        'curr_deleting_root' : None,
                        'curr_deleting_root_start_secs' : None
                       }
        fs_stats = { 'commit_time_buckets' : {},
                  'raw_commit_stats' : {},
                  'snap_deletion_stats' : {},
                  'curr_values' : curr_values,
                }
        res[dev] = fs_stats
    return fs_stats

def get_fs_curr_values(fs_stats):
    curr_values = fs_stats['curr_values']
    return curr_values['curr_txn'], curr_values['curr_txn_vals'], curr_values['curr_deleting_root'], curr_values['curr_deleting_root_start_secs']

def update_fs_curr_values(fs_stats, curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs):
    curr_values = fs_stats['curr_values']
    curr_values['curr_txn'] = curr_txn
    curr_values['curr_txn_vals'] = curr_txn_vals
    curr_values['curr_deleting_root'] = curr_deleting_root
    curr_values['curr_deleting_root_start_secs'] = curr_deleting_root_start_secs

def add_commit_time_bucket(buckets, txn_vals):
    took_ms = txn_vals['took_ms']
    flushed_kb = txn_vals['flushed_total_kb']

    # buckets will be: 1Mb, 4MB, 8MB, 12MB, etc
    # each bucket contains commits up to (and including) that size

    # special treatment for <= 1MB
    if flushed_kb <= 1024:
        bucket = 1
    # special treatment for nice values to put them into the same bucket
    elif flushed_kb % 4096 == 0:
        bucket = flushed_kb / 1024
    else:
        bucket = ((flushed_kb / 4096) + 1) * 4
    bucket_data = buckets.get(bucket)
    if bucket_data is None:
        bucket_data = { 'num_commits' : 0, 'total_commit_time_ms' : 0, 'max_commit_time_ms' : 0}
        buckets[bucket] = bucket_data

    bucket_data['num_commits'] = bucket_data['num_commits'] + 1
    bucket_data['total_commit_time_ms'] = bucket_data['total_commit_time_ms'] + took_ms
    if took_ms > bucket_data['max_commit_time_ms']:
        bucket_data['max_commit_time_ms'] = took_ms

def add_raw_commit_stats(dev, raw_commit_stats, txn, txn_vals):
    old_txn_vals = raw_commit_stats.get(txn)
    if old_txn_vals is not None:
        print_error('Already seen FS[{0} txn={1} old_vals:{2}, new_vals:{3}'.format(dev, txn, old_txn_vals, txn_vals))
    else:
        raw_commit_stats[txn] = txn_vals

def add_commit(dev, fs_stats, txn, txn_vals):
    add_commit_time_bucket(fs_stats['commit_time_buckets'], txn_vals)
    add_raw_commit_stats(dev, fs_stats['raw_commit_stats'], txn, txn_vals)

def add_snap_deletion(fs_stats, root, start_secs, end_secs):
    snap_del_vals = { 'root' : root,
                      'start_secs' : start_secs,
                      'took_secs' : end_secs - start_secs
                      }
    stats = fs_stats['snap_deletion_stats']
    next_idx = len(stats)
    stats[next_idx] = snap_del_vals

LINUX_VERSION_RE = re.compile(r'(^.+)kernel:.+Linux version')
UNMOUNT_RE = re.compile(r'(^.+)kernel:.+zbtrfs_fs_info_fini.+FS\[([^]]+)\]: FINI')
COMMIT_LINE_RE1 = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+FS\[([^]]+)\]: txn\[(\d+)\] took (\d+)ms .+open=(\d+)ms .+read=(\d+)KB flushed=(\d+)/(\d+)Kb')
COMMIT_LINE_RE2 = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+FS\[([^]]+)\]: txn\[(\d+)\] rdr1:(\d+) .+rdr2:(\d+)')
COMMIT_LINE_RE3 = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+FS\[([^]]+)\] txn\[(\d+)\] .+rdr3:(\d+)')
SNAP_DELETE_START_RE = re.compile(r'^.+kernel: \[\s*(\d+)\.\d+\s*\].+btrfs_drop_snapshot.+FS\[([^]]+)\]: starting DELETION of root=\((\d+),(\d+)\)')
SNAP_DELETE_END_RE = re.compile(r'^.+kernel: \[\s*(\d+)\.\d+\s*\].+btrfs_drop_snapshot.+FS\[([^]]+)\]: root=\((\d+),(\d+)\) DELETED err=')

def analyze(opts, fin, res):
    curr_txn = None
    curr_txn_vals = None
    curr_deleting_root = None
    curr_deleting_root_start_secs = None

    for line in fin:
        m = LINUX_VERSION_RE.match(line)
        if m is not None:
            print('{0}: Reboot/crash seen, flush all stats'.format(m.group(1)))
            output_stats(opts, res, None)
            res.clear()
            continue
        m = UNMOUNT_RE.match(line)
        if m is not None:
            dev = m.group(2)
            fs_stats = get_fs_stats(res, dev)
            print('{0}: Unmount/delete FS[{1}] seen, flush stats'.format(m.group(1), dev))
            output_stats(opts, res, dev)
            del res[dev]

        m = COMMIT_LINE_RE1.match(line)
        if m is not None:
            dev = m.group(1)
            fs_stats = get_fs_stats(res, dev)
            curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs = get_fs_curr_values(fs_stats)
            txn = int(m.group(2))
            if curr_txn is not None:
                print_error('Seeing COMMIT_LINE_RE1 for FS[{0}] txn={1} while still processing txn={2}'.format(dev, txn, curr_txn))
                curr_txn_vals = {}
            curr_txn = txn
            curr_txn_vals['took_ms'] = int(m.group(3))
            curr_txn_vals['open_ms'] = int(m.group(4))
            curr_txn_vals['read_kb'] = int(m.group(5))
            curr_txn_vals['flushed_commit_kb'] = int(m.group(6))
            curr_txn_vals['flushed_total_kb'] = int(m.group(7))
            update_fs_curr_values(fs_stats, curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs)
            continue

        m = COMMIT_LINE_RE2.match(line)
        if m is not None:
            dev = m.group(1)
            fs_stats = get_fs_stats(res, dev)
            curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs = get_fs_curr_values(fs_stats)
            txn = int(m.group(2))
            if curr_txn is not None and curr_txn == txn:
                curr_txn_vals['rdr1_ms'] = int(m.group(3))
                curr_txn_vals['rdr2_ms'] = int(m.group(4))
            else:
                print_error('Seeing COMMIT_LINE_RE2 for FS[{0}] txn={1} while curr_txn={2}'.format(dev, txn, curr_txn))
                curr_txn = None
                curr_txn_vals = {}
            update_fs_curr_values(fs_stats, curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs)
            continue

        m = COMMIT_LINE_RE3.match(line)
        if m is not None:
            dev = m.group(1)
            fs_stats = get_fs_stats(res, dev)
            curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs = get_fs_curr_values(fs_stats)
            txn = int(m.group(2))
            if curr_txn is not None and curr_txn == txn:
                curr_txn_vals['rdr3_ms'] = int(m.group(3))
                if len(curr_txn_vals) == 8:
                    add_commit(dev, fs_stats, curr_txn, curr_txn_vals)
                else:
                    print_error('Not all values collected for FS[{0}] txn={1}: {2}'.format(dev, curr_txn, curr_txn_vals))
            else:
                print_error('Seeing COMMIT_LINE_RE3 for FS[{0}] txn={1} while curr_txn={2}'.format(dev, txn, curr_txn))
            curr_txn = None
            curr_txn_vals = {}
            update_fs_curr_values(fs_stats, curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs)
            continue

        m = SNAP_DELETE_START_RE.match(line)
        if m is not None:
            dev = m.group(2)
            fs_stats = get_fs_stats(res, dev)
            curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs = get_fs_curr_values(fs_stats)
            deleting_root = m.group(3) + "-" + m.group(4)
            if curr_deleting_root is not None:
                print_error('Seeing deletion of FS[{0}] root={1} to begin, while root={2} is still deleting'.format(dev, deleting_root, curr_deleting_root))
            curr_deleting_root = deleting_root
            curr_deleting_root_start_secs = int(m.group(1))
            update_fs_curr_values(fs_stats, curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs)
            continue

        m = SNAP_DELETE_END_RE.match(line)
        if m is not None:
            dev = m.group(2)
            fs_stats = get_fs_stats(res, dev)
            curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs = get_fs_curr_values(fs_stats)
            deleting_root = m.group(3) + "-" + m.group(4)
            end_secs = int(m.group(1))
            if curr_deleting_root is None or curr_deleting_root != deleting_root:
                print_error('Seeing deletion of FS[{0}] root={1} to end, but we are now deleting root={2}'.format(dev, deleting_root, curr_deleting_root))
            elif curr_deleting_root_start_secs > end_secs:
                print_error('Deletion of FS[{0}] root={1} started at {2}, but ended at {3}'.format(dev, curr_deleting_root, curr_deleting_root_start_secs, end_secs))
            else:
                add_snap_deletion(fs_stats, curr_deleting_root, curr_deleting_root_start_secs, end_secs)
            curr_deleting_root = None
            curr_deleting_root_start_secs = None
            update_fs_curr_values(fs_stats, curr_txn, curr_txn_vals, curr_deleting_root, curr_deleting_root_start_secs)
            continue

# OUTPUT #######################################################

# Across reboots/passivations, same device name may indicate different
# btrfs pools.
# We have a running index to make filenames unique
OUTPUT_IDX = 0
def output_stats(opts, res, dev):
    global OUTPUT_IDX
    if dev is not None:
        output_stats_for_dev(opts.output_file_prefix, OUTPUT_IDX, dev, res[dev])
        OUTPUT_IDX += 1
    else:
        for dev in res.iterkeys():
            output_stats_for_dev(opts.output_file_prefix, OUTPUT_IDX, dev, res[dev])
            OUTPUT_IDX += 1
    
def output_stats_for_dev(prefix, idx, dev, fs_stats):
    output_filename = '{0}.{1:04}.{2}.commit_time_buckets.csv'.format(prefix, idx, dev)
    with open(output_filename, 'w') as fout:
        output_commit_time_buckets(fs_stats['commit_time_buckets'], fout)
    output_filename = '{0}.{1:04}.{2}.raw_commit_stats.csv'.format(prefix, idx, dev)
    with open(output_filename, 'w') as fout:
        output_raw_commit_stats(fs_stats['raw_commit_stats'], fout)
    output_filename = '{0}.{1:04}.{2}.snap_deletion_stats.csv'.format(prefix, idx, dev)
    with open(output_filename, 'w') as fout:
        output_snap_deletion_stats(fs_stats['snap_deletion_stats'], fout)

def output_commit_time_buckets(buckets, fout):
    writer = csv.writer(fout)
    writer.writerow(('commit_size_mb', 'num_commits', 'avg_commit_time_ms', 'max_commit_time_ms'))

    for bucket in sorted(buckets.iterkeys()):
        bucket_data = buckets[bucket]
        num_commits = bucket_data['num_commits']
        avg_time_ms = bucket_data['total_commit_time_ms'] / num_commits
        max_time_ms = bucket_data['max_commit_time_ms']
        writer.writerow((bucket, num_commits, avg_time_ms, max_time_ms))

def output_raw_commit_stats(stats, fout):
    writer = csv.writer(fout)
    writer.writerow(('txn', 'took_ms', 'open_ms', 'read_kb', 'flushed_commit_kb', 'flushed_total_kb', 'rdr1_ms', 'rdr2_ms', 'rdr3_ms'))

    for txn in sorted(stats.iterkeys()):
        txn_vals = stats[txn]
        writer.writerow((txn, txn_vals['took_ms'], txn_vals['open_ms'], txn_vals['read_kb'],
                         txn_vals['flushed_commit_kb'], txn_vals['flushed_total_kb'],
                         txn_vals['rdr1_ms'], txn_vals['rdr2_ms'], txn_vals['rdr3_ms']))

def output_snap_deletion_stats(stats, fout):
    writer = csv.writer(fout)
    writer.writerow(('idx', 'start_secs', 'took_secs', 'root'))

    for idx in sorted(stats.iterkeys()):
        snap_del_vals = stats[idx]
        writer.writerow((idx, snap_del_vals['start_secs'], snap_del_vals['took_secs'], snap_del_vals['root']))

# MAIN #########################################
def main():
    parser = argparse.ArgumentParser(description='Analyze BTRFS commits size and time')
    parser.add_argument('-o', '--output-file-prefix', required=True, help='prefix for filenames to store the CSV output')
    parser.add_argument('input_files', nargs='+', help='file(s) to be analyzed')
    opts = parser.parse_args()

    res = { }
    fin = None

    try:
        for input_filename in opts.input_files:
            print('=== Opening input file: {0} ==='.format(input_filename))
            fin = open(input_filename, 'r')
            analyze(opts, fin, res)
            fin.close()
            fin = None
        # Output all we have got
        output_stats(opts, res, None)
    except Exception:
        raise
    finally:
        if fin is not None:
            fin.close()

    sys.exit(0)

main()


