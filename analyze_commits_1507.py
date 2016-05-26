#! /usr/bin/env python

from __future__ import print_function

import sys
import argparse
import re
import csv

def print_error(msg):
    print('ERROR: {0}'.format(msg), file=sys.stderr)

def add_commit_time_bucket(buckets, txn_vals):
    took_ms = txn_vals['took_ms']
    flushed_kb = txn_vals['flushed_kb']

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

def add_raw_commit_stats(stats, txn, txn_vals):
    old_txn_vals = stats.get(txn)
    if old_txn_vals is not None:
        print_error('Already seen txn={0}, old_stats:{1}, new_stats:{2}'.format(txn, old_txn_vals, txn_vals))
    else:
        stats[txn] = txn_vals

def add_commit(res, txn, txn_vals):
    add_commit_time_bucket(res['commit_time_buckets'], txn_vals)
    add_raw_commit_stats(res['raw_commit_stats'], txn, txn_vals)

def add_snap_deletion(res, root, start_secs, end_secs):
    snap_del_vals = { 'root' : root,
                      'start_secs' : start_secs,
                      'took_secs' : end_secs - start_secs
                      }
    stats = res['snap_deletion_stats']
    next_idx = len(stats)
    stats[next_idx] = snap_del_vals

COMMIT_LINE_RE1 = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+txn\[(\d+)\] took (\d+) ms .+open=(\d+)ms .+flushed=(\d+) Kb')
COMMIT_LINE_RE2 = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+txn\[(\d+)\] .+rdr1:(\d+) .+rdr2:(\d+)')
COMMIT_LINE_RE3 = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+txn\[(\d+)\] .+rdr3:(\d+)')
SNAP_DELETE_START_RE = re.compile(r'^.+kernel: \[\s*(\d+)\.\d+\s*\].+btrfs_drop_snapshot.+starting DELETION of root=\((\d+),(\d+)\)')
SNAP_DELETE_END_RE = re.compile(r'^.+kernel: \[\s*(\d+)\.\d+\s*\].+btrfs_drop_snapshot.+root=\((\d+),(\d+)\) DELETED err=')

def analyze(res, fin):
    curr_txn = None
    curr_txn_vals = {}
    curr_deleting_root = None
    curr_deleting_root_start_secs = None

    for line in fin:
        m = COMMIT_LINE_RE1.match(line)
        if m is not None:
            txn = int(m.group(1))
            if curr_txn is not None:
                print_error('Seeing COMMIT_LINE_RE1 for txn={0} while still processing txn={1}'.format(txn, curr_txn))
                curr_txn_vals = {}
            curr_txn = txn
            curr_txn_vals['took_ms'] = int(m.group(2))
            curr_txn_vals['open_ms'] = int(m.group(3))
            curr_txn_vals['flushed_kb'] = int(m.group(4))
            continue
        m = COMMIT_LINE_RE2.match(line)
        if m is not None:
            txn = int(m.group(1))
            if curr_txn is not None and curr_txn == txn:
                curr_txn_vals['rdr1_ms'] = int(m.group(2))
                curr_txn_vals['rdr2_ms'] = int(m.group(3))
            else:
                print_error('Seeing COMMIT_LINE_RE2 for txn={0} while curr_txn={1}'.format(txn, curr_txn))
                curr_txn = None
                curr_txn_vals = {}
            continue
        m = COMMIT_LINE_RE3.match(line)
        if m is not None:
            txn = int(m.group(1))
            if curr_txn is not None and curr_txn == txn:
                curr_txn_vals['rdr3_ms'] = int(m.group(2))
                if len(curr_txn_vals) == 6:
                    add_commit(res, curr_txn, curr_txn_vals)
                else:
                    print_error('Not all values collected for txn={0}: {1}'.format(curr_txn, curr_txn_vals))
            else:
                print_error('Seeing COMMIT_LINE_RE3 for txn={0} while curr_txn={1}'.format(txn, curr_txn))
            curr_txn = None
            curr_txn_vals = {}
            continue
        m = SNAP_DELETE_START_RE.match(line)
        if m is not None:
            deleting_root = m.group(2) + "-" + m.group(3)
            if curr_deleting_root is not None:
                print_error('Seeing deletion of root={0} to begin, while root={1} is still deleting'.format(deleting_root, curr_deleting_root))
            curr_deleting_root = deleting_root
            curr_deleting_root_start_secs = int(m.group(1))
            continue
        m = SNAP_DELETE_END_RE.match(line)
        if m is not None:
            deleting_root = m.group(2) + "-" + m.group(3)
            end_secs = int(m.group(1))
            if curr_deleting_root is None or curr_deleting_root != deleting_root:
                print_error('Seeing deletion of root={0} to end, but we are now deleting root={1}'.format(deleting_root, curr_deleting_root))
            elif curr_deleting_root_start_secs > end_secs:
                print_error('Deletion of root={0} started at {1}, but ended at {2}'.format(curr_deleting_root, curr_deleting_root_start_secs, end_secs))
            else:
                add_snap_deletion(res, curr_deleting_root, curr_deleting_root_start_secs, end_secs)
            curr_deleting_root = None
            curr_deleting_root_start_secs = None
            continue

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
    writer.writerow(('txn', 'took_ms', 'open_ms', 'flushed_kb', 'rdr1_ms', 'rdr2_ms', 'rdr3_ms'))

    for txn in sorted(stats.iterkeys()):
        txn_vals = stats[txn]
        writer.writerow((txn, txn_vals['took_ms'], txn_vals['open_ms'], txn_vals['flushed_kb'], txn_vals['rdr1_ms'], txn_vals['rdr2_ms'], txn_vals['rdr3_ms']))

def output_snap_deletion_stats(stats, fout):
    writer = csv.writer(fout)
    writer.writerow(('idx', 'start_secs', 'took_secs', 'root'))

    for idx in sorted(stats.iterkeys()):
        snap_del_vals = stats[idx]
        writer.writerow((idx, snap_del_vals['start_secs'], snap_del_vals['took_secs'], snap_del_vals['root']))

# MAIN #########################################
def main():
    parser = argparse.ArgumentParser(description='Analyze BTRFS commits size and time')
    parser.add_argument('-o', '--output-file', required=True, help='filename to store the CSV output')
    parser.add_argument('input_files', nargs='+', help='file(s) to be analyzed')
    opts = parser.parse_args()

    res = { 'commit_time_buckets' : {},
            'raw_commit_stats' : {},
            'snap_deletion_stats' : {},
          }

    exit_code = 0
    fins = []

    try:
        for input_filename in opts.input_files:
            print('Opening input file: {0}'.format(input_filename))
            fin = open(input_filename, 'r')
            fins.append(fin)
        for fin in fins:
            analyze(res, fin)
        output_filename = opts.output_file + '.commit_time_buckets.csv'
        with open(output_filename, 'w') as fout:
            output_commit_time_buckets(res['commit_time_buckets'], fout)
        output_filename = opts.output_file + '.raw_commit_stats.csv'
        with open(output_filename, 'w') as fout:
            output_raw_commit_stats(res['raw_commit_stats'], fout)
        output_filename = opts.output_file + '.snap_deletion_stats.csv'
        with open(output_filename, 'w') as fout:
            output_snap_deletion_stats(res['snap_deletion_stats'], fout)
    except Exception:
        raise
    finally:
        for fin in fins:
            fin.close()

    sys.exit(exit_code)

main()


