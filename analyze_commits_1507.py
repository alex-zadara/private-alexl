#! /usr/bin/env python

from __future__ import print_function

import sys
import argparse
import re
import csv

COMMIT_LINE_RE = re.compile(r'^.+ZBTRFS_TXN_COMMIT_PHASE_DONE.+took (\d+) ms.+flushed=(\d+) Kb')

def add_commit(res, time_ms, flushed_kb):
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
    bucket_data = res.get(bucket)
    if bucket_data is None:
        bucket_data = { 'num_commits' : 0, 'total_commit_time_ms' : 0, 'max_commit_time_ms' : 0}
        res[bucket] = bucket_data

    bucket_data['num_commits'] = bucket_data['num_commits'] + 1
    bucket_data['total_commit_time_ms'] = bucket_data['total_commit_time_ms'] + time_ms
    if time_ms > bucket_data['max_commit_time_ms']:
        bucket_data['max_commit_time_ms'] = time_ms

def analyze(res, fin):
    for line in fin:
        m = COMMIT_LINE_RE.match(line)
        if m is None:
            continue
        add_commit(res, int(m.group(1)), int(m.group(2)))

def output_results(res, fout):
    writer = csv.writer(fout)
    writer.writerow(('commit_size_mb', 'num_commits', 'avg_commit_time_ms', 'max_commit_time_ms'))

    for bucket in sorted(res.iterkeys()):
        bucket_data = res[bucket]
        num_commits = bucket_data['num_commits']
        avg_time_ms = bucket_data['total_commit_time_ms'] / num_commits
        max_time_ms = bucket_data['max_commit_time_ms']
        writer.writerow((bucket, num_commits, avg_time_ms, max_time_ms))


# MAIN #########################################
def main():
    parser = argparse.ArgumentParser(description='Analyze BTRFS commits size and time')
    parser.add_argument('-o', '--output-file', required=True, help='filename to store the CSV output')
    parser.add_argument('input_files', nargs='+', help='file(s) to be analyzed')
    opts = parser.parse_args()

    res = {}
    exit_code = 0
    fins = []

    try:
        for input_filename in opts.input_files:
            print('Opening input file: {0}'.format(input_filename))
            fin = open(input_filename, 'r')
            fins.append(fin)
        with open(opts.output_file, 'w') as fout:
            for fin in fins:
                analyze(res, fin)
            output_results(res, fout)
    except Exception as exc:
        print('ERROR:', file=sys.stderr)
        print(exc, file=sys.stderr)
        exit_code = 1
    finally:
        for fin in fins:
            fin.close()

    sys.exit(exit_code)

main()


