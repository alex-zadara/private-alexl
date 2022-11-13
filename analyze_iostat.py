#!/usr/bin/env python

from __future__ import print_function

import sys
import argparse
import re
import csv


def bug(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    assert False


# 10/12/20 19:51:43
HEADER = re.compile(r'^(\d+/\d+/\d+\s+\d+:\d+:\d+)')


# Device            r/s     w/s     rkB/s     wkB/s   rrqm/s   wrqm/s  %rrqm  %wrqm r_await w_await aqu-sz rareq-sz wareq-sz  svctm  %util
# vda              0.00    2.00      0.00      8.00     0.00     0.00   0.00   0.00    0.00    0.50   0.00     0.00     4.00   0.00   0.00
IOSTAT = re.compile(r'([\w-]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+([\d.]+)\s+([\d.]+)')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-t', '--total', action='store_true')
    parser.add_argument('blkdevs', nargs='+')

    opts = parser.parse_args()

    samples = []

    curr_header = None
    curr_sample = {}

    with open(opts.infile, 'r') as fin:
        for line in fin:
            m = HEADER.match(line)
            if m is not None:
                if curr_header is not None:
                    samples.append((curr_header, curr_sample))
                curr_header = m.group(1)
                curr_sample = {}
                continue

            m = IOSTAT.match(line)
            if m is not None:
                if curr_header is None:
                    bug('Did not see a timestamp header before line:\n{}'.format(line))

                blkdev = m.group(1)
                if blkdev not in opts.blkdevs:
                    continue

                curr_sample[blkdev] = {'read_per_sec': float(m.group(2)), 'write_per_sec': float(m.group(3)),
                                       'read_kb_sec': float(m.group(4)), 'write_kb_sec': float(m.group(5)),
                                       'read_lat_ms': float(m.group(6)), 'write_lat_ms': float(m.group(7))}

    if curr_header is not None:
        samples.append((curr_header, curr_sample))

    # Produce results
    with open(opts.infile + '.csv', 'w') as csvf:
        csv_writer = csv.writer(csvf)

        if not opts.total:
            row = ['timestamp']
            for blkdev in opts.blkdevs:
                row.append('{}_rd_per_sec'.format(blkdev))
                row.append('{}_rd_mb_sec'.format(blkdev))
                row.append('{}_rd_lat_ms'.format(blkdev))
                row.append('{}_wr_per_sec'.format(blkdev))
                row.append('{}_wr_mb_sec'.format(blkdev))
                row.append('{}_wr_lat_ms'.format(blkdev))
            csv_writer.writerow(row)

            num_samples = 0
            for curr_header, curr_sample in samples:
                row = [curr_header]
                for blkdev in opts.blkdevs:
                    blkdev_stats = curr_sample.get(blkdev)
                    if blkdev_stats is None:
                        row.append('-')
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats['read_per_sec']))
                        row.append('{:.2f}'.format(blkdev_stats['read_kb_sec'] / 1024))
                        row.append('{:.2f}'.format(blkdev_stats['read_lat_ms']))
                        row.append('{:.2f}'.format(blkdev_stats['write_per_sec']))
                        row.append('{:.2f}'.format(blkdev_stats['write_kb_sec'] / 1024))
                        row.append('{:.2f}'.format(blkdev_stats['write_lat_ms']))

                csv_writer.writerow(row)
                num_samples += 1
            print('Total {} samples'.format(num_samples))
        else:
            row = ('timestamp',
                   'total_rd_per_sec', 'total_rd_mb_sec', 'avg_rd_lat_ms',
                   'total_wr_per_sec', 'total_wr_mb_sec', 'avg_wr_lat_ms')
            csv_writer.writerow(row)

            num_samples = 0
            for curr_header, curr_sample in samples:
                total_rd_per_sec = 0
                total_rd_kb_sec = 0
                total_rd_lat_ms = 0
                total_wr_per_sec = 0
                total_wr_kb_sec = 0
                total_wr_lat_ms = 0

                num_rd_lat = 0
                num_wr_lat = 0
                for blkdev in opts.blkdevs:
                    blkdev_stats = curr_sample.get(blkdev)
                    if blkdev_stats is None:
                        continue

                    # if all values are zero don't account rd/wr
                    rd_per_sec = blkdev_stats['read_per_sec']
                    rd_kb_sec = blkdev_stats['read_kb_sec']
                    rd_lat_ms = blkdev_stats['read_lat_ms']
                    if not (rd_per_sec == 0 and rd_kb_sec == 0 and rd_lat_ms == 0):
                        total_rd_per_sec += rd_per_sec
                        total_rd_kb_sec += rd_kb_sec
                        total_rd_lat_ms += rd_lat_ms
                        num_rd_lat += 1

                    wr_per_sec = blkdev_stats['write_per_sec']
                    wr_kb_sec = blkdev_stats['write_kb_sec']
                    wr_lat_ms = blkdev_stats['write_lat_ms']
                    if not (wr_per_sec == 0 and wr_kb_sec == 0 and wr_lat_ms == 0):
                        total_wr_per_sec += wr_per_sec
                        total_wr_kb_sec += wr_kb_sec
                        total_wr_lat_ms += wr_lat_ms
                        num_wr_lat += 1

                row = (curr_header,
                       '{:.2f}'.format(total_rd_per_sec),
                       '{:.2f}'.format(total_rd_kb_sec / 1024),
                       '{:.2f}'.format(0 if num_rd_lat == 0 else total_rd_lat_ms / num_rd_lat),
                       '{:.2f}'.format(total_wr_per_sec),
                       '{:.2f}'.format(total_wr_kb_sec / 1024),
                       '{:.2f}'.format(0 if num_wr_lat == 0 else total_wr_lat_ms / num_wr_lat))
                csv_writer.writerow(row)
                num_samples += 1
            print('Total {} samples'.format(num_samples))
