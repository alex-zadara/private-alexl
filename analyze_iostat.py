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
IOSTAT = re.compile(r'([\w-]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)')

RD_PER_SEC = "rd_per_sec"
RD_MB_SEC = "rd_mb_sec"
RD_LAT_MS = "rd_lat_ms"

WR_PER_SEC = "wr_per_sec"
WR_MB_SEC = "wr_mb_sec"
WR_LAT_MS = "wr_lat_ms"

QU_SZ = 'qu_sz'

ALL_METRICS = (RD_PER_SEC, RD_MB_SEC, RD_LAT_MS,
               WR_PER_SEC, WR_MB_SEC, WR_LAT_MS,
               QU_SZ)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('--metrics', default=RD_PER_SEC + ',' + RD_MB_SEC + ',' + RD_LAT_MS + ',' + WR_PER_SEC + ',' + WR_MB_SEC + ',' + WR_LAT_MS)
    parser.add_argument('--max-samples', type=int, default=0)
    parser.add_argument('--samples-from-end', action='store_true')
    parser.add_argument('--dont-cut-first-line', action='store_true')
    parser.add_argument('blkdevs', nargs='+')

    opts = parser.parse_args()
    metrics = set(opts.metrics.split(','))
    if not metrics:
        bug('No metrics to collect specified')
    for metric in metrics:
        if metric not in ALL_METRICS:
            bug('Unknown metric: {}'.format(metric))

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

                curr_sample[blkdev] = {RD_PER_SEC: float(m.group(2)), WR_PER_SEC: float(m.group(3)),
                                       # we actually store KB/sec here
                                       RD_MB_SEC: float(m.group(4)), WR_MB_SEC: float(m.group(5)),
                                       RD_LAT_MS: float(m.group(6)), WR_LAT_MS: float(m.group(7)),
                                       QU_SZ: float(m.group(8))}

    if curr_header is not None:
        samples.append((curr_header, curr_sample))

    print('Total {} samples'.format(len(samples)))

    # First line in iostat output contains bogus values, cut it
    cut_first_line = not opts.dont_cut_first_line
    if cut_first_line:
        print('Cutting first line of iostat output')
        samples = samples[1:]

    # Limit to max_samples
    if opts.max_samples > 0:
        print('Limiting to {} samples{}'.format(opts.max_samples, ' (from end)' if opts.samples_from_end else ''))
        if opts.samples_from_end:
            samples = samples[len(samples) - opts.max_samples:]
        else:
            samples = samples[:opts.max_samples]

    # Produce results
    with open(opts.infile + '.csv', 'w') as csvf:
        csv_writer = csv.writer(csvf)

        row = ['timestamp']
        for blkdev in opts.blkdevs:
            if RD_PER_SEC in metrics:
                row.append('{}_{}'.format(blkdev, RD_PER_SEC))
            if RD_MB_SEC in metrics:
                row.append('{}_{}'.format(blkdev, RD_MB_SEC))
            if RD_LAT_MS in metrics:
                row.append('{}_{}'.format(blkdev, RD_LAT_MS))
            if WR_PER_SEC in metrics:
                row.append('{}_{}'.format(blkdev, WR_PER_SEC))
            if WR_MB_SEC in metrics:
                row.append('{}_{}'.format(blkdev, WR_MB_SEC))
            if WR_LAT_MS in metrics:
                row.append('{}_{}'.format(blkdev, WR_LAT_MS))
            if QU_SZ in metrics:
                row.append('{}_{}'.format(blkdev, QU_SZ))
        csv_writer.writerow(row)

        num_samples = 0
        for curr_header, curr_sample in samples:
            row = [curr_header]
            for blkdev in opts.blkdevs:
                blkdev_stats = curr_sample.get(blkdev)
                if RD_PER_SEC in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[RD_PER_SEC]))
                if RD_MB_SEC in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[RD_MB_SEC] / 1024))
                if RD_LAT_MS in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[RD_LAT_MS]))
                if WR_PER_SEC in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[WR_PER_SEC]))
                if WR_MB_SEC in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[WR_MB_SEC] / 1024))
                if WR_LAT_MS in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[WR_LAT_MS]))
                if QU_SZ in metrics:
                    if blkdev_stats is None:
                        row.append('-')
                    else:
                        row.append('{:.2f}'.format(blkdev_stats[QU_SZ]))
            csv_writer.writerow(row)
            num_samples += 1
