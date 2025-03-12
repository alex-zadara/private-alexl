#!/usr/bin/env python3

import sys
import re
import argparse
import os
import plotly.express as px


def bug(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    assert False


# 10/12/20 19:51:43
HEADER = re.compile(r'^(\d+/\d+/\d+\s+\d+:\d+:\d+)')


# Device            r/s     w/s     rkB/s     wkB/s   rrqm/s   wrqm/s  %rrqm  %wrqm r_await w_await aqu-sz rareq-sz wareq-sz  svctm  %util
# vda              0.00    2.00      0.00      8.00     0.00     0.00   0.00   0.00    0.00    0.50   0.00     0.00     4.00   0.00   0.00
IOSTAT = re.compile(r'([\w-]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+[\d.]+\s+([\d.]+)')

RD_PER_SEC = "rd_per_sec"
RD_MB_SEC = "rd_mb_sec"
RD_LAT_MS = "rd_lat_ms"

WR_PER_SEC = "wr_per_sec"
WR_MB_SEC = "wr_mb_sec"
WR_LAT_MS = "wr_lat_ms"

QU_SZ = 'qu_sz'
RA_REQ_SZ = 'ra_req_sz'
WA_REQ_SZ = 'wa_req_sz'
UTIL = 'util'

ALL_METRICS = (RD_PER_SEC, RD_MB_SEC, RD_LAT_MS,
               WR_PER_SEC, WR_MB_SEC, WR_LAT_MS,
               QU_SZ, UTIL, RA_REQ_SZ, WA_REQ_SZ)

HTML = 'html'
JPEG = 'jpeg'


def validate_opts(opts):
    # check that metrics are valid
    metrics = set(opts.metrics.split(','))
    if not metrics:
        bug('No metrics to collect specified')
    for metric in metrics:
        if metric not in ALL_METRICS:
            bug('Unknown metric: {}'.format(metric))
    opts.metrics = metrics

    # iostat prints device names without the '/dev/' prefix, so if user specified it, cut it off
    for idx in range(len(opts.blkdevs)):
        if opts.blkdevs[idx].startswith('/dev/'):
            opts.blkdevs[idx] = opts.blkdevs[idx][5:]


def parse_iostat(opts):
    print('Parsing iostat log...')

    samples = []

    curr_header = None
    curr_sample = {}

    with open(opts.infile, 'r') as fin:
        for line in fin:
            m = HEADER.match(line)
            if m is not None:
                if curr_header is not None:
                    # new header found, finalize the previous sample, before starting the new one
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
                                       RD_MB_SEC: float(m.group(4)) / 1024, WR_MB_SEC: float(m.group(5)) / 1024,
                                       RD_LAT_MS: float(m.group(6)), WR_LAT_MS: float(m.group(7)),
                                       QU_SZ: float(m.group(8)),
                                       RA_REQ_SZ: float(m.group(9)), WA_REQ_SZ: float(m.group(10)),
                                       UTIL: float(m.group(11))}

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

    return samples


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-o', '--outfile-prefix', required=False)
    parser.add_argument('--metrics', default=RD_PER_SEC + ',' + RD_MB_SEC + ',' + RD_LAT_MS + ',' + WR_PER_SEC + ',' + WR_MB_SEC + ',' + WR_LAT_MS)
    parser.add_argument('--max-samples', type=int, default=0)
    parser.add_argument('--samples-from-end', action='store_true')
    parser.add_argument('--dont-cut-first-line', action='store_true')
    parser.add_argument('--real-timestamp', action='store_true')
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG), default=HTML)
    parser.add_argument('blkdevs', nargs='+')

    opts = parser.parse_args()
    validate_opts(opts)

    samples = parse_iostat(opts)

    in_proper_name = os.path.realpath(opts.infile)
    in_dir_name = os.path.dirname(in_proper_name)
    in_base_name = os.path.basename(in_proper_name)
    if opts.outfile_prefix is not None:
        out_name = opts.outfile_prefix
    else:
        out_name = in_base_name

    # For each metric, produce a separate graph
    for metric in opts.metrics:
        # Prepare an input for plotly:
        # produce a column for the X-axis (timestamp) and a column for each requested block device
        timestamps = []
        data = {'timestamp': timestamps}
        y = []
        for blkdev in opts.blkdevs:
            if blkdev == 'timestamp':
                bug('Invalid name for a block device: {}'.format(blkdev))
            data[blkdev] = []
            y.append(blkdev)

        sample_idx = 0
        for sample in samples:
            if opts.real_timestamp:
                timestamps.append(sample[0].split()[-1])
            else:
                timestamps.append(sample_idx)
            sample_idx += 1

            for blkdev in opts.blkdevs:
                sample_for_blkdev = sample[1].get(blkdev)
                data[blkdev].append(0 if sample_for_blkdev is None else sample_for_blkdev[metric])

        print('Producing {} plot for metric [{}]...'.format(opts.output_format, metric))

        fig = px.line(data, x='timestamp', y=y,
                      title=metric if opts.fig_title is None else '{},{}'.format(opts.fig_title, metric))

        outfile = os.path.join(in_dir_name, '{}_{}.{}'.format(out_name, metric, opts.output_format))
        if opts.output_format == HTML:
            fig.write_html(outfile)
        elif opts.output_format == JPEG:
            fig.write_image(outfile)
        else:
            bug('Unsupported output format [{}]'.format(opts.output_format))

    print('Done.')
