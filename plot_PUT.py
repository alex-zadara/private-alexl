#!/usr/bin/env python3

from __future__ import print_function

import argparse
import re
import sys
import os
import plotly.express as px


def bug(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    assert False


# Jul 23 15:29:15.999984 [2587] [     ] : ZSTAT-GROUP____________________ actv max-actv total-count avg-ms____ max-ms_____
NEW_SAMPLE_RE = re.compile(r'^(\S+\s+\d+\s+\d{2}:\d{2}:\d{2}).+ZSTAT-GROUP____________________')

# Jul 23 15:36:01.560314 [2564] [     ] : io_mgr_put_total                  53      105        3293     12.803          66
PUT_TOTAL_RE = re.compile(r'^(\S+\s+\d+\s+\d{2}:\d{2}:\d{2}).+io_mgr_put_total\s+\d+\s+\d+\s+(\d+)\s+(\d+\.\d+)')
PUT_TOTAL_IOPS = 'io_mgr_put_total_iops'
PUT_TOTAL_LAT = 'io_mgr_put_total_lat'

# Jul 23 15:29:16.000077 [2587] [     ] : io_mgr_put_mongo                   4       26         568     10.198          46
PUT_MONGO_RE = re.compile(r'^(\S+\s+\d+\s+\d{2}:\d{2}:\d{2}).+io_mgr_put_mongo\s+\d+\s+\d+\s+(\d+)\s+(\d+\.\d+)')
PUT_MONGO_LAT = 'io_mgr_put_mongo'

# Jul 23 15:29:14.973449 [2587] [     ] : io_mgr_put_wait_commit            28       52        3433      5.571          53
PUT_WAIT_COMMIT_RE = re.compile(r'^(\S+\s+\d+\s+\d{2}:\d{2}:\d{2}).+io_mgr_put_wait_commit\s+\d+\s+\d+\s+(\d+)\s+(\d+\.\d+)')
PUT_WAIT_COMMIT_LAT = 'io_mgr_put_wait_commit'

ALL_METRICS = (PUT_TOTAL_IOPS, PUT_TOTAL_LAT, PUT_MONGO_LAT, PUT_WAIT_COMMIT_LAT)

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


def parse_put(opts):
    print('Parsing PUT')

    samples = []
    curr_sample = None

    with open(opts.infile, 'r') as fin:
        for line in fin:
            m = NEW_SAMPLE_RE.match(line)
            if m is not None:
                # Finalize the current sample, if any
                if curr_sample is not None:
                    for metric in ALL_METRICS:
                        assert metric in curr_sample
                # we need to create a new sample
                if opts.max_samples > 0 and len(samples) >= opts.max_samples:
                    print('Terminating parsing due to max_samples')
                    break

                timestamp = m.group(1)
                curr_sample = {'timestamp': timestamp}
                samples.append(curr_sample)
                continue

            m = PUT_TOTAL_RE.match(line)
            if m is not None:
                assert curr_sample is not None
                assert PUT_TOTAL_LAT not in curr_sample
                curr_sample[PUT_TOTAL_LAT] = float(m.group(3))
                assert PUT_TOTAL_IOPS not in curr_sample
                curr_sample[PUT_TOTAL_IOPS] = int(m.group(2))
                continue
            m = PUT_MONGO_RE.match(line)
            if m is not None:
                assert curr_sample is not None
                assert PUT_MONGO_LAT not in curr_sample
                curr_sample[PUT_MONGO_LAT] = float(m.group(3))
                continue
            m = PUT_WAIT_COMMIT_RE.match(line)
            if m is not None:
                assert curr_sample is not None
                assert PUT_WAIT_COMMIT_LAT not in curr_sample
                curr_sample[PUT_WAIT_COMMIT_LAT] = float(m.group(3))
                continue

    if curr_sample is not None:
        for metric in ALL_METRICS:
            assert metric in curr_sample

    print('Total {} samples collected'.format(len(samples)))

    return samples


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-o', '--outfile-prefix', required=False)
    parser.add_argument('--metrics', default=PUT_TOTAL_IOPS)
    parser.add_argument('--max-samples', type=int, default=0)
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG), default=HTML)

    opts = parser.parse_args()
    validate_opts(opts)

    samples = parse_put(opts)

    in_proper_name = os.path.realpath(opts.infile)
    in_dir_name = os.path.dirname(in_proper_name)
    in_base_name = os.path.basename(in_proper_name)
    if opts.outfile_prefix is not None:
        out_name = opts.outfile_prefix
    else:
        out_name = in_base_name

    # Prepare an input for plotly: produce a column for the X-axis (timestamp) and a column for each metric
    timestamps = []
    data = {'timestamp': timestamps}
    y = []
    for metric in opts.metrics:
        data[metric] = []
        y.append(metric)

    sample_idx = 0
    for sample in samples:
        # instead of real timestamp, append sample index; with timestamps the graph looks messy
        timestamps.append(sample_idx)
        sample_idx += 1

        for metric in opts.metrics:
            val = sample[metric]
            data[metric].append(val)

    print('Producing {} plot for metrics [{}]...'.format(opts.output_format, ', '.join(opts.metrics)))

    fig = px.line(data, x='timestamp', y=y,
                  title='PUT stats' if opts.fig_title is None else '{}'.format(opts.fig_title))

    outfile = os.path.join(in_dir_name, '{}_stats.{}'.format(out_name, opts.output_format))
    if opts.output_format == HTML:
        fig.write_html(outfile)
    elif opts.output_format == JPEG:
        fig.write_image(outfile)
    else:
        bug('Unsupported output format [{}]'.format(opts.output_format))
