#!/usr/bin/env python3

from __future__ import print_function

import argparse
import re
import sys
import os
import csv
import plotly.express as px


def bug(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    assert False


# Aug  8 16:55:15.632378 [30033] [oba  ] : ZSTAT-GROUP____________________ actv max-actv total-count total-mb__ avg-ms____ max-ms_____
NEW_SAMPLE_RE = re.compile(r'^(\S+\s+\d+\s+\d{2}:\d{2}:\d{2}).+ZSTAT-GROUP____________________')

# Aug  8 16:55:17.674978 [30033] [oba  ] : src-datamover:PUT:curl            56       64         210        420    284.804         406
PUT_RE = re.compile(r'^(\S+\s+\d+\s+\d{2}:\d{2}:\d{2}).+src-datamover:PUT:curl\s+\d+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+\.\d+)')
PUT_IOPS = 'put_iops'
PUT_MBPS = 'put_mbps'
PUT_LAT = 'put_lat'

ALL_METRICS = (PUT_IOPS, PUT_MBPS, PUT_LAT)

HTML = 'html'
JPEG = 'jpeg'
CSV = 'csv'


def validate_opts(opts):
    # check that metrics are valid
    metrics = set(opts.metrics.split(','))
    if not metrics:
        bug('No metrics to collect specified')
    for metric in metrics:
        if metric not in ALL_METRICS:
            bug('Unknown metric: {}'.format(metric))
    opts.metrics = metrics


def parse_obs(opts):
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

            m = PUT_RE.match(line)
            if m is not None:
                assert curr_sample is not None
                assert PUT_IOPS not in curr_sample
                curr_sample[PUT_IOPS] = int(m.group(2))
                assert PUT_MBPS not in curr_sample
                curr_sample[PUT_MBPS] = int(m.group(3))
                assert PUT_LAT not in curr_sample
                curr_sample[PUT_LAT] = float(m.group(4))
                continue

    if curr_sample is not None:
        for metric in ALL_METRICS:
            assert metric in curr_sample

    print('Total {} samples collected'.format(len(samples)))

    return samples


def do_plotly(opts, in_dir_name, out_name, samples):
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
                  title='OBS stats' if opts.fig_title is None else '{}'.format(opts.fig_title))

    outfile = os.path.join(in_dir_name, '{}_stats.{}'.format(out_name, opts.output_format))
    if opts.output_format == HTML:
        fig.write_html(outfile)
    elif opts.output_format == JPEG:
        fig.write_image(outfile)
    else:
        bug('Unsupported output format [{}]'.format(opts.output_format))


def do_csv(opts, in_dir_name, out_name, samples):
    outfile = os.path.join(in_dir_name, '{}.{}'.format(out_name, 'csv'))
    with open(outfile, 'w') as outf:
        csv_writer = csv.writer(outf)

        header_row = ['timestamp']
        for metric in opts.metrics:
            header_row.append(metric)
        csv_writer.writerow(header_row)

        for sample in samples:
            row = [sample['timestamp']]
            for metric in opts.metrics:
                row.append(sample[metric])
            csv_writer.writerow(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-o', '--outfile-prefix', required=False)
    parser.add_argument('--metrics', default=PUT_LAT)
    parser.add_argument('--max-samples', type=int, default=0)
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG, CSV), default=HTML)

    opts = parser.parse_args()
    validate_opts(opts)

    samples = parse_obs(opts)

    in_proper_name = os.path.realpath(opts.infile)
    in_dir_name = os.path.dirname(in_proper_name)
    in_base_name = os.path.basename(in_proper_name)
    if opts.outfile_prefix is not None:
        out_name = opts.outfile_prefix
    else:
        out_name = in_base_name

    if opts.output_format in (HTML, JPEG):
        do_plotly(opts, in_dir_name, out_name, samples)
    elif opts.output_format == CSV:
        do_csv(opts, in_dir_name, out_name, samples)
    else:
        bug('Invalid output format {}'.format(opts.output_format))
