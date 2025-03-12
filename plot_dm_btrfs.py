#! /usr/bin/env python3

import os
import sys
import re
import argparse
import datetime
import csv
import plotly.express as px


def error(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    sys.exit(1)


HTML = 'html'
JPEG = 'jpeg'
CSV = 'csv'

# Thu Oct 22 10:49:59 UTC 2020
TIMESTAMP_RE = re.compile(r'^(\S+\s+\S+\s+\d+\s+\d\d:\d\d:\d\d\s+\w+\s+\d\d\d\d)$')

METRICS = (
    'resolve_write_locked',
    'migr_compl_s',
    # 'migr_compl_a',
    'unmap_chunk_s',
    # 'unmap_chunk_a',
    'upd_jrnl',
    'wait_locked',
    'remapped',
    'migr',
    'cow_rd',
    'delay_rd',
    'delay_wr',
    'resolve_read',
    'read',
    'read_zeros')

RES = (
    ('resolve_write_locked', re.compile(r'^(resolve_write_locked):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('migr_compl_s', re.compile(r'^(migr_compl_s):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    # 'migr_compl_a', re.compile(r'^(migr_compl_a):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('unmap_chunk_s', re.compile(r'^(unmap_chunk_s):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    # 'unmap_chunk_a', re.compile(r'^(unmap_chunk_a):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('upd_jrnl', re.compile(r'^(upd_jrnl):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('wait_locked', re.compile(r'^(wait_locked):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('remapped', re.compile(r'^(remapped):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('migr', re.compile(r'^(migr):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('cow_rd', re.compile(r'^(cow_rd):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('delay_rd', re.compile(r'^(delay_rd):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('delay_wr', re.compile(r'^(delay_wr):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('resolve_read', re.compile(r'^(resolve_read):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('read', re.compile(r'^(read):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
    ('read_zeros', re.compile(r'^(read_zeros):\s+n:\s+(\d+)\s+a:\s+(\d+)us')),
)


def validate_opts(opts):
    basenames = []
    for fname in opts.infile:
        realname = os.path.realpath(fname)
        basename = os.path.basename(realname)
        if basename in basenames:
            error('Basename {} appears more than once'.format(basename))
        basenames.append(basename)

    metrics = opts.metrics.split(',')
    if not metrics:
        error('No metrics specified')
    have_all = False
    for metric in metrics:
        if metric == 'ALL':
            have_all = True
            break
        if metric not in METRICS:
            error('Invalid metric {}'.format(metric))
    if have_all:
        opts.metrics = METRICS
    else:
        opts.metrics = metrics

    if opts.max_samples_per_file < 0:
        error('max_samples_per_file shoule be zero or positive')

    if opts.outfile_basename is None:
        if len(opts.metrics) == 1:
            opts.outfile_basename = opts.metrics[0]
        else:
            opts.outfile_basename = 'stats_dmbtrfs'

    if opts.fig_title is None:
        if len(opts.metrics) == 1:
            opts.fig_title = opts.metrics[0]
        else:
            opts.fig_title = 'dm-btrfs Stats'

    return basenames


def produce_key(basename, metric):
    return '{}_{}'.format(basename, metric)


def parse_dmbtrfs_stats(opts, fname, basename, samples):
    curr_timestamp = None
    curr_sample = None

    with open(fname, 'r') as f:
        for line in f:
            m = TIMESTAMP_RE.search(line)
            if m is not None:
                new_timestamp = datetime.datetime.strptime(m.group(1), '%a %b %d %H:%M:%S UTC %Y')

                assert curr_timestamp is None or curr_timestamp != new_timestamp
                # we move to new timestamp
                curr_timestamp = new_timestamp
                curr_sample = samples.get(curr_timestamp)
                if curr_sample is None:
                    curr_sample = {}
                    samples[curr_timestamp] = curr_sample
                continue

            for _field, regexp in RES:
                m = regexp.search(line)
                if m is not None:
                    # check if we should collect this metric
                    metric = m.group(1)
                    need_this = metric in opts.metrics
                    if need_this:
                        key = produce_key(basename, metric)
                        if curr_sample.get(key) is not None:
                            error('duplicate key {}'.format(key))
                        curr_sample[key] = float(m.group(3)) / 1000


def do_csv(opts, in_dirname, basenames, samples):
    file_nr = 1
    need_new_file = True
    printed_samples = 0
    outf = None
    csv_writer = None

    # produce all samples sorted by timestamp (Datetime objec)
    for dt in sorted(samples.keys()):
        sample = samples[dt]

        if need_new_file:
            if opts.max_samples_per_file == 0 or len(samples) <= opts.max_samples_per_file:
                outfile = os.path.join(in_dirname, '{}.{}'.format(opts.outfile_basename, 'csv'))
            else:
                outfile = os.path.join(in_dirname, '{}.{:04d}.{}'.format(opts.outfile_basename, file_nr, 'csv'))
            outf = open(outfile, 'w')

            csv_writer = csv.writer(outf)

            header_row = ['timestamp']
            for basename in basenames:
                for metric in opts.metrics:
                    header_row.append(produce_key(basename, metric))
            csv_writer.writerow(header_row)

            need_new_file = False

        # print the current sample
        row = [dt]
        for basename in basenames:
            for metric in opts.metrics:
                key = produce_key(basename, metric)
                val = sample.get(key)
                if val is None:
                    val = ' '
                row.append(val)
        csv_writer.writerow(row)

        printed_samples = printed_samples + 1
        if opts.max_samples_per_file > 0 and printed_samples >= opts.max_samples_per_file:
            file_nr = file_nr + 1
            need_new_file = True
            printed_samples = 0
            outf.close()
            outf = None
            csv_writer = None

    if outf is not None:
        outf.close()


def do_plotly(opts, in_dirname, basenames, samples):
    total_samples = len(samples)
    total_added_samples = 0

    file_nr = 1
    need_new_file = True
    added_samples = 0

    timestamps = None
    data = None
    y = None

    # produce all samples sorted by timestamp (Datetime objec)
    for dt in sorted(samples.keys()):
        sample = samples[dt]

        if need_new_file:
            # Prepare an input for plotly: produce a column for the X-axis (timestamp) and a column for each metric
            timestamps = []
            data = {'timestamp': timestamps}
            y = []
            for basename in basenames:
                for metric in opts.metrics:
                    key = produce_key(basename, metric)
                    data[key] = []
                    y.append(key)

            need_new_file = False

        # instead of real timestamp, append sample index; with timestamps the graph looks messy
        timestamps.append(total_added_samples)

        for basename in basenames:
            for metric in opts.metrics:
                key = produce_key(basename, metric)
                val = sample.get(key)
                if val is None:
                    val = 0
                data[key].append(val)

        added_samples += 1
        total_added_samples += 1

        # we need to flush the current content if:
        # - this is the last sample OR
        # - we have max_samples_per_file limit and we are about to cross it
        if total_added_samples == total_samples or (opts.max_samples_per_file > 0 and added_samples >= opts.max_samples_per_file):
            fig = px.line(data, x='timestamp', y=y, title=opts.fig_title)

            # figure out the file name
            if opts.max_samples_per_file == 0 or total_samples <= opts.max_samples_per_file:
                # single file
                outfile = os.path.join(in_dirname, '{}.{}'.format(opts.outfile_basename, opts.output_format))
            else:
                # multiple files
                outfile = os.path.join(in_dirname, '{}.{:04d}.{}'.format(opts.outfile_basename, file_nr, opts.output_format))

            if opts.output_format == HTML:
                fig.write_html(outfile)
            elif opts.output_format == JPEG:
                fig.write_image(outfile)
            else:
                error('Unsupported output format [{}]'.format(opts.output_format))

            # reset stuff
            file_nr = file_nr + 1
            need_new_file = True
            added_samples = 0
            timestamps = None
            data = None
            y = None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True, nargs='+', help='dmbtrfs latency breakdown files')
    parser.add_argument('-o', '--outfile-basename', required=False)
    parser.add_argument('--metrics', default='ALL')
    parser.add_argument('--max-samples-per-file', type=int, default=0)
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG, CSV), default=HTML)

    opts = parser.parse_args()

    basenames = validate_opts(opts)

    samples = {}
    for fname, basename in zip(opts.infile, basenames):
        print('Parsing {}...'.format(fname))
        parse_dmbtrfs_stats(opts, fname, basename, samples)

    # take the directory name from the first input file
    in_realname = os.path.realpath(opts.infile[0])
    in_dirname = os.path.dirname(in_realname)

    if opts.output_format in (HTML, JPEG):
        do_plotly(opts, in_dirname, basenames, samples)
    elif opts.output_format == CSV:
        do_csv(opts, in_dirname, basenames, samples)
    else:
        error('Invalid output format {}'.format(opts.output_format))
