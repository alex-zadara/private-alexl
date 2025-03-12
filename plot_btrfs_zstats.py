#!/usr/bin/env python3

import argparse
import re
import sys
import os
import plotly.express as px


# COW_UNMAPPED:   0/1808 0%
COW_UNMAPPED_RE = re.compile(r'^COW_UNMAPPED:\s+(\d+)/\d+\s+\d+%')
# COW_MAPPED:   479/1808 26%
COW_MAPPED_RE = re.compile(r'^COW_MAPPED:\s+(\d+)/\d+\s+\d+%')
# NOCOW:   1329/1808 73%
NOCOW_RE = re.compile(r'^NOCOW:\s+(\d+)/\d+\s+\d+%')

COW_UNMAPPED = 'cow_unmapped'
COW_MAPPED = 'cow_mapped'
COW_TOTAL = 'cow_total'
NOCOW = 'nocow'

COW_UNMAPPED_PC = 'cow_unmapped_pc'
COW_MAPPED_PC = 'cow_mapped_pc'
COW_TOTAL_PC = 'cow_total_pc'
NOCOW_PC = 'nocow_pc'

ALL_METRICS = (COW_UNMAPPED, COW_MAPPED, COW_TOTAL, NOCOW,
               COW_UNMAPPED_PC, COW_MAPPED_PC, COW_TOTAL_PC, NOCOW_PC)

HTML = 'html'
JPEG = 'jpeg'


def bug(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    assert False


def validate_opts(opts):
    # check that metrics are valid
    metrics = set(opts.metrics.split(','))
    if not metrics:
        bug('No metrics to collect specified')
    for metric in metrics:
        if metric not in ALL_METRICS:
            bug('Unknown metric: {}'.format(metric))
    opts.metrics = metrics


def verify_sample(sample):
    assert COW_UNMAPPED in sample
    assert COW_MAPPED in sample
    assert NOCOW in sample


def parse_zstats(opts):
    samples = []
    curr_sample = None

    with open(opts.infile, 'r') as fin:
        for line in fin:
            m = COW_UNMAPPED_RE.match(line)
            if m is not None:
                if curr_sample is not None:
                    verify_sample(curr_sample)
                    samples.append(curr_sample)
                    curr_sample = None
                    if opts.max_samples > 0 and len(samples) >= opts.max_samples:
                        print('Terminating parsing due to max_samples')
                        break
                curr_sample = {COW_UNMAPPED: int(m.group(1))}
                continue
            m = COW_MAPPED_RE.match(line)
            if m is not None:
                assert curr_sample is not None
                assert COW_MAPPED not in curr_sample
                curr_sample[COW_MAPPED] = int(m.group(1))
                continue
            m = NOCOW_RE.match(line)
            if m is not None:
                assert curr_sample is not None
                assert NOCOW not in curr_sample
                curr_sample[NOCOW] = int(m.group(1))
                continue

    if curr_sample is not None:
        verify_sample(curr_sample)
        samples.append(curr_sample)

    # calculate percents
    for sample in samples:
        cow_unmapped = sample[COW_UNMAPPED]
        cow_mapped = sample[COW_MAPPED]
        nocow = sample[NOCOW]
        total = cow_unmapped + cow_mapped + nocow
        if total == 0:
            sample[COW_TOTAL] = 0
            sample[COW_UNMAPPED_PC] = 0
            sample[COW_MAPPED_PC] = 0
            sample[COW_TOTAL_PC] = 0
            sample[NOCOW_PC] = 0
        else:
            sample[COW_TOTAL] = cow_unmapped + cow_mapped
            sample[COW_UNMAPPED_PC] = round((cow_unmapped * 100) / total, 2)
            sample[COW_MAPPED_PC] = round((cow_mapped * 100) / total, 2)
            sample[COW_TOTAL_PC] = round(((cow_unmapped + cow_mapped) * 100) / total, 2)
            sample[NOCOW_PC] = round((nocow * 100) / total, 2)

    return samples


def do_plotly(opts, in_dir_name, out_name, samples):
    # Prepare an input for plotly: produce a column for the X-axis (sample index) and a column for each metric
    sample_idxs = []
    data = {'sample_idx': sample_idxs}
    y = []
    for metric in opts.metrics:
        data[metric] = []
        y.append(metric)

    sample_idx = 0
    for sample in samples:
        sample_idxs.append(sample_idx)
        sample_idx += 1

        for metric in opts.metrics:
            val = sample[metric]
            data[metric].append(val)

    print('Producing {} plot for metrics [{}]...'.format(opts.output_format, ', '.join(opts.metrics)))

    fig = px.line(data, x='sample_idx', y=y,
                  title='BTRFS zstats' if opts.fig_title is None else '{}'.format(opts.fig_title))

    outfile = os.path.join(in_dir_name, '{}_stats.{}'.format(out_name, opts.output_format))
    if opts.output_format == HTML:
        fig.write_html(outfile)
    elif opts.output_format == JPEG:
        fig.write_image(outfile)
    else:
        bug('Unsupported output format [{}]'.format(opts.output_format))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-o', '--outfile-prefix', required=False)
    parser.add_argument('--metrics', default=COW_TOTAL_PC)
    parser.add_argument('--max-samples', type=int, default=0)
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG), default=HTML)

    opts = parser.parse_args()
    validate_opts(opts)

    samples = parse_zstats(opts)

    in_proper_name = os.path.realpath(opts.infile)
    in_dir_name = os.path.dirname(in_proper_name)
    in_base_name = os.path.basename(in_proper_name)
    if opts.outfile_prefix is not None:
        out_name = opts.outfile_prefix
    else:
        out_name = in_base_name

    do_plotly(opts, in_dir_name, out_name, samples)
