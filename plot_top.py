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


# top - 15:55:51 up 1 day,  5:45,  1 user,  load average: 19.38, 17.71, 14.32
TOP_START = re.compile(r'^top\s+-\s+(\d\d:\d\d:\d\d)\s+up')

# KiB Mem : 49068280 total, 12083344 free, 19424012 used, 17560924 buff/cache
TOP_MEM_LINE = re.compile(r'^KiB\s+Mem\s+:\s+\d+\s+total,\s+(\d+)\s+free,\s+(\d+)\s+used,\s+(\d+)\s+buff/cache')

# 22300 root      20   0 22.788g 0.018t  38128 S 838.8 39.3 115:37.44 zadara_osm
TOP_CPU_FOR_CMD_LINE = re.compile(r'^\s*\d+\s+\w+\s+\d+\s+\d+\s+[\w\.]+\s+[\w\.]+\s+\d+\s+\w+\s+([0-9\.]+)\s+[0-9\.]+\s+[0-9\.\:]+\s+(\w+)')

CPU_PER_CMD = 'cpu_per_cmd'

MEM_FREE = 'mem_free'
MEM_USED = 'mem_used'
MEM_BUFF_CACHE = 'mem_buff_cache'
MEM_METRICS = (MEM_FREE, MEM_USED, MEM_BUFF_CACHE)

ALL_METRICS = (CPU_PER_CMD, MEM_FREE, MEM_USED, MEM_BUFF_CACHE)

HTML = 'html'
JPEG = 'jpeg'
CSV = 'csv'


def validate_opts(opts):
    # check that metrics are valid
    metrics = set(opts.metrics.split(','))
    if opts.commands is None:
        opts.commands = ()
    if opts.commands and CPU_PER_CMD not in metrics:
        metrics.add(CPU_PER_CMD)
    if not opts.commands and CPU_PER_CMD in metrics:
        metrics.remove(CPU_PER_CMD)
    if not metrics:
        bug('No metrics to collect specified')
    for metric in metrics:
        if metric not in ALL_METRICS:
            bug('Unknown metric: {}'.format(metric))
    opts.metrics = metrics


def parse_top(opts):
    print('Parsing top...')

    samples = []
    curr_sample = None

    with open(opts.infile, 'r') as fin:
        for line in fin:
            m = TOP_START.match(line)
            if m is not None:
                if opts.max_samples > 0 and len(samples) >= opts.max_samples:
                    print('Terminating parsing due to max_samples')
                    break
                curr_sample = {'timestamp': m.group(1), 'cpu_per_cmd': {}}
                samples.append(curr_sample)
                continue

            m = TOP_MEM_LINE.match(line)
            if m is not None:
                assert curr_sample is not None
                curr_sample[MEM_FREE] = int(m.group(1))
                curr_sample[MEM_USED] = int(m.group(2))
                curr_sample[MEM_BUFF_CACHE] = int(m.group(3))
                continue

            m = TOP_CPU_FOR_CMD_LINE.match(line)
            if m is not None:
                assert curr_sample is not None
                command = m.group(2)
                if command not in opts.commands:
                    continue
                cpu_pc = float(m.group(1))
                cpu_per_cmd = curr_sample['cpu_per_cmd']
                if cpu_per_cmd.get(command) is not None:
                    cpu_per_cmd[command] = cpu_per_cmd[command] + cpu_pc
                else:
                    cpu_per_cmd[command] = cpu_pc

    # Check whether the last sample has the 'mem' entries; it could be that top output was cut off
    if samples:
        last_sample = samples[-1]
        if last_sample.get(MEM_FREE) is None:
            del samples[-1]

    print('Total {} samples collected'.format(len(samples)))

    return samples


def do_plotly(opts, in_dir_name, out_name, samples):
    # MEM-metrics #########################################
    # Prepare an input for plotly: produce a column for the X-axis (timestamp) and a column for each MEM-metric
    timestamps = []
    data = {'timestamp': timestamps}
    y = []
    mem_metrics = set(MEM_METRICS) & opts.metrics
    for mem_metric in mem_metrics:
        data[mem_metric] = []
        y.append(mem_metric)

    sample_idx = 0
    for sample in samples:
        # instead of real timestamp, append sample index; with timestamps the graph looks messy
        timestamps.append(sample_idx)
        sample_idx += 1

        for mem_metric in mem_metrics:
            val = sample[mem_metric] / 1024
            data[mem_metric].append(val)

    print('Producing {} plot for metrics [{}]...'.format(opts.output_format, ', '.join(mem_metrics)))

    fig = px.line(data, x='timestamp', y=y,
                  title='Memory (MB)' if opts.fig_title is None else '{}, memory (MB)'.format(opts.fig_title))

    outfile = os.path.join(in_dir_name, '{}_{}.{}'.format(out_name, 'mem', opts.output_format))
    if opts.output_format == HTML:
        fig.write_html(outfile)
    elif opts.output_format == JPEG:
        fig.write_image(outfile)
    else:
        bug('Unsupported output format [{}]'.format(opts.output_format))

    # CPU-metrics #########################################
    if CPU_PER_CMD in opts.metrics:
        bug('CPU metrics plotting is not implemented yet')


def do_csv(opts, in_dir_name, out_name, samples):
    outfile = os.path.join(in_dir_name, '{}.{}'.format(out_name, 'csv'))
    with open(outfile, 'w') as outf:
        csv_writer = csv.writer(outf)

        header_row = ['timestamp']
        for metric in opts.metrics:
            if metric == CPU_PER_CMD:
                for cmd in opts.commands:
                    header_row.append(cmd)
            elif metric in MEM_METRICS:
                header_row.append('{} (mb)'.format(metric))
            else:
                bug('Unknown metric {}'.format(metric))
        csv_writer.writerow(header_row)

        for sample in samples:
            row = [sample['timestamp']]
            for metric in opts.metrics:
                if metric == CPU_PER_CMD:
                    for cmd in opts.commands:
                        val = sample['cpu_per_cmd'].get(cmd)
                        if val is not None:
                            row.append('{:.2f}'.format(val))
                        else:
                            row.append('-')
                elif metric in MEM_METRICS:
                    row.append('{:.2f}'.format(sample[metric] / 1024))
                else:
                    bug('Unknown metric {}'.format(metric))
            csv_writer.writerow(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-o', '--outfile-prefix', required=False)
    parser.add_argument('--metrics', default=MEM_USED)
    parser.add_argument('--commands', nargs='+')
    parser.add_argument('--max-samples', type=int, default=0)
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG, CSV), default=HTML)

    opts = parser.parse_args()
    validate_opts(opts)

    samples = parse_top(opts)

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
