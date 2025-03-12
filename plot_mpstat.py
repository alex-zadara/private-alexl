#!/usr/bin/env python3

import os
import sys
import argparse
import re
import csv
import datetime
import plotly.express as px


def bug(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    assert False


FLOAT_STR = r'[0-9\.]+'

# 14:32:33     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
# 14:32:34     all    0.34    0.34    0.67    0.34    0.00    0.00    0.00    0.00    0.00   98.32
MPSTAT_ALL = re.compile(r'^(\d\d:\d\d:\d\d)\s+(all)\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     r'(' + FLOAT_STR + r')' + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     r'(' + FLOAT_STR + r')')

MPSTAT_EACH = re.compile(r'^(\d\d:\d\d:\d\d)\s+(\d+)\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     r'(' + FLOAT_STR + r')' + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     FLOAT_STR + r'\s+' +
                     r'(' + FLOAT_STR + r')')

MPSTAT_CPU_STR = r'^(\d\d:\d\d:\d\d)\s+({})\s+' +\
                     FLOAT_STR + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     r'(' + FLOAT_STR + r')' + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     FLOAT_STR + r'\s+' +\
                     r'(' + FLOAT_STR + r')'

HTML = 'html'
JPEG = 'jpeg'
CSV = 'csv'


def validate_opts(opts):
    if opts.cpu == 'all' or opts.cpu == 'each':
        pass
    else:
        cpu_int = int(opts.cpu)
        opts.cpu = cpu_int

    if opts.max_samples_per_file < 0:
        bug('max_samples_per_file should be 0 or positive')

    if opts.fig_title is None:
        opts.fig_title = opts.metric


def parse_mpstat(opts):
    regexp = None
    if opts.cpu == 'all':
        regexp = MPSTAT_ALL
    elif opts.cpu == 'each':
        regexp = MPSTAT_EACH
    else:
        regexp = re.compile(MPSTAT_CPU_STR.format(opts.cpu))

    samples = []
    curr_sample = None

    with open(opts.infile, 'r') as fin:
        for line in fin:
            m = regexp.match(line)
            if m is not None:
                curr_ts = curr_sample['ts'] if curr_sample is not None else None
                ts = datetime.datetime.strptime(m.group(1), '%H:%M:%S')
                if ts != curr_ts:
                    curr_sample = {'ts': ts, 'cpus': []}
                    samples.append(curr_sample)
                    curr_ts = ts

                cpu = m.group(2)

                if opts.metric == 'cpu_usage':
                    value = 100 - float(m.group(4))
                elif opts.metric == 'iowait':
                    value = float(m.group(3))
                else:
                    bug('Unsupported metric: {}'.format(opts.metric))
                new_tpl = (cpu, value)

                # In some cases mpstat produces two lines with the same timestamp for the same CPU.
                # In this case, replace the previous one.
                cpus_list = curr_sample['cpus']
                idx = 0
                dup_found = False
                for tpl in cpus_list:
                    if tpl[0] == cpu:
                        dup_found = True
                        break
                    idx = idx + 1
                if dup_found:
                    cpus_list[idx] = new_tpl
                else:
                    cpus_list.append(new_tpl)

    print('Total {} samples'.format(len(samples)))
    return samples


def do_csv(in_dir_name, out_name, samples):
    file_nr = 1
    need_new_file = True
    printed_samples = 0
    outf = None
    csv_writer = None

    for sample in samples:
        if need_new_file:
            if opts.max_samples_per_file == 0 or len(samples) <= opts.max_samples_per_file:
                outfile = os.path.join(in_dir_name, '{}.{}'.format(out_name, 'csv'))
            else:
                outfile = os.path.join(in_dir_name, '{}.{:04d}.{}'.format(out_name, file_nr, 'csv'))
            outf = open(outfile, 'w')

            csv_writer = csv.writer(outf)

            header_row = ['timestamp']
            # use the first sample to produce list of CPUs
            list_of_tuples = samples[0]['cpus']
            for t in list_of_tuples:
                header_row.append('cpu_{}'.format(t[0]))
            csv_writer.writerow(header_row)

            need_new_file = False

        # print the current sample
        row = [sample['ts'].strftime('%H:%M:%S')]
        list_of_tuples = sample['cpus']
        for t in list_of_tuples:
            row.append('{:.2f}'.format(t[1]))
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


def do_plotly(in_dir_name, out_name, samples):
    total_samples = len(samples)
    total_added_samples = 0

    file_nr = 1
    need_new_file = True
    added_samples = 0

    timestamps = None
    data = None
    y = None

    for sample in samples:
        if need_new_file:
            # Prepare an input for plotly: produce a column for the X-axis (timestamp) and a column for each metric
            timestamps = []
            data = {'timestamp': timestamps}
            y = []
            # use the first sample to produce list of CPUs
            list_of_tuples = samples[0]['cpus']
            for t in list_of_tuples:
                cpu = t[0]
                data[cpu] = []
                y.append(cpu)

            need_new_file = False

        # instead of real timestamp, append sample index; with timestamps the graph looks messy
        timestamps.append(total_added_samples)

        list_of_tuples = sample['cpus']
        for t in list_of_tuples:
            data[t[0]].append(t[1])

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
                outfile = os.path.join(in_dir_name, '{}.{}'.format(out_name, opts.output_format))
            else:
                # multiple files
                outfile = os.path.join(in_dir_name, '{}.{:04d}.{}'.format(out_name, file_nr, opts.output_format))

            if opts.output_format == HTML:
                fig.write_html(outfile)
            elif opts.output_format == JPEG:
                fig.write_image(outfile)
            else:
                bug('Unsupported output format [{}]'.format(opts.output_format))

            # reset stuff
            file_nr = file_nr + 1
            need_new_file = True
            added_samples = 0
            timestamps = None
            data = None
            y = None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--infile', required=True)
    parser.add_argument('-o', '--outfile-prefix', required=False)
    parser.add_argument('--cpu', type=str, default='all')
    parser.add_argument('--metric', choices=('cpu_usage', 'iowait'), default='cpu_usage')
    parser.add_argument('--max-samples-per-file', type=int, default=0)
    parser.add_argument('--fig-title')
    parser.add_argument('-f', '--output-format', choices=(HTML, JPEG, CSV), default=HTML)

    opts = parser.parse_args()

    validate_opts(opts)

    samples = parse_mpstat(opts)
    if samples:
        in_proper_name = os.path.realpath(opts.infile)
        in_dir_name = os.path.dirname(in_proper_name)
        in_base_name = os.path.basename(in_proper_name)
        if opts.outfile_prefix is not None:
            out_name = opts.outfile_prefix
        else:
            out_name = in_base_name

        if opts.output_format in (HTML, JPEG):
            do_plotly(in_dir_name, out_name, samples)
        elif opts.output_format == CSV:
            do_csv(in_dir_name, out_name, samples)
        else:
            bug('Invalid output format {}'.format(opts.output_format))
