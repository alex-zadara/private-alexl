#!/usr/bin/env python

from __future__ import print_function

import sys
import time
import os
import argparse
import re
import csv
import subprocess

def end(exit_rc):
    sys.exit(exit_rc)

def error(msg):
    print('ERROR: {}'.format(msg), file=sys.stderr)
    end(1)

def run_cmd(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while p.poll() == None:
        time.sleep(0.1)

    stdout, stderr = p.communicate()

    return p.returncode, stdout.splitlines(), stderr.splitlines()

def run_cmd_success(cmd):
    rc, stdout, stderr = run_cmd(cmd)
    if rc != 0:
        error('Command failed:\n{}\nstdout:\n{}\nstderr:\n{}'.format(cmd, stdout, stderr))
    return stdout, stderr

############################################################################

IO_SIZES_STR = ('512', '1k', '2k', '4k', '8k', '16k', '32k', '64k', '128k', '256k', '512k', '1024k')
IO_SIZES_BYTES = (512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576)
QUEUE_SIZES = (1, 16, 32, 256)

############################################################################

WRITE_SECTION_RE = re.compile(r'^\s+write: IOPS=')
READ_SECTION_RE = re.compile(r'^\s+read: IOPS=')

LAT_RE = re.compile(r'^\s+lat \((msec|usec|nsec)\): .+, avg=\s*([0-9\.]+),')

BW_RE = re.compile(r'^\s+bw \(\s*(K|M)iB/s\): .+, avg=([0-9\.]+),')

IOPS_RE = re.compile(r'^\s+iops\s*: .+, avg=([0-9\.]+),')

def parse_fio_output_file(fpath):
    if not os.path.isfile(fpath):
        error('{} does not exist'.format(fpath))
    print('Parsing {}'.format(fpath))

    res = {'read'  : {'lat_ms' : None, 'bw_kbsec' : None, 'iops' : None},
           'write' : {'lat_ms' : None, 'bw_kbsec' : None, 'iops' : None}
          }
    section = None

    with open(fpath, 'r') as fio_fin:
        for line in fio_fin:
            m = WRITE_SECTION_RE.match(line)
            if m is not None:
                section = 'write'
                continue
            m = READ_SECTION_RE.match(line)
            if m is not None:
                section = 'read'
                continue

            m = LAT_RE.match(line)
            if m is not None:
                if section is None:
                    error('{}: Seeing latency line:\n{}\nnot in in read/write section'.format(fpath, line))
                if res[section]['lat_ms'] is not None:
                    error('{}: {} latency already parsed, current line:\n{}'.format(fpath, section, line))
                lat_str = m.group(2)
                lat = float(lat_str)
                if m.group(1) == 'usec':
                    lat = lat / 1000
                elif m.group(1) == 'nsec':
                    lat = lat / (1000*1000)
                lat = '{:.3f}'.format(lat)
                res[section]['lat_ms'] = lat

            m = BW_RE.match(line)
            if m is not None:
                if section is None:
                    error('{}: Seeing bandwidth line:\n{}\nnot in in read/write section'.format(fpath, line))
                if res[section]['bw_kbsec'] is not None:
                    error('{}: {} bandwidth already parsed, current line:\n{}'.format(fpath, section, line))
                bw_str = m.group(2)
                bw = float(bw_str)
                if m.group(1) == 'M':
                    bw = bw * 1024
                bw = '{:.3f}'.format(bw)
                res[section]['bw_kbsec'] = bw

            m = IOPS_RE.match(line)
            if m is not None:
                if section is None:
                    error('{}: Seeing iops line:\n{}\nnot in in read/write section'.format(fpath, line))
                if res[section]['iops'] is not None:
                    error('{}: {} iops already parsed, current line:\n{}'.format(fpath, section, line))
                iops_str = m.group(1)
                iops = '{:.0f}'.format(float(iops_str))
                res[section]['iops'] = iops

    read_lat =   res['read']['lat_ms']
    read_bw =    res['read']['bw_kbsec']
    read_iops =  res['read']['iops']
    write_lat =  res['write']['lat_ms']
    write_bw =   res['write']['bw_kbsec']
    write_iops = res['write']['iops']
    ok_read = (read_lat is None and read_bw is None and read_iops is None) or\
              (read_lat is not None and read_bw is not None and read_iops is not None)
    ok_write = (write_lat is None and write_bw is None and write_iops is None) or\
               (write_lat is not None and write_bw is not None and write_iops is not None)
    if not (ok_read and ok_write):
        error('{}: partial data seen:\n{}'.format(fpath, res))

    return read_lat, read_bw, read_iops, write_lat, write_bw, write_iops

############################################################################

def open_csv_file(fpath, is_read):
    head, tail = os.path.split(fpath)
    fpath = os.path.join(head, '{}_{}'.format('rd' if is_read else 'wr', tail))

    csvf = open(fpath, 'wb', 0)
    csv_writer = csv.writer(csvf)
    csv_writer.writerow(('IO size (bytes)', 'queue depth', 'latency (ms)', 'bw (kb/sec)', 'IOPs'))
    return csvf, csv_writer

def dir_to_csv(opts):
    read_csvf = None
    read_csv_writer = None
    write_csvf = None
    write_csv_writer = None

    for io_size_str, io_size_bytes in zip(IO_SIZES_STR, IO_SIZES_BYTES):
        for queue_size in QUEUE_SIZES:
            fio_fpath = os.path.join(opts.dir, '{}_{}.fio'.format(io_size_str, queue_size))
            read_lat, read_bw, read_iops, write_lat, write_bw, write_iops = parse_fio_output_file(fio_fpath)
            if read_lat is not None:
                if read_csvf is None:
                    read_csvf, read_csv_writer = open_csv_file(opts.outfile, True)
                read_csv_writer.writerow((io_size_bytes, queue_size, read_lat, read_bw, read_iops))
            if write_lat is not None:
                if write_csvf is None:
                    write_csvf, write_csv_writer = open_csv_file(opts.outfile, False)
                write_csv_writer.writerow((io_size_bytes, queue_size, write_lat, write_bw, write_iops))

    if read_csvf is not None:
        read_csvf.close()
    if write_csvf is not None:
        write_csvf.close()

############################################################################

MIXED_RWS = ('rw', 'readwrite', 'randrw')

def run_fio_loop(opts):
    # create the output directory for the run
    if os.path.exists(opts.outdir):
        error('{} already exists'.format(opts.outdir))
    os.makedirs(opts.outdir)

    if opts.io_pattern in MIXED_RWS:
        if not (opts.readpct > 0 and opts.readpct < 100):
            error('For mixed IO patterns, readpct must be in (0,100)')

    for io_size_str, io_size_bytes in zip(IO_SIZES_STR, IO_SIZES_BYTES):
        for queue_size in QUEUE_SIZES:
            fio_out_fpath = os.path.join(opts.outdir, '{}_{}.fio'.format(io_size_str, queue_size))
            cmd = ['fio',
                   '--output={}'.format(fio_out_fpath),
                   '--rw={}'.format(opts.io_pattern),
                   '--bs={}'.format(io_size_bytes),
                   '--numjobs=1',
                   '--iodepth={}'.format(queue_size),
                   '--runtime={}'.format(opts.runtime),
                   '--time_based',
                   '--size=100%',
                   '--loops=1',
                   '--ioengine=libaio',
                   '--direct=1',
                   '--invalidate=1',
                   '--fsync_on_close=1',
                   '--randrepeat=1',
                   '--norandommap',
                   '--group_reporting',
                   '--exitall'
                  ]
            if opts.io_pattern in MIXED_RWS:
                cmd.append('--rwmixread={}'.format(opts.readpct))
            cmd.append('--name={}_{}'.format(io_size_str, queue_size))
            cmd.append('--filename={}'.format(opts.bdev))

            print('{}: IO size {}, queue depth {}'.format(opts.bdev, io_size_str, queue_size))
            run_cmd_success(cmd)

############################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Performance matrix with fio')
    subparsers = parser.add_subparsers()

    sub_parser = subparsers.add_parser('dir_to_csv', help='Parse fio loop results and convert to csv')
    sub_parser.add_argument('--dir', required=True)
    sub_parser.add_argument('--outfile', required=True)
    sub_parser.set_defaults(func=dir_to_csv)

    sub_parser = subparsers.add_parser('run_fio_loop', help='Run fio with different sizes/queue depth and parse the results')
    sub_parser.add_argument('--io-pattern', choices=('read','write','randread','randwrite','rw','readwrite','randrw'), default='write')
    sub_parser.add_argument('--readpct', type=int, default=50)
    sub_parser.add_argument('--runtime', default=72)
    sub_parser.add_argument('--bdev', required=True)
    sub_parser.add_argument('--outdir', required=True)
    sub_parser.set_defaults(func=run_fio_loop)

    opts = parser.parse_args()

    opts.func(opts)

    end(0)

