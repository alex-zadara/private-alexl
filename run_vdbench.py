#! /usr/bin/env python

from __future__ import print_function
import sys
import os
import time
import argparse
import tempfile
import subprocess

def usage_and_exit(msg, parser):
    print(msg, file=sys.stderr)
    parser.print_help(file=sys.stderr)
    sys.exit(1)

def gen_out_dir(opts):
    return os.path.join(opts.out_root_dir, 
        '{0}__O{1}_r{2}_s{3}_x{4}_i{5}_e{6}'.format(time.strftime("%Y-%m-%d__%H-%M-%S"),
            opts.outstanding, opts.readpct, opts.seekpct, opts.xfersize,
            opts.iorate, opts.elapsed))

def prepare_vdbench_input_file(fname, opts):
    with open(fname, 'w') as f:
        sd_idx = 0
        for blkdev in opts.blkdevs:
            f.write('sd=sd{0},lun={1},threads={2},openflags=o_direct\n'.format(sd_idx, blkdev, opts.outstanding))
            sd_idx = sd_idx + 1
        f.write('wd=wd1,sd=(sd*),rdpct={0},seekpct={1},xfersize={2}k\n'.format(opts.readpct, opts.seekpct, opts.xfersize))
        f.write('rd=run_vdbench,wd=(wd1),iorate={0},elapsed={1},interval=1\n'.format(opts.iorate, opts.elapsed))
        f.flush()

def main():
    parser = argparse.ArgumentParser(description='Run basic vdbench test on a block device')
    parser.add_argument('blkdevs', nargs='+', help='block devices to run the test on')
    parser.add_argument('-O', '--outstanding', type=int, default=32, help='number of outstanding IOs, default is 32')
    parser.add_argument('-r', '--readpct', type=int, default=0, help='read percentage, default is 0')
    parser.add_argument('-s', '--seekpct', type=int, default=100, help='seek percentage (random vs sequential), default is 100')
    parser.add_argument('-x', '--xfersize', type=int, default=4, help='transfer size in KB, default is 4')
    parser.add_argument('-i', '--iorate', default='max', help='IOPS for the test, can be \'max\', \'curve\' or a number, default is \'max\'')
    parser.add_argument('-e', '--elapsed', type=int, default=30, help='duration of the test in seconds, default is 30')
    parser.add_argument('-o', '--out_root_dir', default=os.getcwd(), help='directory in which auto-generated output directories will be created')
    parser.add_argument('--exact_out_dir', help='exact directory, in which test output will be (overrides --out_root_dir)')
    parser.add_argument('-v', '--vdbench', default=os.path.join(os.getcwd(), 'vdbench'), help='path to the vdbench run-script')
    opts = parser.parse_args()

    # check params
    if opts.outstanding <= 0:
        usage_and_exit('--oustanding must be positive', parser)
    if opts.readpct < 0 or opts.readpct > 100:
        usage_and_exit('--readpct must be between 0 and 100 (including)', parser)
    if opts.seekpct < 0 or opts.seekpct > 100:
        usage_and_exit('--seekpct must be between 0 and 100 (including)', parser)
    if opts.xfersize <= 0:
        usage_and_exit('--xfersize must be positive', parser)
    if opts.elapsed <= 0:
        usage_and_exit('--elapsed must be positive', parser)
    if opts.iorate != 'max' and opts.iorate != 'curve':
        try:
            val = int(opts.iorate)
            if val <= 0:
                raise Exception('iorate is not positive')
        except Exception:
            usage_and_exit('--iorate must be \'max\', \'curve\' or a positive integer', parser)

    # figure out the output dir for the test
    out_dir = ""
    if opts.exact_out_dir is not None and len(opts.exact_out_dir) > 0:
        out_dir = opts.exact_out_dir
    else:
        out_dir = gen_out_dir(opts)

    # prepare vdbench input file
    os_fd, fname = tempfile.mkstemp(prefix='run_vdbench_', dir=os.getcwd(), text=True)
    prepare_vdbench_input_file(fname, opts)
    print()
    print('== Going to run vdbench with the following input ({0}): =='.format(fname))
    print('== Output will be in: {0}'.format(out_dir))
    with open(fname, 'r') as f:
        for line in f:
            print(line, end='')
    print()

    # run vdbench
    args = [opts.vdbench, '-f', fname, '-o', out_dir]
    subp_obj = subprocess.Popen(args)
    subp_obj.communicate()

    os.unlink(fname)

main()

