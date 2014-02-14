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
        '{0}__O{1}_r{2}_s{3}_x{4}_e{5}'.format(time.strftime("%Y-%m-%d__%H-%M-%S"),
            opts.outstanding, opts.readpct, opts.seekpct, opts.xfersize,
            opts.elapsed))

def prepare_vdbench_input_file(fname, opts):
    with open(fname, 'w') as f:
        f.write('sd=sd1,lun={0},threads={1},openflags=o_direct\n'.format(opts.blkdev, opts.outstanding))
        f.write('wd=wd1,sd=(sd1),rdpct={0},seekpct={1},xfersize={2}k\n'.format(opts.readpct, opts.seekpct, opts.xfersize))
        f.write('rd=run_vdbench,wd=(wd1),iorate=max,elapsed={0},interval=1\n'.format(opts.elapsed))
        f.flush()

def main():
    parser = argparse.ArgumentParser(description='Run basic vdbench test on a block device')
    parser.add_argument('blkdev', help='block device to run the test on')
    parser.add_argument('-O', '--outstanding', type=int, default=32, help='number of outstanding IOs')
    parser.add_argument('-r', '--readpct', type=int, default=0, help='read percentage')
    parser.add_argument('-s', '--seekpct', type=int, default=100, help='seek percentage (random vs sequential)')
    parser.add_argument('-x', '--xfersize', type=int, default=4, help='transfer size in KB')
    parser.add_argument('-e', '--elapsed', type=int, default=30, help='duration of the test')
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

