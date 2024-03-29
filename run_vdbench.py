#! /usr/bin/env python

from __future__ import print_function
import sys
import os
import platform
import time
import argparse
import re
import tempfile
import subprocess

def usage_and_exit(msg, parser):
    print(msg, file=sys.stderr)
    parser.print_help(file=sys.stderr)
    sys.exit(1)

VDBENCH_PLATFORM_NAME = {
    'Linux' : 'vdbench',
    'Windows' : 'vdbench.bat'
}
def vdbench_platform_name():
    name = VDBENCH_PLATFORM_NAME.get(platform.system())
    if name is None:
        name = 'vdbench'
    return name

def gen_out_dir(opts):
    return os.path.join(opts.out_root_dir,
        '{}__O{}_r{}_s{}_x{}'.format(time.strftime("%Y-%m-%d__%H-%M-%S"),
            opts.outstanding, opts.readpct, opts.seekpct, opts.xfersize))

RANGE_RE=re.compile(r'^\(\d+[mgt]\,\d+[mgt]\)$')
MAXDATA_RE=re.compile(r'^\d+[kmgt]$')

def validate_simple_optional_attr(opts, attr_name, rexp):
    attr_val = getattr(opts, attr_name)
    if attr_val is None:
        return 0
    if rexp.match(attr_val) is not None:
        return 0
    return 1

# how much data is transferred for each I/O operation
# allows (k)ilo and (m)ega bytes
SINGLE_XFER_SIZE_RE=re.compile(r'^\d+[km]?$')

# distribution of data transfer sizes: pairs of transfer size and percentages
# the total of the percentages must add up to 100
DISTRI_XFER_SIZE_RE=re.compile(r'^\(\d+[km]?\,\d+(\,\d+[km]?,\d+)*\)$')

# uses three values: xfersize=(min,max,align)
# this causes a random value between "min" and "max", with a multiple of "align"
# this also requires the use of the SD align= parameter
# the "align" value is in bytes and must be a multiple of 512
RANDOM_XFER_SIZE_RE=re.compile(r'^\(\d+[km]?\,\d+[km]?\,(\d+)\)$')

def validate_xfersize(opts):
    setattr(opts, 'align', None)

    if SINGLE_XFER_SIZE_RE.match(opts.xfersize) is not None:
        # print("SINGLE")
        return 0

    if DISTRI_XFER_SIZE_RE.match(opts.xfersize) is not None:
        # print("DISTRI")
        return 0

    m = RANDOM_XFER_SIZE_RE.match(opts.xfersize)
    if m is not None:
        opts.align = m.group(1)
        # print("RANDOM, align={}".format(opts.align))
        return 0

    return 1

DEDUPUNIT_RE=re.compile(r'^\d+k$')

def validate_dedupunit(opts):
    if DEDUPUNIT_RE.match(opts.dedupunit) is not None:
        return 0
    return 1

DATA_VALIDATION_TYPES = {
    'v'  : 'Activate data validation',
    'vr' : 'Activate data validation, immediately re-read after each write',
    'vw' : 'Activate data validation, but don\'t read before write',
    'vt' : 'Activate data validation, keep track of each write timestamp (memory intensive)',
}

def prepare_vdbench_input_file_and_cmdline_args(fname, opts):
    cmdline_args = []

    with open(fname, 'w') as f:
        # We always want this, as we want to fail on any IO or data validation error
        f.write('data_errors=1\n')

        # Data validation
        if opts.validation is not None:
            validation_opt = '-' + opts.validation
            f.write('# Data validation will be enabled via \'{}\' command-line option\n'.format(validation_opt))
            f.write('# This means: {}\n'.format(DATA_VALIDATION_TYPES[opts.validation]))
            cmdline_args.append(validation_opt)
        else:
            f.write('# Data validation is not performed\n')

        # Compression and dedup
        if opts.compratio > 1:
            f.write('compratio={}\n'.format(opts.compratio))
        if opts.dedupratio > 1:
            f.write('dedupratio={}\n'.format(opts.dedupratio))
            f.write('dedupunit={}\n'.format(opts.dedupunit))

        # SD - one for each block device
        sd_idx = 0
        for blkdev in opts.blkdevs:
            sd = 'sd=sd{},lun={},threads={},hitarea=0,openflags=directio'.format(sd_idx, blkdev, opts.outstanding)
            if opts.align is not None:  # relevant only when random transfer size is requested
                sd = sd + ',align={}'.format(opts.align)
            sd = sd + '\n'
            f.write(sd)
            sd_idx = sd_idx + 1

        # WD - one definition for all block devices used
        wd = 'wd=wd1,sd=(sd*),rdpct={},seekpct={},rhpct=0,whpct=0,xfersize={}'.format(opts.readpct, opts.seekpct, opts.xfersize)
        if opts.range is not None:
            wd = wd + ',range={}'.format(opts.range)
        wd = wd + '\n'
        f.write(wd)

        # RD
        rd = 'rd=run_vdbench,wd=(wd1),iorate={},elapsed={},interval={}'.format(opts.iorate, opts.elapsed, opts.interval)
        if opts.maxdata is not None:
            rd = rd + ',maxdata={}'.format(opts.maxdata)
        rd = rd + '\n'
        f.write(rd)
        f.flush()

    return cmdline_args

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run basic vdbench test on a block device')
    parser.add_argument('blkdevs', nargs='+', help='block devices to run the test on')
    parser.add_argument('-O', '--outstanding', type=int, default=32, help='number of outstanding IOs, default is 32')
    parser.add_argument('-r', '--readpct', type=int, default=0, help='read percentage, default is 0')
    parser.add_argument('-s', '--seekpct', type=int, default=100, help='seek percentage (random vs sequential), default is 100')
    parser.add_argument('-x', '--xfersize', type=str, default="4k", help='transfer size like "16k" or "(min,max,align)", default is 4k')
    parser.add_argument('-R', '--range', type=str, default=None, help='Range of the block device to cover by the test, like "(1g,2g)", default is to cover the whole block device')
    parser.add_argument('-c', '--compratio', type=int, default=1, help='Expected compression ratio, default is 1 (i.e. no compression)')
    parser.add_argument('-d', '--dedupratio', type=int, default=1, help='Expected dedupe ratio, default is 1 (i.e. no dedupe)')
    parser.add_argument('-u', '--dedupunit', type=str, default="8k", help='dedupe unit size like "16k", default is 8k; relevant only when dedupratio is > 1')
    parser.add_argument('-i', '--iorate', default='max', help='IOPS for the test, can be \'max\' or a number, default is \'max\'')
    parser.add_argument('-e', '--elapsed', type=int, default=30, help='duration of the test in seconds, default is 30')
    parser.add_argument('-m', '--maxdata', type=str, default=None, help='Stop after this amount of data, like "50g", by default not enabled')
    parser.add_argument('-I', '--interval', type=int, default=1, help='vdbench printing interval in seconds, default is 1 sec')
    parser.add_argument('-V', '--validation', type=str, default=None, choices=DATA_VALIDATION_TYPES.keys(), help='Whether to perform data validation, by default disabled')
    parser.add_argument('-o', '--out_root_dir', default=os.getcwd(), help='directory in which auto-generated output directories will be created')
    parser.add_argument('--exact_out_dir', help='exact directory, in which test output will be (overrides --out_root_dir)')
    parser.add_argument('-v', '--vdbench', default=os.path.join(os.getcwd(), vdbench_platform_name()), help='path to the vdbench run-script')
    parser.add_argument('-N', '--dry-run', action='store_true', help='Do not actually run vdbench')
    opts = parser.parse_args()

    # check params
    if opts.outstanding <= 0:
        usage_and_exit('--oustanding must be positive', parser)
    if opts.readpct < 0 or opts.readpct > 100:
        usage_and_exit('--readpct must be between 0 and 100 (including)', parser)
    if opts.seekpct < 0 or opts.seekpct > 100:
        usage_and_exit('--seekpct must be between 0 and 100 (including)', parser)
    if validate_xfersize(opts) != 0:
        usage_and_exit('--xfersize value {} is invalid'.format(opts.xfersize), parser)
    if validate_simple_optional_attr(opts, 'range', RANGE_RE) != 0:
        usage_and_exit('--range value {} is invalid'.format(opts.range), parser)
    if opts.compratio < 1:
        usage_and_exit('--compratio value {} is invalid, must be >= 1'.format(opts.compratio), parser)
    if opts.dedupratio < 1:
        usage_and_exit('--dedupratio value {} is invalid, must be >= 1'.format(opts.dedupratio), parser)
    if opts.dedupratio > 1:
        # dedupunit is relevant only when dedupratio is enabled
        if validate_dedupunit(opts) != 0:
            usage_and_exit('--dedupunit value {} is invalid'.format(opts.dedupunit), parser)
    if opts.iorate != 'max':
        try:
            val = int(opts.iorate)
            if val <= 0:
                raise Exception('iorate is not positive')
        except Exception:
            usage_and_exit('--iorate must be \'max\', or a positive integer', parser)
    if opts.elapsed <= 0:
        usage_and_exit('--elapsed must be positive', parser)
    if validate_simple_optional_attr(opts, 'maxdata', MAXDATA_RE) != 0:
        usage_and_exit('--maxdata value {} is invalid'.format(opts.maxdata), parser)
    if opts.interval <= 0:
        usage_and_exit('--interval must be positive', parser)

    # figure out the output dir for the test
    out_dir = ""
    if opts.exact_out_dir is not None and len(opts.exact_out_dir) > 0:
        out_dir = opts.exact_out_dir
    else:
        out_dir = gen_out_dir(opts)

    # prepare vdbench input file
    os_fd, fname = tempfile.mkstemp(prefix='__vdbench_input_', dir=os.getcwd(), text=True)
    os.close(os_fd)
    addtnl_cmdline_args = prepare_vdbench_input_file_and_cmdline_args(fname, opts)
    cmdline_args = [opts.vdbench]
    cmdline_args.extend(addtnl_cmdline_args)
    cmdline_args.extend(('-f', fname, '-o', out_dir))

    print()
    print('== Going to run vdbench with the following input ({}): =='.format(fname))
    with open(fname, 'r') as f:
        for line in f:
            print(line, end='')
    print()
    print('== vdbench command-line: ==')
    print(' '.join(cmdline_args))
    print()
    print('== Output will be in: {}'.format(out_dir))
    print()

    if not opts.dry_run:
        # run vdbench
        subp_obj = subprocess.Popen(cmdline_args)
        subp_obj.communicate()
        # move the input file to the output directory
        os.rename(fname, os.path.join(out_dir, os.path.basename(fname)))
    else:
        print('Dry run: not running vdbench')
        # remove the input file
        os.unlink(fname)

