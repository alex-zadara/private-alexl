#!/bin/bash
#
# Copyright (C) 2011 Mellanox, Ltd. All rights reserved.
# Copyright (C) 2011 Alexander Nezhinsky (alexandern@mellanox.com)
# Copyright (C) 2012 Roi Dayan (roid@mellanox.com)
#

DEF_APP=fio
DEF_BLK_SZ="1 4 8 16 32 64"
DEF_THREADS="2 4"
DEF_TIME=15
DEF_MULT=1
DEF_IODEPTH=1
DEF_ENGINE=libaio
DEF_PATTERNS='write read'
DEF_READ_RATIO=50
DEF_WSLEEP=10
DEF_SCHEDULER="noop"
DEF_PARALLEL=false
DEF_QUIET=false
DEF_GENERATE_LOG=false
DEF_INTERACTIVE=false
DEF_VERBOSE=false
DEF_RAND_SEQ_RUN=0
DEF_NRUNS=1
DEF_MISC_OPTS=

test_app=${DEF_APP}
BLK_SZ=${DEF_BLK_SZ}
THREADS=${DEF_THREADS}
MULT=${DEF_MULT}
IODEPTH=${DEF_IODEPTH}
ENGINE=${DEF_ENGINE}
START_SLEEP=3
RSLEEP=3
WSLEEP=${DEF_WSLEEP}
IO_SCHEDULER=${DEF_SCHEDULER}
GENERATE_LOG=${DEF_GENERATE_LOG}
NRUNS=${DEF_NRUNS}
READ_RATIO=${DEF_READ_RATIO}
MISC_OPTS=${DEF_MISC_OPTS}

rand_seq_run=${DEF_RAND_SEQ_RUN}
parallel=${DEF_PARALLEL}
quiet=${DEF_QUIET}
interactive=${DEF_INTERACTIVE}
verbose=${DEF_VERBOSE}

app_name="$(basename $0)"

export t_name
export t_pattern
export t_misc
export t_sleep

FILES_DIR=io_tests
iostat_file="/tmp/${app_name}.iostat.$$"

# ----------------------------------------------------------------------------
# struct and tables processing
# ----------------------------------------------------------------------------

function struct_create() # params: struct name, fields list
{
	local name=$1
	shift
	local fld=($*) # rest of args is the list of fields
	local i
	local eval_str

	for (( i=0; i<${#fld[*]}; i++ )); do
		# map field index to field name, thru array
		eval_str="${name}_fields[$i]=${fld[$i]}"
		eval $eval_str

		# map field name to index, thru variable
		eval_str="${name}_fld_${fld[$i]}=$i";
		eval $eval_str
	done
}

function struct_fld_name() # params: struct name, field index
{
	local name=$1
	local col=$2
	local eval_str="\${${name}_fields[${col}]}"

	eval echo ${eval_str}
}

function struct_fld_ind() # params: struct name, field name
{
	local name=$1
	local fld_name=$2
	local eval_str="\${${name}_fld_${fld_name}}"

	eval echo ${eval_str}
}

function struct_num_flds() # params: struct name
{
	local name=$1;
	local eval_str="\${#${name}_fields[*]}"

	eval echo ${eval_str}
}

function table_index() # params: struct name, row index, field name
{
	local name=$1;
	local row_ind=$2;
	local fld_name=$3;
	local fld_ind=`struct_fld_ind ${name} ${fld_name}`
	local num_flds=`struct_num_flds ${name}`
	local i;

	(( i=(row_ind*num_flds)+fld_ind ));
	echo $i;
}

function table_get() # params: table var, struct name, row index, field name
{
	local table_var=$1
	local struct=$2
	local row_ind=$3
	local fld_name=$4
	local index=`table_index $struct $row_ind $fld_name`
	local eval_str="\${${table_var}[${index}]}"

	eval echo ${eval_str}
}

function table_row_get() # params: struct var, table var, struct name, row index
{
	local struct_var=$1
	local table_var=$2
	local struct=$3
	local row_ind=$4

	local fld_0_name=`struct_fld_name $struct 0`
	local row_len=`struct_num_flds $struct`
	local index=`table_index $struct $row_ind $fld_0_name`
	local i

	eval "${struct_var}=" # init the struct var
	for ((i=0; i<row_len; i++,index++)); do
		eval "${struct_var}[$i]=\${${table_var}[${index}]}"
	done
}

function table_row_set() # params: table var, struct name, row index, values list
{
	local table_var=$1
	local struct=$2
	local row_ind=$3
	shift 3

	local fld_0_name=`struct_fld_name $struct 0`
	local row_len=`struct_num_flds $struct`
	local index=`table_index $struct $row_ind $fld_0_name`
	local i

	for ((i=0; i<row_len; i++,index++)); do
		eval "${table_var}[${index}]=\$1"
		shift
	done
}

function table_set() # params: table var, struct name, row index, field name, value
{
	local table_var=$1
	local struct=$2
	local row_ind=$3
	local fld_name=$4
	local value=$5

	local index=`table_index $struct $row_ind $fld_name`
	local eval_str="${table_var}[${index}]=\"${value}\""

	eval ${eval_str}
}

# struct processing
# based on tables, a struct can be viewed as a single-row table
# so that the table columns are mapped to the struct fields

function struct_get() # params: struct var, struct name, field name
{
	table_get $1 $2 0 $3
}

function struct_set() # params: struct var, struct name, field name, value
{
	table_set $1 $2 0 $3 $4
}

function struct_list() # params: struct var
{
	local eval_str="\${$1[*]}"
	local lst
	eval lst=${eval_str}
	echo ${lst// /, }
}

# ----------------------------------------------------------------------------

function init_log()
{
	if [[ "${GENERATE_LOG}" == "true" ]]; then
		echo "date, time, test_app, engine" > ${OUT_FILE}
		echo "`date +%d-%b-%Y`, `date +%H:%M:%S`, ${test_app}, ${ENGINE}" >> ${OUT_FILE}
		echo >> ${OUT_FILE}
		echo "pattern, read%, write%, num_devs, dev_name, iodepth, threads, blk_sz_kb, read_kbs, write_kbs, total_kbs, wait_us, cpu_total, cpu_usr, cpu_sys, cpu_iowait" >> ${OUT_FILE}
	fi
}

function do_log()
{
	if [[ "${GENERATE_LOG}" == "true" ]]; then
		echo -e "$@" >> ${OUT_FILE}
	fi
}

CLEARLINE='\033[1K' # clear line from the beginning to the cursor

error()
{
	local err_msg=$1
	if [ -n "${err_msg}" ]; then echo -e "ERROR:\n\t${err_msg}"; fi
}

usage()
{
	error "$1"
	echo -e "usage:"
	echo -e "\t${app_name}"
	echo -e "\t\t<-d devs> <-f files> <-b blk_sizes> <-x patterns> <-t th_nums>"
	echo -e "\t\t<-l time> <-s size> <-m num_inst> <-a app>"
	echo -e "\t\t<-i io_depth> <-e io_engine> <-z wr_wait>"
	echo -e "\t\t<-p> <-o logfile> <-q> <-I> <-h>"
	echo -e "values:"
	echo -e "\t-d : list of [d]evice names (with or without /dev/)"
	echo -e "\t-f : list of [f]ile names"
	echo -e "\t-b : list of io[b]lock sizes, default: \"${DEF_BLK_SZ}\""
	echo -e "\t-x : list of e[x]ecution patterns (in fio notation):"
	echo -e "\t\tread, write, rw, randread, randwrite, randrw"
	echo -e "\t-t : list of [t]thread numbers, default: \"${DEF_THREADS}\""
	echo -e "\t-l : [l]ength of the tests in seconds, default: ${DEF_TIME}s"
	echo -e "\t-s : [s]ize of the test data in KB; may be limited by time"
	echo -e "\t-n : [n]umber of identical runs, default: ${DEF_NRUNS}"
	echo -e "\t-m : num of [M]ultiple instances to run on each device, default: ${DEF_MULT}"
	echo -e "\t-a : benchmark [A]pplication (disktest,fio), default: ${DEF_APP}"
	echo -e "\t-i : [i]/O depth per thread (if supported by app), default: ${DEF_IODEPTH}"
	#echo -e "\t-R : sequential [R]un length before jumping to [R]andom offset,"
	#echo -e "\t\t(if supported by app), default: ${DEF_RAND_SEQ_RUN}"
	echo -e "\t-e : i/o [e]ngine (if supported by app), default: ${DEF_ENGINE}"
	echo -e "\t-r : [r]atio of Reads in r/w mix, default: ${DEF_READ_RATIO}%"
	echo -e "\t-z : Waiting time after write tests, in seconds, default: ${DEF_WSLEEP}s"
	echo -e "\t-p : execute in [p]arallel over all supplied devices, default: ${DEF_PARALLEL}"
	echo -e "\t-Q : use specified [Q]ueue scheduler, default: ${DEF_SCHEDULER}"
	echo -e "\t-o : CSV-format [o]utput file name for statistics, enabled by default: ${DEF_GENERATE_LOG}"
	echo -e "\t-O : extra [O]ptions to pass to the application"
	echo -e "\t-q : [q]uiet mode, only print the output directory name, default: ${DEF_QUIET}"
	echo -e "\t-I : [I]nteractive mode, show performance during the test, default: ${DEF_INTERACTIVE}"
	echo -e "\t-v : [v]erbose mode, show all command lines issued during the test, default: ${DEF_VERBOSE}"
	echo
	echo -e "examples:"
	echo -e "\t$name -d \"sdc /dev/sdd sdf\" -t \"8 16\" -b \"1 16 64\" -x \"randwr write\" -a fio -p";
	echo -e "\t$name -d \"\`./sg_grep -g TH_NULL -v NULL1\`\" -t 16 -b \"32 64\" -x rw -p -z 30";
	echo -e "\t$name -f \"test1 /tmp/test2\" -t \"1 32\" -b \"1 256\" -x \"read write\"";
	exit 1
}

function echo_q()
{
	if [ ${quiet} == false ]; then
		echo "$@"
	fi
}

hr_str="================================================================================"

function print_hr()
{
	ch=${1:-'='};
	echo_q "${hr_str//=/${ch}}"
}

clean_num()
{
	local numstr=$1
	numstr=${numstr/\(/ }
	numstr=${numstr/,/ }
	numstr=${numstr/\)/ }
	echo ${numstr}
}

ls_full_path()
{
	local l
	ls -Rl1 $1 | while read l; do case $l in \
		*:) d=${l%:};; \
		"") d=;; \
		*) echo "$l $d";; \
		esac; done
}

function get_fusion_info()
{
	fio_dev=$1
	if [[ -e /proc/fusion/iodrive && -e /proc/fusion/fio/${fio_dev}/data_device ]]; then
		fct_dev=`cat /proc/fusion/fio/${fio_dev}/data_device | cut -d : -f 1`
		prd_line=`grep Product /proc/fusion/iodrive/${fct_dev}/board_info`

		echo ${prd_line/Product:/}
	fi
}

function set_dev_scheduler()
{
	local dev=$1
	local sched_type=$2
	local nesting=$3

	dev_info=`sg_map -i 2>/dev/null | grep "/dev/${dev} "`;
	if [[ -z ${dev_info} ]]; then
		fusion_line=$(get_fusion_info ${dev})
		if [ -n "${fusion_line}" ]; then
			dev_info="\"${fusion_line}\""
		else
			dev_info="non-SCSI block dev";
		fi
	fi

	sys_sched="/sys/block/${dev}/queue/scheduler";
	if [[ -e ${sys_sched} ]]; then
		echo_q "scheduler ${sched_type} for: ${dev} - ${dev_info}"
		echo ${sched_type} > ${sys_sched};
	else
		echo_q "/dev/$dev (${dev_info}): ${sys_sched} does not exist";
	fi
}

set_scheduler_dm_slaves()
{
	local dm_dev=$1
	local sched_type=$2
	local nesting=$3

	local ndevs=`dmsetup deps /dev/${dm_dev} | awk '{print $1}'`
	local maj
	local min
	local nfield=4
	local slave_dev
	local i
	local k

	echo_q "dm device /dev/${dev}, ${ndevs} slaves:"
	for (( i=0; i<${ndevs}; i++ )); do
		local awk_str="{print \$${nfield}}"
		maj=`dmsetup deps /dev/${dm_dev} 2>/dev/null | awk "${awk_str}"`
		maj=`clean_num ${maj}`
		(( nfield++ ))

		awk_str="{print \$${nfield}}"
		min=`dmsetup deps /dev/${dm_dev} 2>/dev/null | awk "${awk_str}"`
		min=`clean_num ${min}`
		(( nfield++ ))

		slave_dev=`ls_full_path '/dev/*' | grep -E " ${maj},[ \t]*${min} " | awk '{printf "%s/%s",$11,$10}'`
		slave_dev=${slave_dev//\/dev\//}
		slave_dev=${slave_dev#\/}

		for (( k=1; k<nesting; k++ )); do echo_q -en "     "; done
		echo_q -en " --> "
		set_dev_scheduler_by_type ${slave_dev} ${sched_type} ${nesting}
	done
}

function set_dev_scheduler_by_type()
{
	local dev=$1
	local sched_type=$2
	local nesting=$3

	(( nesting++ ))
	set_dev_scheduler "${dev}" "${sched_type}" ${nesting}

	local dm_line=`dmsetup deps /dev/${dev} 2>/dev/null`
	if [ -n "${dm_line}" ]; then
		set_scheduler_dm_slaves "${dev}" "${sched_type}" ${nesting}
	fi
}

function set_all_devs_scheduler()
{
	local sched_type=$1
	local dev

	for dev in ${DEVICES}
	do
		set_dev_scheduler_by_type "${dev}" "${sched_type}" 0
	done
	print_hr
}

function sleep_sec()
{
	local sec=$1

	echo_q -n "sleep ${sec}..."
	sleep ${sec}
	echo_q -n -e "\r"
}



PID_LIST=
watchdog_pid=0
watchdog_acted=0

function kill_all_test_apps()
{
	local pid

	killall -9 ${test_app} > /dev/null 2>&1

	for pid in ${PID_LIST}; do
		kill -9 ${pid} > /dev/null 2>&1
	done
}

function kill_watchdog()
{
	if [[ ${watchdog_pid} > 0 ]]; then
		kill ${watchdog_pid} > /dev/null 2>&1
		watchdog_pid=0
		watchdog_acted=0
	fi
}

control_c()
{
	echo_q -en '\nExiting...\n'
	if [[ -e ${iostat_file} ]]; then
		rm -f ${iostat_file}
	fi
	kill_watchdog
	kill_all_test_apps
	exit $?
}

# trap keyboard interrupt (control-c)
trap control_c SIGINT
trap control_c SIGTERM

function spawn_app()
{
	local app=$1
	shift
	local save_pid

	# pass all remaining params, $@ preserves the original string args
	# and prevents string "flattening"
	${app} "$@" &
	save_pid=$!
	PID_LIST="${PID_LIST} ${save_pid}"
}

function start_watchdog()
{
	watchdog_acted=0
	sleep ${MAX_TIME}
	echo -e "\n${test_app} (pids:${PID_LIST}) takes too long, terminating..."
	watchdog_pid=0
	watchdog_acted=1
	kill_all_test_apps
}

function jobs_running()
{
	local pid
	local nprocs=0

	for pid in ${PID_LIST}; do
		kill -0 ${pid} > /dev/null 2>&1
		if [[ $? == 0 ]]; then
			(( nprocs++ ))
		fi
	done
	return ${nprocs}
}

function wait_for_jobs()
{
	local err=0
	local ret_code
	local pid

	if [[ -n "${MAX_TIME}" ]]; then
		start_watchdog &
		watchdog_pid=$!
		disown ${watchdog_pid} # this supresses termination message
	fi

	for pid in ${PID_LIST}
	do
		wait ${pid}
		ret_code=$?
		if [[ ${ret_code} != 0 ]]; then
			err=${ret_code}
		fi
	done

	kill_watchdog
	PID_LIST=

	return ${err}
}

function run_disktest()
{
	# spawn_app run_disktest ${dev} ${TIME} ${sz} ${thr} \
	#       		"${test_pattern}" "${misc_params}"
	local dev=$1
	shift
	local tsec=$1
	shift
	local blk_sz_kb=$1
	shift
	local thr=$1
	shift
	local pattern="$1"
	shift
	local misc_params="$@"
	if [ "${misc_params}" == "--" ]; then misc_params=; fi
	shift

	local dt_mode="BD"
	if [ -n "${FILES}" ]; then
		dt_mode="FD"
	fi

	disktest_cmd="disktest -PT -h1 -T${tsec} -B${blk_sz_kb}k -K${thr} \
			${dev} -I ${dt_mode} ${pattern} ${misc_params}"

	if [[ "${verbose}" == true ]]; then
		echo -en " command:\n\t"
		echo ${disktest_cmd}
	fi

	${disktest_cmd} > /dev/null

	err=$?
	if [ ${err} != 0 ]; then
		kill -SIGINT $$ # send ctrl-c to self-pid
	fi
	return ${err}
}

function run_fio()
{
	# spawn_app run_fio ${TIME} ${TEST_TOTAL_SZ} ${sz} ${thr} ${IODEPTH} ${ENGINE} \
	#       	    "${test_pattern}" "${misc_params}" ${TEST_DEVS}
	local tsec=$1
	shift
	local total_sz=$1
	shift
	local blk_sz_kb=$1
	shift
	local thr=$1
	shift
	local iodepth=$1
	shift
	local ioengine=$1
	shift
	local pattern=$1
	shift
	local misc_params="$1"
	if [ "${misc_params}" == "--" ]; then misc_params=; fi
	shift

	local fio_names
	local d
	local test_name
	local size_arg
	local time_arg

	for d in $*; do
		if [[ ${tsec} > 0 ]]; then
			test_name="${d}-${pattern}-${blk_sz_kb}k-${thr}thr-${ioengine}-${iod}iodepth-${tsec}sec"
		else
			test_name="${d}-${pattern}-${blk_sz_kb}k-${thr}thr-${ioengine}-${iod}iodepth-${total_sz}_sz"
		fi
		test_name="${test_name//:/_}" # swap all : to -
		test_name="${test_name//\//-}" # swap all / to -
		test_name="${test_name#-}"   # strip leading -

		fio_names="${fio_names} --name ${test_name}"
		fio_names="${fio_names} --filename=${d}"
		if [ -f ${d} ]; then
			fio_names="${fio_names} --direct=0"
		fi
	done

	if [[ ${tsec} -gt 0 ]]; then
		if [[ ${total_sz} -eq 0 ]]; then
			time_arg="--runtime=${tsec} --time_based"
		else
			time_arg="--timeout=${tsec}"
		fi
	fi

	if [[ ${total_sz} -gt 0 ]]; then
		size_arg="--size=${total_sz}k"
	else
		size_arg="--size=${TEST_TOTAL_SZ}k"
	fi

	fio_cmd="fio \
		--rw=${pattern} --bs=${blk_sz_kb}k ${misc_params} \
		--numjobs=${thr} --iodepth=${iod} \
		${time_arg} ${size_arg} --loops=1 \
		--ioengine=${ioengine} --direct=1 --invalidate=1 --fsync_on_close=1 \
		--randrepeat=1 --norandommap --group_reporting --exitall \
		${fio_names}"

	if [[ "${verbose}" == true ]]; then
		echo -en " command:\n\t"
		echo ${fio_cmd}
	fi

	${fio_cmd} > /dev/null

	err=$?
	if [ ${err} != 0 ]; then
		kill -SIGINT $$ # send ctrl-c to self-pid
	fi
	return ${err}
}

function process_iostat_data()
{
	local stat=($1) # make an array
	local total_rd=0;
	local total_wr=0;
	local total_await=0;
	local i

	for ((i=0; i<${#stat[*]}; )); do
		(( total_rd+=stat[i] ));
		(( total_wr+=stat[i+1] ));
		(( total_await+=stat[i+2] ));
		(( i+=3 ))
	done

	total_await=$(echo "scale=0; ${total_await}/${TEST_NDEVS}" | bc -l)
	local total_perf=$((total_rd+total_wr))

	echo "${total_rd} ${total_wr} ${total_perf} ${total_await}"
}

function process_iostat_cpu()
{
	local arr_cpu_stat=($*)

	local cpu_user=`struct_get arr_cpu_stat struct_iostat_cpu user`
	local cpu_nice=`struct_get arr_cpu_stat struct_iostat_cpu niced`
	local cpu_user=$(echo "scale=10; ${cpu_user}+${cpu_nice}" | bc -l)
	local cpu_sys=`struct_get arr_cpu_stat struct_iostat_cpu system`
	local cpu_iowait=`struct_get arr_cpu_stat struct_iostat_cpu iowait`
	local cpu_steal=`struct_get arr_cpu_stat struct_iostat_cpu steal`
	local cpu_iowait=$(echo "scale=10; ${cpu_iowait}+${cpu_steal}" | bc -l)
	local cpu_total=`struct_get arr_cpu_stat struct_iostat_cpu idle`
	local cpu_total=$(echo "scale=10; 100-${cpu_total}" | bc -l) # total=100%-idle

	echo "${cpu_total} ${cpu_user} ${cpu_sys} ${cpu_iowait}"
}

function print_status_line()
{
	local test_descr="$1"
	local total_rd=`printf "%'d" $2`
	local total_wr=`printf "%'d" $3`
	local total_await=`printf "%'d" $4`
	local cpu_info="$5"
	local str

	if [ "${interactive}" == true ]; then
		str="${CLEARLINE}\r${test_descr}"
	fi

	if [[ (${total_rd} == 0) && (${total_wr} == 0) ]]; then
		str="${str} - R/W: 0"
	else
		str="${str} -"
		if [ ${total_rd} != 0 ]; then str="${str} R: ${total_rd}K"; fi
		if [ ${total_wr} != 0 ]; then str="${str} W: ${total_wr}K"; fi
	fi
	str="${str} wait_us:${total_await} ${cpu_info}"
	echo_q -en "${str}"
}

function run_iostat()
{
	local iostat_time=$1
	local tail_lines=$2

	iostat -x -k ${iostat_time} 2 ${TEST_PROC_DEVS} | grep . | \
                        tail -n ${tail_lines} > ${iostat_file}
}

function run_single_test()
{
	local last="$1"; shift;
	local iod="$1"; shift;
	local thr="$1"; shift;
	local blk_sz="$1"; shift;
	local test_pattern_name="$1"; shift;
	local test_pattern_param="$1"; shift;
	local misc_params="$1"; shift;
	local post_sleep="$1"; shift;
	local rd_ratio="$1"; shift;
	local wr_ratio="$1"; shift;
	local test_time # entire test duration
	local iostat_time # iostat long sample length, it's less than test_time to remove ramp-up and ramp-down
	local test_devs
	local tail_lines
	local i

	(( tail_lines=TEST_NDEVS+2 ))

	if [ ${TEST_NDEVS} == 1 ]; then
		test_devs="${TEST_DEVS}"
	else
		test_devs="*"
	fi

	local test_descr="${test_devs} ${test_pattern_name} ${rd_ratio}:${wr_ratio} ${iod}iodepth ${thr}thr ${blk_sz}kb";
	echo_q -en "${CLEARLINE}\r${test_descr}"

	if [ -n "${TIME}" ]; then
		# we will increase the overall time by 2 sec, this alows the overall iostat:
		#    1. measure perf without the ramp-up, ramp-down periods
		#    2. make sure iostat ends before the test-run ends
		(( test_time=TIME+2 ))
		(( iostat_time=TIME ))
	else
		test_time=0
		iostat_time=0
	fi

	# run multiple app instances
	for (( i=0; i<${MULT}; i++ ))
	do
		if [ ${test_app} == "disktest" ]; then
			for dev in ${TEST_DEVS}; do
				spawn_app run_disktest ${dev} ${test_time} ${blk_sz} ${thr} \
					"${test_pattern_param}" "${misc_params}"
			done
		elif [ ${test_app} == "fio" ]; then
			local test_sz

			if [ -n "${SIZE}" ]; then
				(( test_sz=SIZE/thr ))
			else
				test_sz=0
			fi

			spawn_app run_fio ${test_time} ${test_sz} ${blk_sz} ${thr} ${iod} ${ENGINE} \
					"${test_pattern_param}" "${misc_params}" ${TEST_DEVS}
		fi
	done

	# this iostat measures the overall perf during the entire test
	if [ ${iostat_time} -ne 0 ]; then
		spawn_app run_iostat ${iostat_time} ${tail_lines} 
	fi

	local stat
	local retval
	local total_rd
	local total_wr
	local total_perf
	local interval=1
	local njobs
	local avg_rd=0
	local avg_wr=0
	local avg_perf=0
	local avg_await=0
	local iter=0
	local iostat_cpu_str
	local arr_cpu_info

	while true; do
		stat=`iostat -x -k ${interval} 2 ${TEST_PROC_DEVS} | grep . | tail -n ${tail_lines}`

		jobs_running; njobs=$?
		if [[ ${njobs} == 0 ]]; then break; fi

		iostat_cpu_str=`echo "${stat}" | head -n 1`
		arr_cpu_info=(`process_iostat_cpu "${iostat_cpu_str}"`)

		stat=`echo "${stat}" | tail -n ${TEST_NDEVS} | awk '{printf "%d %d %d ", $6 ,$7, 1000*$10}'`
		retval=$(process_iostat_data "${stat}")
		total_rd=`echo "$retval" | awk '{print $1}'`
		total_wr=`echo "$retval" | awk '{print $2}'`
		total_perf=`echo "$retval" | awk '{print $3}'`
		total_await=`echo "$retval" | awk '{print $4}'`

		(( avg_rd=(avg_rd*iter)+total_rd ))
		(( avg_wr=(avg_wr*iter)+total_wr ))
		(( avg_perf=(avg_perf*iter)+total_perf ))
		(( avg_await=(avg_await*iter)+total_await ))
		(( iter++ ))
		(( avg_rd=avg_rd/iter ))
		(( avg_wr=avg_wr/iter ))
		(( avg_perf=avg_perf/iter ))
		(( avg_await=avg_await/iter ))

		if [[ "${interactive}" == "true" ]]; then
			print_status_line "${test_descr}" "${total_rd}" "${total_wr}" "${avg_await}" \
				"cpu tot:`struct_get arr_cpu_info struct_cpu total` usr:`struct_get arr_cpu_info struct_cpu user` sys:`struct_get arr_cpu_info struct_cpu system` iow:`struct_get arr_cpu_info struct_cpu iowait`"
		fi
	done

	wait_for_jobs
	local err=$?
	if [ ${err} != 0 ]; then
		echo -e "\nError, code:${err}, exiting..."
		exit ${err}
	fi

	if [ ${watchdog_acted} != 0 ]; then
		echo "acted"
		return
	fi

	if [ ${iostat_time} -ne 0 ]; then
		iostat_cpu_str=`head -n 1 ${iostat_file}`
		iostat_perf=`tail -n ${TEST_NDEVS} ${iostat_file} | awk '{printf "%d %d %d ", $6 ,$7, 1000*$10}'`
		rm -f ${iostat_file}

		arr_cpu_info=(`process_iostat_cpu "${iostat_cpu_str}"`)

		retval=$(process_iostat_data "${iostat_perf}")
		total_rd=`echo "$retval" | awk '{print $1}'`
		total_wr=`echo "$retval" | awk '{print $2}'`
		total_perf=`echo "$retval" | awk '{print $3}'`
		total_await=`echo "$retval" | awk '{print $4}'`

		print_status_line "${test_descr}" "${total_rd}" "${total_wr}" "${total_await}" \
			"cpu total:`struct_get arr_cpu_info struct_cpu total` usr:`struct_get arr_cpu_info struct_cpu user` sys:`struct_get arr_cpu_info struct_cpu system` iow:`struct_get arr_cpu_info struct_cpu iowait`"

		do_log "${test_pattern_name}, ${rd_ratio}, ${wr_ratio}, ${TEST_NDEVS}, ${test_devs}, ${iod}, ${thr}, ${blk_sz}, ${total_rd}, ${total_wr}, ${total_perf}, ${total_await}, `struct_list arr_cpu_info`"

	else
		print_status_line "${test_descr} - ${iter} s" ${avg_rd} ${avg_wr}
	fi

	echo
	if [[ "${verbose}" == true ]]; then echo; fi

	if [ "${last}" == "false" ]; then
		sleep_sec  ${post_sleep}
	fi
}

function run_tests()
{
	local thr
	local blk_sz
	local pattern
	local n_total_runs
	local n_run
	local last

	((n_total_runs=N_BLK_SZ*N_PATTERNS*NRUNS))

	for iod in ${IODEPTH}
	do
		for thr in ${THREADS}
		do
			n_run=0
			last="false"
			for blk_sz in ${BLK_SZ}
			do
				for (( pattern=0; pattern<${N_PATTERNS}; pattern++ ))
				do
					for (( n=0; n<${NRUNS}; n++ ))
					do
						((n_run++))
						if [[ ${n_run} == ${n_total_runs} ]]; then
							last="true"
						fi
						run_single_test ${last} ${iod} ${thr} ${blk_sz} \
							"${t_name[${pattern}]}" "${t_pattern[${pattern}]}" \
							"${t_misc[${pattern}]}" "${t_sleep[${pattern}]}" \
							"${t_rd_ratio[${pattern}]}" "${t_wr_ratio[${pattern}]}"
					done
				done
				if [ "${last}" == "false" ]; then
					print_hr '.'
				fi
			done
			print_hr
		done
	done
}

function check_app()
{
	case ${test_app} in
		disktest)
			;;
		fio)
			;;
		#lmdd)
		#       ;;
		*)
			usage "Unsupported application: ${test_app}"
			;;
	esac
}

mapper_dev_name()
{
	local full_dev=$1
	local major
	local minor
	local major_minor
	local sys_dev

	major=`ls -lH $full_dev | awk '{print $5}'`
	minor=`ls -lH $full_dev | awk '{print $6}'`
	major_minor="${major}${minor}"
	major_minor=${major_minor//,/:}

	sys_dev=`grep -rl "${major_minor}" /sys/block/dm*/dev`
	sys_dev=${sys_dev##/sys/block/}
	sys_dev=${sys_dev%%/dev}

	echo "${sys_dev}"
}

function device_proc_name()
{
	local full_d=$1
	local dm_line
	local proc_d

	# determine under which name device is listed in /proc/partitions
	dm_line=`dmsetup deps ${full_d} 2>/dev/null`
	if [ -n "${dm_line}" ]; then
		# for mapper devices, need to get the dm-x name to look-up size
		proc_d=`mapper_dev_name ${full_d}`
		if [ -z ${proc_d} ]; then
			error "failed to identify /proc name of ${full_d}"
			exit 1
		fi
	else
		proc_d=${full_d//\/dev\//} # /proc/partitions lists devices w/out /dev
	fi

	# check if actually appears in /proc/partitions
	if [ -z "`grep ${proc_d} /proc/partitions`" ]; then
		error "${proc_d} not found in /proc/partitions"
		exit 1
	fi
	echo "${proc_d}"
}

# should prepare: DEVICES, DEVICES_ARR, NDEVICES, FULL_DEVS, PROC_DEVS, DEV_SZ
function check_devices()
{
	local d
	local full_d
	local sz

	if [ -z "${DEVICES}" ]; then return; fi

	# first strip /dev/ to normalize different forms of input
	DEVICES=${DEVICES//\/dev\// }

	# create full names list and extract information about devices
	DEVICES_ARR=( ${DEVICES} ); # make array
	NDEVICES=${#DEVICES_ARR[*]}; # num of devices = arr size
	for d in ${DEVICES}; do
		full_d="/dev/${d}"
		if [ ! -b ${full_d} ]; then
			error "Not a valid block device: ${full_d}";
                        exit 1
		fi
		# add to list in /dev/* form
		FULL_DEVS="${FULL_DEVS} ${full_d}"

		# add to list of the names used in /proc/partitions
		proc_d=`device_proc_name ${full_d}`
		PROC_DEVS="${PROC_DEVS} ${proc_d}"

		# calculate device size
		sz=`blockdev --getsize64 ${full_d}`
		if [ -z ${sz} ]; then
			error "failed to obtain size of ${full_d}"
                        exit 1
		fi
		# add to list of device sizes
		DEV_SZ="$DEV_SZ${sz} ";
	done
}

function dev_of_file()
{
	df "$1" | head -n 2 | tail -n 1  | awk '{print $1}'
}

# should prepare: FILES, FILES_ARR, NFILES, FULL_DEVS, PROC_DEVS, DEV_SZ
function check_files()
{
	local f
	local fdev
	local proc_d
	local sz

	if [ -z "${FILES}" ]; then return; fi

	# extract information about FILES, create files list and calculate sizes
	FILES_ARR=( ${FILES} ); # make array
	NFILES=${#FILES_ARR[*]}; # num of FILES = arr size
	for f in ${FILES}; do
		if [ -e ${f} ]; then
			# add filename to relevant lists
			FULL_DEVS="${FULL_DEVS} ${f}"
			fdev=`dev_of_file ${f}`
			proc_d=`device_proc_name ${fdev}`
			PROC_DEVS="${PROC_DEVS} ${proc_d}"
			# calculate file size and add to the list
			sz=`ls -lH ${f} | awk '{printf $5}'`
			DEV_SZ="$DEV_SZ${sz} ";
		else
			error "file: ${f} does not exist";
                        exit 1
		fi
	done
}

function calc_total_sz()
{
	if [ -z "${FULL_DEVS}" ]; then
		error "No devices or files supplied";
                exit 1
	fi

	TOTAL_DEV_SZ=0
	for s in ${DEV_SZ}; do
		((TOTAL_DEV_SZ+=${s}));
	done
}

function check_patterns()
{
	local i=0
	local io_pattern

	pattern_list=${pattern_list:-${DEF_PATTERNS}}

	if [[ ${READ_RATIO} -gt 100 || ${READ_RATIO} -lt 0 ]]; then
		error "Read ratio (-r) must be [0..100], value: ${READ_RATIO} is illegal"
                exit 1
	fi
	(( WRITE_RATIO=100-READ_RATIO ))

	for io_pattern in ${pattern_list}; do
		t_name[$i]="${io_pattern}"
		t_misc[$i]="${MISC_OPTS}"

		if [[ "${io_pattern}" == "read" || "${io_pattern}" == "randread" ]]; then
			t_rd_ratio[$i]=100
			t_wr_ratio[$i]=0
		elif [[ "${io_pattern}" == "write" || "${io_pattern}" == "randwrite" ]]; then
			t_rd_ratio[$i]=0
			t_wr_ratio[$i]=100
		else
			t_rd_ratio[$i]=${READ_RATIO}
			t_wr_ratio[$i]=${WRITE_RATIO}
		fi

		if [ ${test_app} == "fio" ]; then
			case ${io_pattern} in
			read)
				t_pattern[$i]="${io_pattern}"
				t_sleep[$i]=${RSLEEP};;
			write)
				t_pattern[$i]="${io_pattern}"
				t_sleep[$i]=${WSLEEP};;
			rw)
				t_pattern[$i]="${io_pattern}"
				t_misc[$i]="${t_misc[$i]} --rwmixread=${READ_RATIO} --rwmixcycle=100"
				t_sleep[$i]=${WSLEEP};;
			randread)
				if [[ ${rand_seq_run} == 0 ]]; then
					t_pattern[$i]="${io_pattern}";
				else
					t_pattern[$i]="${io_pattern}:${rand_seq_run}";
				fi
				t_sleep[$i]=${RSLEEP};;
			randwrite)
				if [[ ${rand_seq_run} == 0 ]]; then
					t_pattern[$i]="${io_pattern}";
				else
					t_pattern[$i]="${io_pattern}:${rand_seq_run}";
				fi
				t_sleep[$i]=${WSLEEP};;
			randrw)
				if [[ ${rand_seq_run} == 0 ]]; then
					t_pattern[$i]="${io_pattern}";
				else
					t_pattern[$i]="${io_pattern}:${rand_seq_run}";
				fi
				t_misc[$i]="${t_misc[$i]} --rwmixread=${READ_RATIO} --rwmixcycle=100"
				t_sleep[$i]=${WSLEEP};;
			*)
				usage "Illegal pattern: ${io_pattern}"
				;;
			esac
		elif [ ${test_app} == "disktest" ]; then
			case ${io_pattern} in
			read)
				t_pattern[$i]="-r"
				t_sleep[$i]=${RSLEEP};;
			write)
				t_pattern[$i]="-w"
				t_sleep[$i]=${WSLEEP};;
			rw)
				t_pattern[$i]="-r -w"
				t_misc[$i]="-D ${READ_RATIO}:${WRITE_RATIO}"
				t_sleep[$i]=${WSLEEP};;
			randread | randwrite | randrw)
				usage "Pattern: ${io_pattern} not supported with disktest"
				;;
			*)
				usage "Illegal pattern: ${io_pattern}"
				;;
			esac
		fi

		if [ -z "${t_misc[$i]}" ]; then
			t_misc[$i]="--"
		fi

		((i++));
	done
	N_PATTERNS=$i
}

function print_header()
{
	if [ "${quiet}" == true ]; then return 0; fi

	echo
	echo "Application : ${test_app}"

	echo "Devices list: ${FULL_DEVS}"
	echo "iostat names: ${PROC_DEVS}"
	echo "Device sizes: ${DEV_SZ}"

	echo -n "Run mode    : "
	if [ "${parallel}" == true ]; then
		echo -n "Parallel for all devices; "
	else
		echo -n "Separate for each device; "
	fi
	if [ "${quiet}" == true ]; then
		echo -n "Quiet; "
	fi
	if [ "${verbose}" == true ]; then
		echo -n "Verbose; "
	fi
	if [ "${interactive}" == true ]; then
		echo -n "Interactive stats; "
	else
		echo -n "Total stats only; "
	fi
	echo

	echo -n "I/O patterns: "
	for ((i=0; i<${#t_name[*]}; i++)); do
		if [[ $i > 0 ]]; then echo -n ", "; fi
		echo -n "${t_name[$i]} (R:W=${t_rd_ratio[$i]}:${t_wr_ratio[$i]})"
	done
	echo

	if [[ -n "${MISC_OPTS}" ]]; then
	echo "Misc.Options: ${MISC_OPTS}"
	fi

	if [[ -z "${SIZE}" ]]; then
	echo "Run by Time : ${TIME} sec"
	else
	local byte_sz
	(( byte_sz=SIZE*1024 ))
	echo -n "Run by Size : ${SIZE} KB (${byte_sz})"
	if [[ -n "${TIME}" ]]; then
		echo -n "; Time limit : ${TIME} sec"
	fi
	echo
	fi

	echo "Threads num : ${THREADS}"
	echo "Block sizes : ${BLK_SZ}"

	if [ ${test_app} == "fio" ]; then
	echo "I/O depth   : ${IODEPTH}"
	echo "I/O engine  : ${ENGINE}"
	fi

	echo "Write sleep : ${WSLEEP} sec"
	if [ "${GENERATE_LOG}" == "true" ]; then
	echo "CSV log file: ${OUT_FILE}"
	fi

	echo
}

if [ $# == 0 ]; then usage; fi

while getopts "a:d:f:t:b:l:s:z:n:m:i:e:x:r:R:Q:o:O:pqIvh" opt
do
	case ${opt} in
	a)
		test_app=$OPTARG;;
	d)
		DEVICES=$OPTARG;;
	f)
		FILES=$OPTARG;;
	t)
		THREADS=$OPTARG;;
	b)
		BLK_SZ=$OPTARG;;
	l)
		TIME=$OPTARG;;
	s)
		SIZE=$OPTARG;;
	z)
		WSLEEP=$OPTARG;;
	n)
		NRUNS=$OPTARG;;
	m)
		MULT=$OPTARG;;
	i)
		IODEPTH=$OPTARG;;
	e)
		ENGINE=$OPTARG;;
	x)
		pattern_list=$OPTARG;;
	r)
		READ_RATIO=$OPTARG;;
	R)
		rand_seq_run=$OPTARG;;
	Q)
		IO_SCHEDULER=$OPTARG;;
	p)
		parallel=true;;
	o)
		GENERATE_LOG=true;
		OUT_FILE=$OPTARG;;
	O)
		MISC_OPTS="${MISC_OPTS} $OPTARG";;
	q)
		quiet=true;;
	I)
		interactive=true;;
	v)
		verbose=true;;
	h*)
		usage;;
	esac
done
# if need to add some more var length args
#shift $(($OPTIND - 1))

# declare struct formats to be used
struct_create struct_iostat_cpu user niced system iowait steal idle # define struct_iostat_cpu
struct_create struct_cpu total user system iowait # define struct_cpu

FULL_DEVS=
check_devices
check_files
calc_total_sz

check_app
check_patterns

if [[ -z "${SIZE}" && -z "${TIME}" ]]; then
	TIME=${DEF_TIME}
fi
if [[ -n "${TIME}" ]]; then
	((MAX_TIME=(3*TIME)/4))
fi

print_header
set_all_devs_scheduler ${IO_SCHEDULER}

# initial sleep, to give a chance to stop
sleep_sec ${START_SLEEP}

# start the tests

init_log

FULL_DEVS_ARR=( ${FULL_DEVS} )
PROC_DEVS_ARR=( ${PROC_DEVS} )
DEV_SZ_ARR=( ${DEV_SZ} )
NDEVS=${#DEV_SZ_ARR[*]}

if [[ ${NDEVS} != ${#PROC_DEVS_ARR[*]} || ${NDEVS} != ${#FULL_DEVS_ARR[*]} ]]; then
	error "Internal error: PROC_DEVS_ARR, FULL_DEVS_ARR, DEV_SZ_ARR sizes not equal"
	exit 1
fi

THREADS_ARR=( ${THREADS} )
N_THREADS=${#THREADS_ARR[*]}

BLK_SZ_ARR=( ${BLK_SZ} )
N_BLK_SZ=${#BLK_SZ_ARR[*]}

if [ "${parallel}" == true ]; then
	TEST_NDEVS=${NDEVS}
	TEST_DEVS="${FULL_DEVS}"
	TEST_PROC_DEVS="${PROC_DEVS}"
	TEST_DEVS_SZ="${DEV_SZ}"
	TEST_TOTAL_SZ=${TOTAL_DEV_SZ}

	run_tests
else
	for (( dev_n=0; dev_n<${NDEVS}; dev_n++ )); do
		TEST_NDEVS=1
		TEST_DEVS="${FULL_DEVS_ARR[dev_n]}"
		TEST_PROC_DEVS="${PROC_DEVS_ARR[dev_n]}"
		TEST_DEVS_SZ="${DEV_SZ_ARR[dev_n]}"
		TEST_TOTAL_SZ="${DEV_SZ_ARR[dev_n]}"

		run_tests

		echo
	done
fi

echo "Tests finished."

