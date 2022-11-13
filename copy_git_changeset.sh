#!/bin/bash

print_usage()
{
	echo "Usage:"
	echo "$0 [-s] [-w working_tree_path] [-f change_set_file] dest_dir"
	echo "    -s:    copy only staged files (has no effect when -f is used)"
	echo "    -f:    use the specified changeset file, instead of asking git which files changed"
}

create_changeset_file()
{
	local dest_dir=$1
	local staged_only=$2
	local change_set_filename=$dest_dir/`basename $dest_dir`.changeset

	# Create or refresh the changeset file
	if [ -f $change_set_filename ]
	then
		rm $change_set_filename
	fi
	touch $change_set_filename
	
	# Get the changes from git
	local changed_files=
	local changed_files2=
	if [ "$staged_only" = "0" ]; then
		changed_files=`git status | egrep 'modified:|new file:' | cut -d ':' -f 2`
		changed_files2=`git status | egrep "renamed:" | egrep -o "\-> .+" | cut -d' ' -f2`
	else
		# Note that the below command has TAB as delimiter; this is also the default delimiter of "cut"
		changed_files=`git diff --cached --name-status  | cut  -f2`
	fi

	# Fill the changeset file
	for filename in $changed_files
	do
		# Add the filename to the changeset
		echo $filename >> $change_set_filename
	done
	for filename in $changed_files2
	do
		# Add the filename to the changeset
		echo $filename >> $change_set_filename
	done

	# Return the filename
	echo $change_set_filename
}

copy_changed_files()
{
	local change_set_filename=$1
	local changed_files=`cat $change_set_filename`
	
	# Start copying files
	local num_copied=0
	
	for filename in $changed_files
	do
		local file_rel_path=`dirname $filename`
		local filename_only=`basename $filename`
		
		mkdir -p $DEST_DIR/$file_rel_path
		
		if [ -f $DEST_DIR/$filename ]
		then
			echo "[$DEST_DIR/$filename] already exists"
			continue
		fi
		
		if [ ! -f $WORKING_TREE_PATH/$filename ]
		then
			echo "[$WORKING_TREE_PATH/$filename] not found, skipping"
			continue
		fi

		echo "Copying [$WORKING_TREE_PATH/$filename] to [$DEST_DIR/$filename]..."
		if cp $WORKING_TREE_PATH/$filename $DEST_DIR/$file_rel_path
		then
			num_copied=`expr $num_copied + 1`
		else
			exit 1
		fi
	done
	
	echo "Totally copied $num_copied file(s)."
}

#################################### main ###############################################

# Global variables
ONLY_STAGED_FILES=0
WORKING_TREE_PATH=.
USER_CHANGESET_FILENAME=
DEST_DIR=

while getopts 'sw:f:' OPTNAME
do
case $OPTNAME in
	s)  ONLY_STAGED_FILES=1
		;;
	w)  WORKING_TREE_PATH="$OPTARG"
		;;
	f) USER_CHANGESET_FILENAME="$OPTARG"
		;;  
esac
done

# Get rid of all the options
shift $((OPTIND-1))

# Check parameters
DEST_DIR=$1
if [ ! "$DEST_DIR" ] || [ ! "$WORKING_TREE_PATH" ]
then
	print_usage
	exit 1
fi


# Create the destination directory
WORKING_DIR_CANON=`readlink -f $WORKING_TREE_PATH`
DEST_DIR="$DEST_DIR/`date +%Y-%m-%d__%H-%M-%S`_`basename $WORKING_DIR_CANON`"
if [ -e "$DEST_DIR" ]
then
    echo "Directory: $DEST_DIR already exists"
    exit 1
fi
mkdir -p $DEST_DIR

# Information about which branch we are in and what is the top commit
git branch -v -v -a > $DEST_DIR/existing.branches__
git log -1 > $DEST_DIR/top.commit__

if [ "$USER_CHANGESET_FILENAME" ]
then
	echo "Using user-supplied changeset file [$USER_CHANGESET_FILENAME]..."
	copy_changed_files $USER_CHANGESET_FILENAME
else
	echo "Generating changeset file..."
	if [ "$ONLY_STAGED_FILES" -ne "0" ]; then
		echo "(only files staged for commit)"
	fi
	changeset_filename=`create_changeset_file $DEST_DIR $ONLY_STAGED_FILES`
	copy_changed_files $changeset_filename
fi 
