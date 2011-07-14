#!/bin/bash

print_usage()
{
	echo "Usage:"
	echo "$0 [-w working_tree_path] [-f change_set_file - to use instead of git status] [-g git_repo_path] dest_dir"
}

create_changeset_file()
{
	local dest_dir=$1
	local change_set_filename=$dest_dir/`basename $dest_dir`.changeset
	
	# Create or refresh the changeset file
	if [ -f $change_set_filename ]
	then
		rm $change_set_filename
	fi
	touch $change_set_filename
	
	# Get the changes from git
	cd $GIT_REPO_PATH
	local changed_files=`git status | egrep 'modified:|new file:' | cut -d ':' -f 2`
	
	# Fill the changeset file
	for filename in $changed_files
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
		else
			echo "Copying [$WORKING_TREE_PATH/$filename] to [$DEST_DIR/$filename]..."
			if cp $WORKING_TREE_PATH/$filename $DEST_DIR/$file_rel_path
			then
				num_copied=`expr $num_copied + 1`
			else
				exit 1
			fi
		fi
	done
	
	echo "Totally copied $num_copied file(s)."
}

#################################### main ###############################################

# Global variables
WORKING_TREE_PATH=.
GIT_REPO_PATH=.
USER_CHANGESET_FILENAME=
DEST_DIR=

while getopts 'w:g:f:' OPTNAME
do
case $OPTNAME in
	w)  WORKING_TREE_PATH="$OPTARG"
		;;
	g)  GIT_REPO_PATH="$OPTARG"
		;;
	f) USER_CHANGESET_FILENAME="$OPTARG"
		;;  
esac
done

# Get rid of all the options
shift $((OPTIND-1))

# Check parameters
DEST_DIR=$1
if [ ! "$DEST_DIR" ] || [ ! "$WORKING_TREE_PATH" ] || [ ! "$GIT_REPO_PATH" ]
then
	print_usage
	exit 1
fi


# Create the destination directory
mkdir -p $DEST_DIR

if [ "$USER_CHANGESET_FILENAME" ]
then
	echo "Using user-supplied changeset file [$USER_CHANGESET_FILENAME]..."
	copy_changed_files $USER_CHANGESET_FILENAME
else
	echo "Generating changeset file..."
	changeset_filename=`create_changeset_file $DEST_DIR`
	copy_changed_files $changeset_filename
fi 
