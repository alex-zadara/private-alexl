#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <asm/types.h>

#define MD_MAJOR 9
#include "mdadm-3.1.4/md_u.h"
#include "mdadm-3.1.4/md_p.h"

void print_usage_and_die(int argc, char* argv[])
{
	fprintf(stderr, "Bad parameters\n");
	exit(1);
}

int main(int argc, char* argv[])
{
	int fd = 0;
	mdu_array_info_t array;
	mdu_disk_info_t disks[384];
	unsigned int idx = 0;
	unsigned int next_spare_idx = 0;
	
	if (argc < 2)
		print_usage_and_die(argc, argv);

	fd = open(argv[1], O_RDONLY);
	if (fd < 0)
	{
		int err = errno;
		fprintf(stderr, "Cannot open [%s]: %s\n", argv[1], strerror(err));
		exit(1);
	}

	memset(&array, 0, sizeof(array));
	memset(&disks, 0, sizeof(disks));
	for (idx = 0; idx < 384; ++idx)
	{
		disks[idx].number = idx;
		disks[idx].major = disks[idx].minor = 0;
		disks[idx].raid_disk = idx;
		disks[idx].state = (1<<MD_DISK_REMOVED);
	}

	if (ioctl(fd, GET_ARRAY_INFO, &array)<0)
	{
		int err = errno;
		if (err == ENODEV)
			fprintf(stderr, "device %s does not appear to be active.\n", argv[1]);
		else
			fprintf(stderr, "cannot get array detail for %s: %s\n", argv[1], strerror(err));
		close(fd);
		exit(1);
	}

	next_spare_idx = array.raid_disks;

	for (idx = 0; idx < 384; ++idx)
	{
		mdu_disk_info_t disk_info;
		disk_info.number = idx;
		if (ioctl(fd, GET_DISK_INFO, &disk_info) < 0)
		{
			if (idx < array.raid_disks)
				fprintf(stderr, "cannot get device detail for RAID device %d: %s\n", idx, strerror(errno));
			else
				fprintf(stderr, "cannot get device detail for spare device %d: %s\n", idx, strerror(errno));
			continue;
		}
		if (disk_info.major == 0 && disk_info.minor == 0) /* Disk with this number is not found */
			continue;
		
		if (disk_info.raid_disk >= 0 && disk_info.raid_disk < array.raid_disks) /* This disk is part of the array (not outside the array) */
			disks[disk_info.raid_disk] = disk_info;
		else if (next_spare_idx < 384) /* This disk is outside the array */
			disks[next_spare_idx++] = disk_info;
	}


	printf("MD Array [%s]:\n", argv[1]);
	printf("level=%d, nr_disks=%d, raid_disks=%d, md_minor=%d\n", array.level, array.nr_disks, array.raid_disks, array.md_minor);
	printf("clean_state=0x%x, active_disks=%d, working_disks=%d, failed_disks=%d, spare_disks=%d\n", 
		   array.state, array.active_disks, array.working_disks, array.failed_disks, array.spare_disks);

	for (idx = 0; idx < array.raid_disks; ++idx)
	{
		if (disks[idx].major == 0 && disks[idx].minor == 0)
			printf("ARRAY DISK %d: MISSING\n", idx);
		else
			printf("ARRAY DISK %d: <%d:%d> [%s %s %s %s] \n", idx, disks[idx].major, disks[idx].minor,
			       (disks[idx].state & 1<<MD_DISK_FAULTY) ? "FAULTY" : "",
			       (disks[idx].state & 1<<MD_DISK_ACTIVE) ? "ACTIVE" : "",
			       (disks[idx].state & 1<<MD_DISK_SYNC)   ? "SYNC"   : "",
			       (disks[idx].state & 1<<MD_DISK_REMOVED)? "REMOVED": "");
	}
	for (idx = array.raid_disks; idx < 384; ++idx)
	{
		if (disks[idx].major == 0 && disks[idx].minor == 0)
			continue;
		printf("OUTSIDE DISK %d: <%d:%d> [%s %s %s %s] \n", disks[idx].raid_disk, disks[idx].major, disks[idx].minor,
			   (disks[idx].state & 1<<MD_DISK_FAULTY) ? "FAULTY" : "",
			   (disks[idx].state & 1<<MD_DISK_ACTIVE) ? "ACTIVE" : "",
			   (disks[idx].state & 1<<MD_DISK_SYNC)   ? "SYNC"   : "",
			   (disks[idx].state & 1<<MD_DISK_REMOVED)? "REMOVED": "");
	}

	close(fd);

	return 0;
}

