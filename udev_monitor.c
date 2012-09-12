#include <libudev.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

void print_device_props(struct udev_device *dev)
{
	struct udev_list_entry * entry = NULL;
	time_t now = time(NULL);
	char time_str[128] = {'\0'}; // Should be at least 26 bytes according to manpage

	ctime_r(&now, time_str);
	printf("============= %s", time_str); // time_str already includes \n
	
	printf("Devpath[%s], subsystem[%s], devtype[%s], syspath[%s]\n", 
		   udev_device_get_devpath(dev), 
		   udev_device_get_subsystem(dev), 
		   udev_device_get_devtype(dev), 
		   udev_device_get_syspath(dev));
	printf("Sysname[%s], sysnum[%s], devnode[%s], initialized[%d]\n", 
		   udev_device_get_sysname(dev),
		   udev_device_get_sysnum(dev),
		   udev_device_get_devnode(dev),
		   udev_device_get_is_initialized(dev));
	printf("Driver[%s], devnum[%lu], ACTION[%s], seqnum[%llu], usec_init[%llu]\n",
		   udev_device_get_driver(dev),
		   udev_device_get_devnum(dev),
		   udev_device_get_action(dev),
		   udev_device_get_seqnum(dev),
		   udev_device_get_usec_since_initialized(dev));
	

	entry = udev_device_get_properties_list_entry(dev);
	while (entry != NULL) {
		printf("  Prop: %s=%s\n", udev_list_entry_get_name(entry), udev_list_entry_get_value(entry));
		entry = udev_list_entry_get_next(entry);
	}

	entry = udev_device_get_tags_list_entry(dev);
	while (entry != NULL) {
		printf("  Tag: %s\n", udev_list_entry_get_name(entry));
		entry = udev_list_entry_get_next(entry);
	}

	entry = udev_device_get_sysattr_list_entry(dev);
	while (entry != NULL) {
		printf("  Attr: %s=%s\n", udev_list_entry_get_name(entry), udev_list_entry_get_value(entry));
		entry = udev_list_entry_get_next(entry);
	}

	printf("\n");
}

int main (void)
{
	struct udev *udev;
	struct udev_enumerate *enumerate;
	struct udev_list_entry *devices, *dev_list_entry;
	struct udev_device *dev;

   	struct udev_monitor *mon;
	int fd;
	
	/* Create the udev object */
	udev = udev_new();
	if (!udev) {
		printf("Can't create udev\n");
		exit(1);
	}

	/* This section sets up a monitor which will report events when
	   devices attached to the system change.  Events include "add",
	   "remove", "change", "online", and "offline".
	   
	   This section sets up and starts the monitoring. Events are
	   polled for (and delivered) later in the file.
	   
	   It is important that the monitor be set up before the call to
	   udev_enumerate_scan_devices() so that events (and devices) are
	   not missed.  For example, if enumeration happened first, there
	   would be no event generated for a device which was attached after
	   enumeration but before monitoring began.
	   
	   Note that a filter is added so that we only get events for
	   "block" devices. */
	
	/* Set up a monitor to monitor hidraw devices */
	mon = udev_monitor_new_from_netlink(udev, "udev");
	udev_monitor_filter_add_match_subsystem_devtype(mon, "block", NULL);
	udev_monitor_enable_receiving(mon);
	/* Get the file descriptor (fd) for the monitor.
	   This fd will get passed to select() */
	fd = udev_monitor_get_fd(mon);


	/* Create a list of the devices in the 'hidraw' subsystem. */
	enumerate = udev_enumerate_new(udev);
	udev_enumerate_add_match_subsystem(enumerate, "block");
	udev_enumerate_scan_devices(enumerate);
	devices = udev_enumerate_get_list_entry(enumerate);
	/* For each item enumerated, print out its information.
	   udev_list_entry_foreach is a macro which expands to
	   a loop. The loop will be executed for each member in
	   devices, setting dev_list_entry to a list entry
	   which contains the device's path in /sys. */
	udev_list_entry_foreach(dev_list_entry, devices) {
		const char *path;
		
		/* Get the filename of the /sys entry for the device
		   and create a udev_device object (dev) representing it */
		path = udev_list_entry_get_name(dev_list_entry);
		dev = udev_device_new_from_syspath(udev, path);

		print_device_props(dev);
	
		udev_device_unref(dev);
	}
	/* Free the enumerator object */
	udev_enumerate_unref(enumerate);
	
	/* Begin polling for udev events. Events occur when devices
	   attached to the system are added, removed, or change state. 
	   udev_monitor_receive_device() will return a device
	   object representing the device which changed and what type of
	   change occured.

	   The select() system call is used to ensure that the call to
	   udev_monitor_receive_device() will not block.
	   
	   The monitor was set up earler in this file, and monitoring is
	   already underway.
	   
	   This section will run continuously, calling usleep() at the end
	   of each pass. This is to demonstrate how to use a udev_monitor
	   in a non-blocking way. */
	while (1) {
		/* Set up the call to select(). In this case, select() will
		   only operate on a single file descriptor, the one
		   associated with our udev_monitor. Note that the timeval
		   object is set to 0, which will cause select() to not
		   block. */
		fd_set fds;
		struct timeval tv;
		int ret;
		
		FD_ZERO(&fds);
		FD_SET(fd, &fds);
		tv.tv_sec = 0;
		tv.tv_usec = 0;
		
		ret = select(fd+1, &fds, NULL, NULL, &tv);
		
		/* Check if our file descriptor has received data. */
		if (ret > 0 && FD_ISSET(fd, &fds)) {
			/* Make the call to receive the device.
			   select() ensured that this will not block. */
			dev = udev_monitor_receive_device(mon);
			if (dev) {
				print_device_props(dev);
				udev_device_unref(dev);
			}
			else {
				printf("No Device from receive_device(). An error occured.\n");
			}					
		}
		usleep(250*1000);
		fflush(stdout);
	}

	udev_unref(udev);

	return 0;       
}

