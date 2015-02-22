#!/usr/bin/python
########################################
# This file takes input from stdin
# And draws the pyplot
########################################
import sys
import numpy as np
matplotlib.use('Agg')
import matplotlib.pyplot as plt

pageTitle = sys.argv[1]

sf = np.arange(0.0, 1.1, 0.1)
data_time = []
size = []
# data_tuplenum = []
for line in sys.stdin:
	strlist = line.rstrip().split(":")
#    if strlist[0] is "Tuples" :
#		data_tuplenum.append(int(strlist[1]))
	if strlist[0] == "Execution time":
		data_time.append(float(strlist[1]))
	if strlist[0] == "Size":
		size.append( float(strlist[1]) );

plt.figure(1)
# plt.subplot(211)
plt.xlabel('Scale Factor')
plt.ylabel('Time(seconds)')
plt.title('Time Taken to Run Workload' + pageTitle);

lines = plt.plot(sf, data_time[0:11], 'r^-', 
         sf, data_time[11:22], 'b^-',
         sf, data_time[22:33], 'g^-',
         sf, data_time[33:44], 'k^-',
         sf, data_time[44:55], 'ro-',
         sf, data_time[55:66], 'bo-',
         sf, data_time[66:77], 'go-',
         sf, data_time[77:], 'ko-')

plt.setp(lines[0], label = '4KB Page, Mode 1')
plt.setp(lines[1], label = '4KB Page, Mode 2')
plt.setp(lines[2], label = '4KB Page, Mode 3')
plt.setp(lines[3], label = '4KB Page, Mode 4')
plt.setp(lines[4], label = '32KB Page, Mode 1')
plt.setp(lines[5], label = '32KB Page, Mode 2')
plt.setp(lines[6], label = '32KB Page, Mode 3')
plt.setp(lines[7], label = '32KB Page, Mode 4')


plt.legend(loc='upper left', fontsize = 'x-small', ncol=2, borderaxespad=0.)
"""
plt.subplot(212)
plt.xlabel('Scale Factor')
plt.ylabel('Size(kilobytes)')
plt.title('Size of Storage Files After Running Workload')
plt.plot(sf, np.cos(2*np.pi*t2), 'r--')
"""
plt.savefig('timefig-' + pageTitle + '.png');

plt.figure(1);
plt.xlabel('Scale Factor')
plt.ylabel('Storage File Size (kB)')
plt.title('Storage File Size - Workload: ' + pageTitle);

lines = plt.plot(sf, size[0:11], 'r^-', 
         sf, size[11:22], 'b^-',
         sf, size[22:33], 'g^-',
         sf, size[33:44], 'k^-',
         sf, size[44:55], 'ro-',
         sf, size[55:66], 'bo-',
         sf, size[66:77], 'go-',
         sf, size[77:], 'ko-')

plt.setp(lines[0], label = '4KB Page, Mode 1')
plt.setp(lines[1], label = '4KB Page, Mode 2')
plt.setp(lines[2], label = '4KB Page, Mode 3')
plt.setp(lines[3], label = '4KB Page, Mode 4')
plt.setp(lines[4], label = '32KB Page, Mode 1')
plt.setp(lines[5], label = '32KB Page, Mode 2')
plt.setp(lines[6], label = '32KB Page, Mode 3')
plt.setp(lines[7], label = '32KB Page, Mode 4')


plt.legend(loc='upper left', fontsize = 'x-small', ncol=2, borderaxespad=0.)
"""
plt.subplot(212)
plt.xlabel('Scale Factor')
plt.ylabel('Size(kilobytes)')
plt.title('Size of Storage Files After Running Workload')
plt.plot(sf, np.cos(2*np.pi*t2), 'r--')
"""
plt.savefig('sizefig-' + pageTitle + '.png');


