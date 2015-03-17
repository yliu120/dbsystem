import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

N = 3;
# the x locations for each groups
ind = np.arange(N)
width = 0.25;

# Data are approximates
dbMeans = (3,173,70);
bnljMeans = (2000,2000,2000);
hashMeans = (50,250,1800);

menStd = (0, 0, 0);
#rects1 = ax.bar(ind, dbMeans, width, color='r', yerr=menStd);
#rects1 = ax.bar(ind, bnljMeans, width, color='b', yerr=menStd);
#rects1 = ax.bar(ind, hashMeans, width, color='g', yerr=menStd);

fig, ax = plt.subplots();

rects1 = ax.bar(ind, dbMeans, width, color='r', yerr=menStd);
rects2 = ax.bar(ind + width, bnljMeans, width, color='b', yerr=menStd);
rects3 = ax.bar(ind + width * 2, hashMeans, width, color='g', yerr=menStd);

fig = plt.gcf();
fig.set_size_inches(12,7);

ax.set_ylabel('Times (Second)');
ax.set_title('Performance Benchmark of DBSQL and CodeBase');
ax.set_xticks(ind + width);
ax.set_xticklabels(('Query1','Query2','Query3'))

ax.legend( (rects1[0], rects2[0], rects3[0]), ('DBSQL','BNLJ','Hash') );

def autolabel(rects, inf):
    # attach some text labels
    for rect in rects: 
        height = rect.get_height()
        if inf:
          ax.text(rect.get_x()+rect.get_width()/2., 0.95*height, '>%d'%int(height),
                ha='center', va='bottom')
        else:          
          ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')


autolabel(rects1, False);
autolabel(rects2, True);
autolabel(rects3, False);

plt.savefig('hw2-benchmark' + '.png');

