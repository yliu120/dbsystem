########################################
# This is a python script file
# This fill calls Adder class.
########################################

import sys
from adder import Adder

if len(sys.argv) is not 3:
   raise ValueError("please give two command line args.");
else:
   a = int( sys.argv[1] )
   b = int( sys.argv[2] )
   print Adder(a,b).sum()
