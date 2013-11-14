import os
import sys
import shutil

from base import *



def collect_output( logDir ):
	cdir = os.path.join( logDir, 'alloutput' )
	mkdir( cdir )
	for root, dirs, files in os.walk(logDir):
		bname = os.path.basename( root )
		if bname != 'output':
			continue

		segs = splitall( root )
		mname = segs[-2]
		for fileName in files:
			ipath = os.path.join( root, fileName )
			opath = os.path.join( cdir, mname+'_'+fileName )
			print ipath
			print '\t-->', opath
			shutil.copyfile( ipath, opath )

if __name__ == '__main__':
	logDir = sys.argv[1]
	collect_output( logDir )

