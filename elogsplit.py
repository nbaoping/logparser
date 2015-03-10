import os
import sys
import logging

from base import *
from elogreorder import *

def parse_module_name( fileName ):
	idx = fileName.rfind( '.' )
	mname = fileName
	if idx > 0:
		mname = fileName[0:idx]
	if mname.endswith( '000' ):
		return None
	if mname.endswith( 'errorlog' ):
		tail = len(mname) - len('errorlog')
		mname = mname[0:tail]
	elif mname.endswith( 'err' ):
		tail = len(mname) - len('err')
		mname = mname[0:tail]
	elif mname.endswith( 'log' ):
		tail = len(mname) - len('log')
		mname = mname[0:tail]
	if mname.endswith( '_' ) or mname.endswith('-'):
		mname = mname[0:len(mname)-1]
	return mname


def split_error_files( logDir ):
	for fileName in os.listdir(logDir):
		ipath = os.path.join( logDir, fileName )
		if os.path.isfile(ipath):
			mname = parse_module_name( fileName )
			if mname is None or mname==fileName:
				continue

			odir = os.path.join( logDir, mname )
			mkdir( odir )
			opath = os.path.join( odir, fileName )
			logging.info( 'ipath:'+ipath )
			logging.info( 'opath:'+opath )
			os.rename( ipath, opath )


def reorder_error_files( logDir ):
	for fileName in os.listdir(logDir):
		ipath = os.path.join( logDir, fileName )
		if os.path.isdir(ipath):
			reorder_elog_edir( ipath, False )

if __name__ == '__main__':
	logDir = sys.argv[1]
	split_error_files( logDir )
	reorder_error_files( logDir )
			




