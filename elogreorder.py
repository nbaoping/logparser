from base import *
import os
import sys
from operator import itemgetter


__LOG_TFMT = '%m/%d/%Y %H:%M:%S'

def get_file_time( path ):
	fin = open( path, 'r' )
	for line in fin:
		try:
			segs = line.split( '.' )
			tstr = segs[0]
			segs = segs[1].split( '(' )
			msec = int( segs[0] )
			dtime = datetime.strptime( tstr, __LOG_TFMT )
			fin.close()
			return (total_seconds( dtime ), msec)
		except:
			pass
	fin.close()
	return (0, 0)

def untar_files( ipath ):
	for root, dirs, files in os.walk(ipath):
		for fname in files:
			if not fname.endswith( '.gz' ):
				continue
			print 'untar file:', fname
			segs = fname.split( '-' )
			segs = segs[-1].split( '.' )
			nstr = segs[0]
			unpath = os.path.join( root, nstr )
			mkdir( unpath )
			tarpath = os.path.join( root, fname )
			uncmd = 'tar -C ' + unpath + ' -xzf ' + tarpath
			os.system( uncmd )

def get_ordered_list( ipath ):
	loglist = list()
	for root, dirs, files in os.walk(ipath):
		for fname in files:
			print fname
			if fname.endswith('.current'):
				continue
			path = os.path.join( root, fname )
			(time, msec) = get_file_time( path )
			loglist.append( (time, root, fname, msec) )

	return sorted( loglist, key=itemgetter(0) )

def reorder_logs( loglist, ipath, cover ):
	odir = 'reordered'
	opath = os.path.join( ipath, odir )
	mkdir( opath )
	count = 0
	for item in loglist:
		count += 1
		tstr = str_seconds( item[0], '%Y%m%d-%H:%M:%S' ) + '.' + str( item[3] )
		ifile = os.path.join( item[1], item[2] )
		nfname = tstr + '_' + item[2]
		if not cover:
			nfname +=  '.' + str(count)
		ofile = os.path.join( opath, nfname )
		print 'rename file:', ifile, 'to', ofile
		os.rename( ifile, ofile )

def reorder_elog_edir( ipath, cover ):
	loglist = get_ordered_list( ipath )
	reorder_logs( loglist, ipath, cover )

if __name__ == '__main__':
	ipath = sys.argv[1]
	cover = False
	untarFile = False
	if len(sys.argv) > 2 and sys.argv[2] == '-c':
		cover = True
	if len(sys.argv) > 3 and sys.argv[3] == '-x':
		untarFile = True
	print untarFile
	if untarFile:
		untar_files( ipath )
	loglist = get_ordered_list( ipath )
	print loglist
	reorder_logs( loglist, ipath, cover )





