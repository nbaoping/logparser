from operator import itemgetter
import sys
import os

from base import *


def init_sort_list( hline, cline ):
	hsegs = hline.split( ';' )
	csegs = cline.split( ';' )
	if len(hsegs) != len(csegs):
		logging.error( 'hline cline mismatch' )
		return None

	clist = list()
	i = 0
	for head in hsegs:
		if i > 0:
			val = float( csegs[i] )
			clist.append( (head, val) )
		i += 1
	
	return clist

def dump_sort_list( opath, clist ):
	fout = open( opath, 'w' )
	for (head, val) in clist:
		line = head + ';' + str(val) + '\n'
		fout.write( line )
	fout.close()

def sort_map( ipath ):
	logging.debug( 'begin sort:'+ipath )
	opath = ipath + '.sort'
	fin = open( ipath, 'r' )
	hline = fin.readline().strip().strip(';')
	cline = fin.readline().strip().strip(';')
	if len(hline) == 0 or len(cline) == 0:
		logging.error( 'invalid file content in file:'+ipath )
		return
	fin.close()

	clist = init_sort_list( hline, cline )
	clist = sorted( clist, key=itemgetter(1), reverse=True )
	dump_sort_list( opath, clist )

def sort_map_in_dir( idir ):
	for fname in os.listdir(idir):
		fpath = os.path.join( idir, fname )
		if not os.path.isfile(fpath):
			continue

		fin = open( fpath, 'r' )
		count = 0
		canSort = True
		for line in fin:
			count += 1
			if count > 2:
				canSort = False
				break
		fin.close()
		if canSort:
			sort_map( fpath )


def print_usage():
	print 'usage:'
	print '\tpostprocess.py -t <parse type>'

def main():
	init_logging( logging.DEBUG )
	if len(sys.argv) < 2:
		print_usage()
		return

	ptype = sys.argv[2]
	if ptype == 'sort':
		sort_map_in_dir( sys.argv[3] )

if __name__ == '__main__':
	main()

