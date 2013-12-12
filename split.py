import sys
import logging

from base import *

def split_dict_line( line ):
	line = line.strip()
	segs = line.split( ';' )
	time = segs[0]
	kvstr = segs[1][ 1:-1 ]
	kvsegs = kvstr.split( ',' )
	kvlist = list()
	for kv in kvsegs:
		segs = kv.split( ':' )
		key = segs[0]
		key = key.strip()
		if key[0] == '\'':
			key = key[ 1:-1 ]
		value = segs[1]
		value = value.strip()
		kvlist.append( (key, value) )
	return ( time, kvlist )

def init_kvmap( kvlist ):
	kvmap = dict()
	for kv in kvlist:
		key = kv[0]
		kvmap[key] = '0'
	return kvmap

def split_dict( bfile, srcFile ):
	fbase = open( bfile, 'r' )
	kvlist = None
	for line in fbase:
		(time, kvlist) = split_dict_line( line )
		break
	fbase.close()
	if kvlist is None:
		logging.info( 'file valid:'+str(bfile) )
		return
	fsrc = open( srcFile, 'r' )
	fout = open( srcFile+'.split', 'w' )
	nline = None
	for kv in kvlist:
		if nline is None:
			nline = 'time,' + kv[0]
		else:
			nline += ',' + kv[0]
	logging.debug( nline )
	fout.write( nline + '\n' )
	for line in fsrc:
		(time, tlist) = split_dict_line( line )
		kvmap = init_kvmap( kvlist )
		for kv in tlist:
			kvmap[kv[0]] = kv[1]
		nline = None
		for kv in kvlist:
			key = kv[0]
			if nline is None:
				nline = time + ',' + kvmap[key]
			else:
				nline += ',' + kvmap[key]
		nline += '\n'
		fout.write( nline )
	fsrc.close()
	fout.close()

def scan_dict_files( path ):
	bmap = dict()
	smap = dict()
	for root, dirs, files in os.walk( path ):
		for fname in files:
			if not fname.endswith( '.txt' ):
				continue
			segs = fname.split( '_' )
			if len(segs) < 2:
				continue
			name = segs[0]
			pace = segs[1]
			fpath = os.path.join( root, fname )
			if pace == '-1':
				bmap[name] = fpath
			else:
				smap[name] = fpath
	logging.info( bmap )
	logging.info( smap )
	for name in bmap.keys():
		if name in smap:
			bpath = bmap[name]
			spath = smap[name]
			logging.debug( 'split dict file'+spath )
			split_dict( bpath, spath )

if __name__ == '__main__':
	scan_dict_files( sys.argv[1] )


