from logparser import *
import os
import sys
from operator import itemgetter


class XactParser:
	def __init__( self ):
		self.anlyFactory = AnalyserFactory()
		pass

	def parse( self, args ):
		if args.type = 'extsqu':
			__parse_extsqu( self, args )

	def __parse_extsqu( self, args ):
		print 'parse extsqu translog in', args.path
		parser = WELogParser( WE_XACTLOG_EXT_SQUID_STR )
		files = __stat_files( self, args.path, parser )
		startTime = to_datetime( files[0][0] )
		endTime = to_datetime( files[-1][0] )
		anlyList = self.anlyFactory.create_from_args( args, startTime, endTime )


	def __stat_files( self, path, parser ):
		files = list()
		for root, dirs, files in os.walk( path ):
			for fname in files:
				fpath = os.path.join( root, fname )
				stime = __get_file_stime( self, fpath, parser )
				if stime > 0:
					files.append( (stime, fpath) )
		#sort the list
		return sorted( files, key=itemgetter(0) )

	def __analyse_files( self, path, files, parser, anlyList ):
		for name in files:
			npath = os.path.join( path, name )
			__analyse_file( self, npath, parser, anlyList )

	def __analyse_file( self, path, parser, anlyList ):
		fin = open( path, 'r' )
		first = True
		for line in fin:
			if first:
				first = False
				continue
			logInfo = parser.parse_line( line )
			for anly in anlyList:
				anly.analyse_log( logInfo )
		fin.close()

	#get the first received time
	def __get_file_stime( self, path, parser ):
		fin = open( path, 'r' )
		num = 0
		logInfo = None
		for line in fin:
			if num == 1:
				logInfo = parser.parse_line( line )
				break
			num += 1
		fin.close()
		if logInfo is not None:
			return total_seconds( logInfo.rtime )
		return -1

