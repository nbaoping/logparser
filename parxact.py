from logparser import *
import os
import sys
from operator import itemgetter
from analyser import *


class XactParser:
	def __init__( self ):
		self.anlyFactory = AnalyserFactory()
		pass

	def parse( self, args ):
		if args.type == 'extsqu':
			self.__parse_extsqu( args )

	def __parse_extsqu( self, args ):
		print 'parse extsqu translog in', args.path
		fmt = WE_XACTLOG_EXT_SQUID_STR
		if args.fmt is not None:
			fmt = args.fmt
		print 'translog format:', fmt
		parser = WELogParser( fmt )
		files = self.__stat_files( args.path, parser )
		startTime = files[0][0]
		endTime = files[-1][0]
		print 'start time:', startTime, 'end time:', endTime
		anlyList = self.anlyFactory.create_from_args( args, startTime, endTime )
		self.__analyse_files( files, parser, anlyList )
		for anly in anlyList:
			anly.close()


	def __stat_files( self, path, parser ):
		fileList = list()
		print 'stat files in:', path
		for root, dirs, files in os.walk( path ):
			if RES_DIR in dirs:
				dirs.remove( RES_DIR )
			for fname in files:
				if fname.endswith( '.status' ):
					continue
				#print 'stat file', root, fname
				fpath = os.path.join( root, fname )
				#try:
				stime = self.__get_file_stime( fpath, parser )
				if stime > 0:
					fileList.append( (stime, fpath) )
				#except:
				#	print 'stat file not support', fpath
		#sort the list
		return sorted( fileList, key=itemgetter(0) )

	def __analyse_files( self, files, parser, anlyList ):
		count = 0
		for item in files:
			count += 1
			path = item[1]
			print 'analyse the', str(count), 'th file>>', path
			self.__analyse_file( path, parser, anlyList )

	def __analyse_file( self, path, parser, anlyList ):
		fin = open( path, 'r' )
		first = True
		for line in fin:
			if first:
				first = False
				continue
			if line[0] == '#':
				continue
			line = line.strip()
			if len(line) == 0:
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
				line = line.strip()
				if len(line) == 0:
					continue
				logInfo = parser.parse_line( line )
				break
			num += 1
		fin.close()
		if logInfo is not None:
			return logInfo.rtime
		return -1

