#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


import os
import sys
from operator import itemgetter
import time

from analyser import *
from logparser import *
from factory import *


class XactParser:
	def __init__( self ):
		self.anlyFactory = AnalyserFactory()
		self.anlyList = None
		pass

	def parse( self, args ):
		if args.type == 'extsqu':
			self.__parse_extsqu( args )
	
	def close( self ):
		if self.anlyList is not None:
			for anly in self.anlyList:
				anly.close()

	def __parse_extsqu( self, args ):
		print 'parse extsqu translog in', args.path
		fmt = WE_XACTLOG_EXT_SQUID_STR
		if args.fmt is not None:
			fmt = args.fmt
		print 'translog format:', fmt
		parser = WELogParser( fmt, args.fieldParser )
		files = self.__stat_files( args.path, parser )
		if len(files) == 0:
			print 'no translog files, please check the logs path:', args.path
			return
		startTime = files[0][0]
		endTime = files[-1][0]
		print 'all files>> start time:', str_seconds(startTime), 'end time:', str_seconds(endTime)
		anlyList = self.anlyFactory.create_from_args( args, startTime, endTime )
		self.anlyList = anlyList
		files = self.__sample_files( files, anlyList )
		if len(files) == 0:
			print 'no file need to be parsed'
			return
		startTime = files[0][0]
		endTime = files[-1][0]
		print 'sampled files>> start time:', str_seconds(startTime), 'end time:', str_seconds(endTime)
		self.__analyse_files( files, parser, anlyList )
		for anly in anlyList:
			anly.close()

	def __sample_files( self, fileList, anlyList ):
		stime = -1
		etime = -1
		for anly in anlyList:
			if anly.startTime > 0:
				if stime < 0 or anly.startTime < stime:
					stime = anly.startTime
			else:
				stime = -1
				break
		for anly in anlyList:
			if anly.endTime > 0:
				if etime < 0 or anly.endTime > etime:
					etime = anly.endTime
			else:
				etime = -1
				break
		print '-----', stime, etime
		#sample the files based on the time range
		if stime < 0 and etime < 0:
			return fileList
		sidx = -1
		eidx = 0
		size = len(fileList)
		print fileList[0][0], fileList[-1][0]
		while eidx < size:
			time = fileList[eidx][0]
			if stime > 0 and sidx < 0 and time > stime:
				sidx = eidx
				if etime < 0:
					break
			if etime > 0 and time > etime:
				break
			eidx += 1
		if sidx < 0:
			sidx = 0
		if etime <= 0:
			eidx = size
		print sidx, eidx
		print fileList[sidx][0]
		return fileList[ sidx:eidx ]


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
		print 'total ', len(files), ' files to be analyzed'
		for item in files:
			count += 1
			path = item[1]
			tstr = str_seconds( item[0] )
			print 'analyse the', str(count), 'th file--> [', tstr, ']', path
			start = time.time()
			self.__analyse_file( path, parser, anlyList )
			elapsed = time.time() - start
			print '===============================:', elapsed * 1000, 'ms'

	def __analyse_file( self, path, parser, anlyList ):
		fin = open( path, 'r' )
		first = True
		for line in fin:
			line = line.strip()
			if len(line) == 0:
				continue
			if line[0] == '#':
				continue
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
			line = line.strip()
			if len(line) == 0:
				continue
			if line[0] == '#':
				continue
			if num == 1:
				logInfo = parser.parse_line( line )
				break
			num += 1
		fin.close()
		if logInfo is not None:
			return logInfo.recvdTime
		return -1

