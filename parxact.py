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
import traceback

from analyser import *
from logparser import *
from factory import *
from errorlog import *


class XactParser:
	def __init__( self ):
		self.anlyFactory = AnalyserFactory()
		self.anlyList = None
		pass

	def parse( self, args ):
		logType = args.type
		if logType == 'translog':
			self.__parse_translog( args )
		elif logType == 'errorlog':
			args.sorted = True
			self.__parse_errorlog( args )
	
	def close( self ):
		if self.anlyList is not None:
			for anly in self.anlyList:
				anly.close()

	def __parse_errorlog( self, args ):
		print 'parse errorlog in', args.path
		parser = ErrorlogParser()
		if args.inputType == 'stdin':
			self.__parse_stdin( args, parser )
		else:
			self.__parse_logs( args, parser )

	def __parse_translog( self, args ):
		print 'parse translog in', args.path
		fmt = WE_XACTLOG_EXT_SQUID_STR
		if args.fmt is not None:
			fmt = args.fmt
		print 'translog format:', fmt
		parser = WELogParser( fmt, args.fieldParser, args.fmtType )
		if args.inputType == 'stdin':
			self.__parse_stdin( args, parser )
		else:
			self.__parse_logs( args, parser )

	def __parse_stdin( self, args, parser ):
		anlyList = self.anlyFactory.create_from_args( args, -1, -1)
		self.anlyList = anlyList
		self.__analyse_stdin( parser, anlyList )
		for anly in anlyList:
			anly.close()

	def __parse_logs( self, args, parser ):
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
		if files is None or len(files) == 0:
			print 'no file need to be parsed'
			return
		startTime = files[0][0]
		endTime = files[-1][0]
		print 'sampled files>> start time:', str_seconds(startTime), 'end time:', str_seconds(endTime)
		self.__analyse_files( files, parser, anlyList )
		for anly in anlyList:
			anly.close()

	def __parse_anly_time_ranges( self, anlyList ):
		timeList = list()
		eflag = False
		#generate the time list
		for anly in anlyList:
			stime = anly.startTime
			if stime < 0:
				stime = -1
			etime = anly.endTime
			if etime <= 0:
				etime = -1
			timeList.append( (stime, etime) )
				
		#sort the time list
		timeList = sorted( timeList, key=itemgetter(0) )
		self.__print_range_list( timeList )

		rangeList = list()
		stime = -10000
		etime = -10000
		for item in timeList:
			if stime == -10000:
				stime = item[0]
				etime = item[1]
			elif etime < 0:				#it covers all the rest range, break here
				rangeList.append( (stime, etime) )
				stime = -10000
				break
			else:
				s = item[0]
				e = item[1]
				if s > etime:		#we got a complete range here
					rangeList.append( (stime, etime) )
					stime = -10000
				elif e < 0 or e > etime:		#merge the two ranges
					etime = e
		if stime != -10000:
			rangeList.append( (stime, etime) )

		return rangeList

	def __sample_files( self, fileList, anlyList ):
		rangeList = self.__parse_anly_time_ranges( anlyList )
		self.__print_range_list( rangeList )

		tarList = None
		for rg in rangeList:
			stime = rg[0]
			etime = rg[1]
			clist = self.__find_files_in_range( fileList, stime, etime )
			if len(clist) == 0:
				print 'error, no files for range(',  stime, ',', etime, ')'
				continue
			if tarList is None:
				tarList = clist
			else:
				#merge clist and tarList
				if clist[0][0] >= tarList[-1][0]:
					tarList += clist
				else:
					idx = 0
					size = len(tarList)
					fitem = clist[0]
					while idx < size:
						if tarList[idx] == fitem:
							break
						idx += 1
					tarList = tarList[0:idx] + clist

		return tarList

	def __print_range_list( self, rangeList ):
		ss = None
		print 'time range list'
		for rg in rangeList:
			sstr = str_seconds( rg[0] )
			estr = str_seconds( rg[1] )
			if rg[1] < 0:
				estr = '-1'
			tmp = '(' + sstr + ',' + estr + ')'
			if ss is None:
				ss = tmp
			else:
				ss += ', ' + tmp
		print '\t', ss

	def __sample_files_old( self, fileList, anlyList ):
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
		print '-----', str_seconds(stime), str_seconds(etime)
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
		#in case we miss some logs
		sidx -= 2
		if eidx >= 0:
			eidx += 2
		if sidx < 0:
			sidx = 0
		if etime <= 0:
			eidx = size
		print sidx, eidx
		print fileList[sidx][0]
		print str_seconds(fileList[sidx][0])
		return fileList[ sidx:eidx ]


	def __find_files_in_range( self, fileList, stime, etime ):
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
		#in case we miss some logs
		sidx -= 2
		if eidx >= 0:
			eidx += 2
		if sidx < 0:
			sidx = 0
		if etime <= 0:
			eidx = size
		print sidx, eidx
		print fileList[sidx][0]
		print str_seconds(fileList[sidx][0])
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
				if fname == 'working.log':
					continue
				#print 'stat file', root, fname
				fpath = os.path.join( root, fname )
				stime = self.__get_file_stime( fpath, parser )
				if stime > 0:
					fileList.append( (stime, fpath) )

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
			lineCount = self.__analyse_file( path, parser, anlyList )
			elapsed = time.time() - start
			print '===============================:', elapsed * 1000, 'ms,', lineCount, 'lines'

	def __analyse_file( self, path, parser, anlyList ):
		fin = open( path, 'r' )
		first = True
		lineCount = 0
		for line in fin:
			lineCount += 1
			self.__analyse_line( parser, anlyList, line )

		fin.close()
		return lineCount

	def __analyse_stdin( self, parser, anlyList ):
		lineCount = 0
		startTime = time.time()
		for line in sys.stdin:
			lineCount += 1
			self.__analyse_line( parser, anlyList, line )
			if (lineCount%10000) == 0:
				elapsed = time.time() - startTime
				print '===============================:', elapsed * 1000, 'ms,', lineCount, 'lines'
				startTime = time.time()

		elapsed = time.time() - startTime
		print '===============================:', elapsed * 1000, 'ms, total', lineCount, 'lines'

	def __analyse_line( self, parser, anlyList, line ):
		line = line.strip()
		if len(line) == 0:
			return False
		if line[0] == '#':
			return False
		
		try:
			logInfo = parser.parse_line( line )
		except:
			logInfo = None
			traceback.print_exc()

		if logInfo is None:
			return False
		
		if logInfo.exist_member( 'servTime' ) and logInfo.exist_member( 'bytesSentAll' ):
			logInfo.transrate = logInfo.bytesSentAll * 1000000 * 8 / logInfo.servTime
		else:
			logInfo.transrate = 100000000
		for anly in anlyList:
			anly.analyse_log( logInfo )
		return True

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

