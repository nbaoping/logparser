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
import logging

from analyser import *
from logparser import *
from factory import *
from errorlog import *
from anlyhandler import *
from anlyworker import *


class XactParser:
	def __init__( self ):
		self.anlyFactory = AnalyserFactory()
		self.anlyList = None
		pass

	def parse( self, args ):
		logType = args.type

		anlyList = self.anlyFactory.create_from_args( args, -1, -1)
		self.anlyList = anlyList
		parser = create_parser_from_type( args )
		if parser == None:
			logging.error( 'wrong log type:'+str(logType) )
			return
		logging.debug( 'args:'+str(args) )
		if args.inputType == 'stdin':
			self.__parse_stdin( args, parser )
		else:
			if args.mergeMode:
				mergerHelper = MergerHelper( args.path, anlyList, parser )
				mergerHelper.merge()
			else:
				self.__parse_logs( args, parser )
	
	def close( self ):
		if self.anlyList is not None:
			for anly in self.anlyList:
				anly.close()

	def __parse_stdin( self, args, parser ):
		anlyHandler = AnlyHandler( parser, self.anlyList, args )
		self.__analyse_stdin( parser, anlyHandler )
		anlyHandler.close()

	def __parse_logs( self, args, parser ):
		anlyList = self.anlyList
		parser.formatter = args.formatter
		files = self.__stat_files( args.path, parser )
		if len(files) == 0:
			logging.error( 'no translog files, please check the logs path:'+args.path )
			return
		startTime = files[0][0]
		endTime = files[-1][0]
		logging.info( 'all files>> start time:'+str_seconds(startTime)+',end time:'+str_seconds(endTime) )
		logging.debug( 'args:'+str(args) )
		files = self.__sample_files( files, anlyList )
		if files is None or len(files) == 0:
			logging.error( 'no file need to be parsed' )
			return
		startTime = files[0][0]
		endTime = files[-1][0]
		logging.info( 'sampled files>> start time:'+str_seconds(startTime)+',end time:'+str_seconds(endTime) )
		anlyHandler = AnlyHandler( parser, anlyList, args )
		if args.enableParallel:
			anlyHandler.analyse_files( files )
		else:
			for anly in anlyList:
				anly.open_output_files()
			self.__analyse_files( files, parser, anlyHandler )
		anlyHandler.close()

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
				logging.error( 'error, no files for range('+str(stime)+','+str(etime)+')' )
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
		logging.debug( 'time range list' )
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
		logging.debug( '\t', ss )

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
		logging.debug( '-----'+str_seconds(stime)+str_seconds(etime) )
		#sample the files based on the time range
		if stime < 0 and etime < 0:
			return fileList
		sidx = -1
		eidx = 0
		size = len(fileList)
		logging.debug( str(fileList[0][0])+str(fileList[-1][0]) )
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
		return fileList[ sidx:eidx ]


	def __find_files_in_range( self, fileList, stime, etime ):
		if stime < 0 and etime < 0:
			return fileList
		sidx = -1
		eidx = 0
		size = len(fileList)
		logging.debug( str(fileList[0][0])+str(fileList[-1][0]) )
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
		return fileList[ sidx:eidx ]

	def __stat_files( self, path, parser ):
		fileList = list()
		logging.info( 'stat files in:'+path )
		for fname in os.listdir(path):
			fpath = os.path.join( path, fname )
			if not os.path.isfile(fpath):
				continue
			else:
				if fname.endswith( '.status' ):
					continue
				if fname == 'working.log':
					continue
				stime = self.__get_file_stime( fpath, parser )
				if stime > 0:
					fileList.append( (stime, fpath) )

		#sort the list
		return sorted( fileList, key=itemgetter(0) )

	def __analyse_files( self, files, parser, anlyHandler ):
		count = 0
		logging.info( 'total '+str(len(files))+' files to be analyzed' )
		startTime = time.time()
		totalLineCount = 0
		for item in files:
			count += 1
			path = item[1]
			tstr = str_seconds( item[0] )
			logging.info( 'analyse the '+str(count)+'th file--> ['+tstr+']'+path )
			start = time.time()
			lineCount = self.__analyse_file( path, parser, anlyHandler )
			totalLineCount += lineCount
			elapsed = time.time() - start
			logging.info( '===============================elapsed:'+str(elapsed * 1000)+'ms,'+str(lineCount)+' lines' )

		spent = time.time() - startTime
		logging.info( '===============================total spent:'+str(spent)+' seconds, total line count:'+str(totalLineCount)+' lines' )

	def __analyse_file( self, path, parser, anlyHandler ):
		fin = open( path, 'r' )
		baseName = os.path.basename( path )
		first = True
		lineCount = 0
		lastTime = time.time()
		for line in fin:
			lineCount += 1
			self.__analyse_line( parser, anlyHandler, line )
			if (lineCount%20000) == 0:
				spent = time.time() - lastTime
				logging.info( 'parsed '+str(lineCount)+' lines in '+str(spent)+' seconds in '+path )

		fin.close()
		anlyHandler.flush()
		return lineCount

	def __analyse_stdin( self, parser, anlyHandler ):
		lineCount = 0
		startTime = time.time()
		for line in sys.stdin:
			lineCount += 1
			self.__analyse_line( parser, anlyHandler, line )
			if (lineCount%10000) == 0:
				elapsed = time.time() - startTime
				logging.info( '===============================:'+str(elapsed * 1000)+'ms,'+str(lineCount)+'lines' )
				startTime = time.time()

		elapsed = time.time() - startTime
		logging.info( '===============================:'+str(elapsed * 1000)+'ms, total '+str(lineCount)+' lines' )

	def __analyse_line( self, parser, anlyHandler, line, fileName=None ):
		line = line.strip()
		if len(line) == 0:
			return False
		if line[0] == '#':
			return False
		
		#for test
		if fileName is not None:
			line += ' -->' + fileName
		anlyHandler.parse_log( line )
		return True

	#get the first received time
	def __get_file_stime( self, path, parser ):
		fileTime = self.__try_get_time_from_file_name( path )
		if fileTime > 0:
			return fileTime

		fin = open( path, 'r' )
		num = 0
		logInfo = None
		formatter = parser.formatter
		for line in fin:
			line = line.strip()
			if len(line) == 0:
				continue
			if line[0] == '#':
				continue
			try:
				logInfo = parser.parse_line( line )
				if logInfo is not None:
					if formatter is not None:
						formatter.fmt_log( logInfo )
					break
			except:
				logging.debug( '\n'+traceback.format_exc() )
				pass
			num += 1
		fin.close()
		if logInfo is not None:
			return logInfo.recvdTime
		return -1

	def __try_get_time_from_file_name( self, filePath ):
		baseName = os.path.basename( filePath )
		segs = baseName.split( '_' )
		if len(segs) < 3:
			return -1

		dateStr = segs[-3]
		timeStr = dateStr + '-' + segs[-2]
		timeFmt = '%Y%m%d-%H%M%S'
		try:
			dtime = strptime( timeStr, timeFmt )
			seconds = total_seconds( dtime )
			return seconds
		except:
			return -1




