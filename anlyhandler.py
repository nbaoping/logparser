import threading
import logging
import time
import os
import traceback
from StringIO import StringIO
import re
from operator import itemgetter

import pprocess
from base import *
from anlyworker import *

def str_list( alist ):
	ss = ''
	for item in alist:
		ss += str(item) + '\n'
	return ss

def str_map( amap ):
	ss = ''
	for key in amap.keys():
		ss += str(key) + ':' + str(amap[key]) + '\n'
	return ss

class OfileMerger( BaseObject ):
	def __init__( self, anly, ofileList ):
		super( OfileMerger, self ).__init__( )
		self.anly = anly 
		self.ofileList = ofileList
		self.bufTime = 1800
		self.optEnable = False

	def get_out_path( self ):
		return self.anly.outPath

	def merge( self ):
		logging.info( '>> begin to merge files, total size:'+str(len(self.ofileList))+' anly:'+str(self.anly) )
		ofileList = self.ofileList
		if len(ofileList) == 0:
			return

		self.__init_anly( self.anly )
		(ofileList, headChanged) = self.__update_ofile_list_info( self.anly, ofileList )

		firstFile = True
		for ofile in ofileList:
			self.__merge_outfile( self.anly, ofile, firstFile )
			firstFile = False
			ofile.fin.close()

		while True:
			#time.sleep( 0.2 )
			break

		self.anly.close()

	def __init_anly( self, anly ):
		anly.open_output_files()

	def __update_ofile_list_info( self, anly, ofileList ):
		newOfileList = list()
		logging.debug( '>> ofile list len:'+str(len(ofileList)) )
		for ofile in ofileList:
			self.__update_ofile_info( anly, ofile )
			if ofile.isValid:
				newOfileList.append( ofile )
			else:
				logging.warn( '>> ofile not valid'+str(ofile) )

		logging.debug( '>> ofile new list len:'+str(len(newOfileList)) )
		headChanged = self.__update_ofile_list_offset( anly, newOfileList )

		return (newOfileList, headChanged)

	def __update_ofile_info( self, anly, ofile ):
		logging.debug( '>>'+str(ofile) )
		ofile.isValid = True

		fileSize = os.path.getsize( ofile.outPath )
		ofile.fileSize = fileSize

		if fileSize == 0:
			ofile.isValid = False
			return

		#get the head info
		pos = 0
		fin = open( ofile.outPath, 'r' )
		hline = fin.readline()
		pos += len(hline)
		hstr = hline.strip()
		anly.decode_output_head( hstr )
		#check if the ofile has the head
		tmpHead = anly.encode_output_head()
		if tmpHead is None:
			hstr = ''
			fin.seek( 0, 0 )
			pos = 0

		ofile.fin = fin
		ofile.head = hstr
		ofile.contentOffset = pos

		#get the first content line time
		if ofile.fileStartTime < 0:
			firstLine = fin.readline()
			vstr = firstLine.strip()
			if vstr == '':
				#no content in the file, set the ofile invalid
				ofile.isValid = False
	
			(vtime, value) = anly.decode_output_value( vstr )
			ofile.fileStartTime = vtime
			ofile.firstLineLen = len(firstLine)
		else:
			firstLine = fin.readline()
			ofile.firstLineLen = len(firstLine)

		#get the last content line time
		if ofile.isValid and ofile.fileEndTime <= 0:
			hlen = self.__get_average_line_len( ofile )
			rate = 2
			vtime = -1
			offset = fileSize
			while offset > hlen and vtime < 0:
				offset = fileSize - rate * hlen
				if offset < 0:
					offset = 0
				vtime = self.__get_file_end_time( anly, fin, offset )
				rate += 1
			
			if vtime <= 0:
				ofile.isValid = False
			else:
				ofile.fileEndTime = vtime

	def __get_average_line_len( self, ofile ):
		hlen = len(ofile.head)
		if hlen > ofile.firstLineLen:
			return hlen
		return ofile.firstLineLen

	def __update_ofile_list_offset( self, anly, ofileList ):
		headChanged = self.__is_ofile_list_changed( ofileList )

		idx = 0
		for ofile in ofileList:
			if headChanged or not self.optEnable:
				ofile.headOffset = ofile.fileSize
				ofile.tailOffset = 0
			else:
				#must first load the head info for future decoding
				anly.decode_output_head( ofile.head )
				self.__update_ofile_offset( anly, ofile, ofileList, idx )
			idx += 1

		return headChanged

	def __is_ofile_list_changed( self, ofileList ):
		lastHead = None
		for ofile in ofileList:
			if lastHead is None:
				lastHead = ofile.head
			else:
				if lastHead != ofile.head:
					return True

		return False

	def __update_ofile_offset( self, anly, ofile, ofileList, index ):
		logging.debug( '>>'+str(ofile.fin) )
		#by default, we will use the whole file
		ofile.headOffset = ofile.fileSize
		ofile.tailOffset = ofile.contentOffset

		if anly.pace < 0:
			#for negative pace, should merge the whole file
			return

		size = len(ofileList)

		#update the tailOffset
		#first find the min fileStartTime of the behind ofileList
		idx = index+1
		minStartTime = -1
		while idx < size:
			tfile = ofileList[idx]
			if minStartTime < 0:
				minStartTime = tfile.fileStartTime
			else:
				if tfile.fileStartTime < minStartTime:
					minStartTime = tfile.fileStartTime
			idx += 1

		#seek the minStartTime in file
		if minStartTime > 0:
			(offset, seekTime, seekLineLen) = self.__seek_time_in_file( anly, ofile, 
					ofile.contentOffset, ofile.fileSize, minStartTime )
			logging.debug( 'minStartTime:'+str_seconds(minStartTime)+ \
					'offset:'+str(offset)+',seekTime:'+str_seconds(seekTime)+',seekLineLen:'+str(seekLineLen) )
			if offset >= ofile.contentOffset:
				ofile.tailOffset = offset
		elif index+1 == size:
			#for the last file, should not have the tailOffset
			ofile.tailOffset = ofile.fileSize

		#update the headOffset
		#first find the maxEndTime of the front ofileList
		idx = 0
		maxEndTime = -1
		while idx < index:
			tfile = ofileList[idx]
			if tfile.fileEndTime > maxEndTime:
				maxEndTime = tfile.fileEndTime
			idx += 1

		#seek the maxEndTime in file
		if maxEndTime > 0:
			(offset, seekTime, seekLineLen) = self.__seek_time_in_file( anly, ofile, 
					ofile.contentOffset, ofile.fileSize, maxEndTime )
			logging.debug( 'maxEndTime:'+str_seconds(maxEndTime)+ \
					'offset:'+str(offset)+',seekTime:'+str_seconds(seekTime)+',seekLineLen:'+str(seekLineLen) )
			if offset >= ofile.contentOffset:
				ofile.headOffset = offset + seekLineLen
			else:
				#no head part
				ofile.headOffset = ofile.contentOffset
		elif index == 0:
			#for the first file, should not have headOffset
			ofile.headOffset = ofile.contentOffset

	def __get_file_end_time( self, anly, fin, offset ):
		fin.seek( offset, 0 )
		vtime = -1

		lastLine = None
		for line in fin:
			lastLine = line

		if lastLine is not None:
			vstr = lastLine.strip()
			try:
				(vtime, value) = anly.decode_output_value( vstr )
			except:
				pass

		return vtime

	def __seek_time_in_file( self, anly, ofile, startOffset, endOffset, seekTime ):
		if seekTime < ofile.fileStartTime:
			return (-1, 0, 0)
		elif seekTime == ofile.fileStartTime:
			return (ofile.contentOffset, ofile.fileStartTime, ofile.firstLineLen)

		if seekTime > ofile.fileEndTime:
			return (ofile.fileSize, 0, 0)

		fin = ofile.fin
		head = startOffset
		tail = endOffset
	
		#init the return values
		timeOffset = -1
		offset = -1
		seekLineLen = -1

		curTime = -1
		lastOffset = -1
		lastTime = -1
		lastLineLen = -1
		while head < tail:
			mid = (head+tail) / 2
			fin.seek( mid, 0 )
			(curTime, roff, lineLen) = self.__parse_curline_time( anly, fin )
			offset = mid + roff
			if curTime > 0:
				lastOffset = offset
				lastTime = curTime
				lastLineLen = lineLen

			if curTime == seekTime:
				timeOffset = offset
				seekLineLen = lineLen
				break
			elif curTime > seekTime:
				tail = mid
			elif curTime <= 0:
				#normaily, this will not gonna happen, should be error
				break
			elif curTime < seekTime:
				head = mid

		#in case when timeOffset < 0, use the last offset instead
		if timeOffset < 0:
			if lastOffset > 0 and lastOffset > 0:
				timeOffset = lastOffset
				seekTime = lastTime
				seekLineLen = lastLineLen

		return (timeOffset, seekTime, seekLineLen)

	def __parse_curline_time( self, anly, fin ):
		offset = 0
		for line in fin:
			lineLen = len(line)
			vstr = line.strip()
			try:
				(vtime, value) = anly.decode_output_value( vstr )
				return (vtime, offset, lineLen)
			except:
				pass
			offset += lineLen

		return (-1, offset)

	def __merge_outfile( self, anly, ofile, firstFile ):
		self.__validate_outfile( ofile )

		logging.debug( 'merge output file:'+str(ofile) )
		anly.decode_output_head( ofile.head )

		fileSize = ofile.fileSize
		headOffset = ofile.headOffset
		tailOffset = ofile.tailOffset
		pos = ofile.contentOffset
		fin = ofile.fin
		fin.seek( pos, 0 )

		#merge the head part. Using the previous anly config
		#if it's the first file, cannot merge the head
		#if headOffset is ofile.contentOffset, no need to merge the head part
		if headOffset > ofile.contentOffset:
			logging.debug( 'merge head part, headOffset:'+str(headOffset) )
			for line in fin:
				vstr = line.strip()
				(vtime, value) = anly.decode_output_value( vstr )
				anly.anly_outut_value( vtime, value )

				pos += len(line)
				if pos >= headOffset:
					break


		#check if need to dump the middle part
		if headOffset < tailOffset:
			#we need first flush before outputing the middle part
			anly.flush()

			size = tailOffset - headOffset
			logging.debug( 'begin to output file data, len:'+str(size)+\
					' ['+str(headOffset)+'-'+str(tailOffset)+']' )
			fin.seek( headOffset, 0 )
			self.__output_file_data( anly, fin, size )

		#check if needs to merge the tail part;(when the startTime changes)
		#if need to merge tail part, then the anly needs to be reset
		if tailOffset < fileSize:
			logging.debug( 'merge tail part, tailOffset:'+str(tailOffset) )
			fin.seek( tailOffset, 0 )
			vstr = fin.readline().strip()
			(vtime, value) = anly.decode_output_value( vstr )
			self.__reset_anly( anly, vtime, self.bufTime )

			#begin to merge the tail part
			anly.anly_outut_value( vtime, value )
			for line in fin:
				vstr = line.strip()
				(vtime, value) = anly.decode_output_value( vstr )
				anly.anly_outut_value( vtime, value )

	def __reset_anly( self, anly, startTime, bufTime ):
		anly.restart( startTime, -1, bufTime )

	def __validate_outfile( self, ofile ):
		fileSize = os.path.getsize( ofile.outPath )
		headOffset = ofile.headOffset
		tailOffset = ofile.tailOffset

		#make sure all offset no less than the contentOffset
		if headOffset < ofile.contentOffset:
			headOffset = fileSize
		if tailOffset < ofile.contentOffset:
			tailOffset = fileSize
		
		if tailOffset <= headOffset:
			#make sure tailOffset is always >= headOffset
			headOffset = fileSize
			tailOffset = fileSize

		ofile.headOffset = headOffset
		ofile.tailOffset = tailOffset
		ofile.fileSize = fileSize

	def __output_file_data( self, anly, fin, size ):
		paceSize = 1024*1024	#1M bytes
		while size > 0:
			if paceSize > size:
				paceSize = size
			data = fin.read( paceSize )
			rsize = len(data)
			if rsize == 0:
				break
			anly.output_data( data, rsize )
			size -= rsize

class MergerHelper( BaseObject ):
	def __init__( self, inDir, anlyList, parser ):
		super( MergerHelper, self ).__init__( )
		self.parser = parser
		self.inDir = inDir
		self.pattern = '(\S+?)__(\d+)_(\d+_)?(\S+?_).+\.txt\.?(\S+)?'
		self.regex = re.compile( self.pattern )
		self.timeFmt = '%Y%m%d-%H%M%S' 
		self.anlyList = self.__sort_anly_list(anlyList)

	def __sort_anly_list( self, anlyList ):
		logging.debug( 'begin sorting the anlyList' )
		itemList = list()
		for anly in anlyList:
			fileName = os.path.basename(anly.outPath) 
			ninfo = self.__parse_file_name( fileName )
			if ninfo is None:
				logging.error( 'ninfo None error with fileName:'+fileName )
				raise Exception( 'invalid anlyList' )

			itemList.append( (ninfo[-1], anly) )

		itemList = sorted( itemList, key=itemgetter(0) )
		sortList = list()
		for (anlyId, anly) in itemList:
			sortList.append( anly )

		logging.debug( 'sorted anlyList:\n'+str_list(sortList) )
		return sortList

	def merge( self ):
		logging.info( 'merging in dir:'+self.inDir )
		resultsList = self.__parse_file_dir( self.inDir )
		for parseListList in resultsList:
			self.__merge_parse_list_list( parseListList )

	def __merge_parse_list_list( self, parseListList ):
		if parseListList is None or len(parseListList)==0:
			logging.info( 'no files list got from the dir:'+self.inDir )
			return

		startTime = time.time() 
		for parseList in parseListList:
			logging.debug( 'got one parseList:\n'+str_list(parseList) )
			mergerList = list()
			for (anly, ntime, ofileList) in parseList:
				self.__reset_anly( anly, ntime )
				anly.parser = self.parser
				merger = OfileMerger( anly, ofileList )
				mergerList.append( merger )
	
			self.merge_ofiles( mergerList )
	
		mspent = time.time() - startTime
		logging.info( '=====================merge spent time:'+str(mspent)+' seconds' )

	def __reset_anly( self, anly, ntime ):
		fileName = os.path.basename( anly.outPath )
		dirName = os.path.dirname( anly.outPath )
		timeStr = str_seconds( ntime, self.timeFmt )

		nidx = fileName.find( '_' )
		tailPart = fileName[nidx:len(fileName)]
		fileName = timeStr + tailPart

		anly.outPath = os.path.join( dirName, fileName )

	def __get_file_list_by_time( self, inDir ):
		allFileList = list()
		for fileName in os.listdir(inDir):
			ipath = os.path.join( inDir, fileName )
			if not os.path.isfile(ipath):
				continue

			info = self.__parse_file_name( fileName )
			if info is None:
				continue

			allFileList.append( (info[0], fileName) )

		allFileList = sorted( allFileList, key=itemgetter(0) )

		sortSplitList = list()
		curList = list()
		lastTime = None
		for (curTime, fileName) in allFileList:
			if lastTime is None:
				lastTime = curTime
				curList.append( fileName )
			elif curTime == lastTime:
				curList.append( fileName )
			else:
				#the time changed, start a new list
				sortSplitList.append( curList )
				logging.debug( 'add sorted file list:\n'+str_list(curList) )
				curList = list()
				lastTime = curTime
				curList.append( fileName )

		if len(curList) > 0:
			logging.debug( 'tail add sorted file list:\n'+str_list(curList) )
			sortSplitList.append( curList )

		return sortSplitList

	def __parse_file_dir( self, inDir ):
		resultsList = list()
		sortSplitList = self.__get_file_list_by_time( inDir )
		for fileList in sortSplitList:
			parseListList = self.__parse_file_list( inDir, fileList )
			resultsList.append( parseListList )

		return resultsList

	def __parse_file_list( self, inDir, fileList ):
		logging.debug( 'parsing fileList:\n'+str_list(fileList) )
		anlyList = self.anlyList
		fileMap = dict()
		pidList = list()
		for fileName in fileList:
			info = self.__parse_file_name( fileName )
			if info is None:
				continue

			pid = info[2]
			if pid in fileMap:
				flist = fileMap[pid]
			else:
				pidList.append( pid )
				flist = list()
				fileMap[pid] = flist

			flist.append( info )

		pidList = sorted( pidList )
		logging.debug( 'pidList:\n'+str_list(pidList) )
		#sort the file list by time
		for pid in pidList:
			flist = fileMap[pid]
			flist = sorted( flist, key=itemgetter(0) )
			fileMap[pid] = flist
			logging.debug( 'pid:'+str(pid)+',sorted flist:\n'+str_list(flist) )
		
		#parseListList = self.__parse_ofile_lists( inDir, self.anlyList, pidList, fileMap )
		splitFileMap = self.__split_list_in_map( fileMap )
		mapList = self.__get_valid_file_map_list( anlyList, pidList, splitFileMap )
		parseListList = self.__parse_map_list( inDir, anlyList, pidList, mapList )
		return parseListList

	def __split_list_in_map( self, fileMap ):
		for pid in fileMap.keys():
			flist = fileMap[pid]
			#split the list by time
			splitList = list()
			cycleList = list()
			curTime = -1
			for info in flist:
				time = info[0]
				if curTime < 0:
					curTime = time
					cycleList.append( info )
				else:
					if time == curTime:
						#in the current cycle
						cycleList.append( info )
					else:
						#make sure the cycleList is in order by the anlyId
						cycleList = sorted( cycleList, key=itemgetter(-1) )
						splitList.append( cycleList )
						logging.debug( 'pid:'+str(pid)+',add cycleList:\n'+str_list(cycleList) )
						#start a new cycle
						cycleList = list()
						curTime = time
						cycleList.append( info )
			
			if len(cycleList) > 0:
				#make sure the cycleList is in order by the anlyId
				cycleList = sorted( cycleList, key=itemgetter(-1) )
				logging.debug( 'pid:'+str(pid)+',tail add cycleList:\n'+str_list(cycleList) )
				splitList.append( cycleList )

			fileMap[pid] = splitList
		
		return fileMap

	def __get_valid_file_map_list( self, anlyList, pidList, splitFileMap ):
		pid = pidList[0]
		slist = splitFileMap[pid]
		number = len(slist)
		mapList = list()
		idx = 0
		while idx < number:
			curMap = dict()
			for pid in pidList:
				slist = splitFileMap[pid]
				if idx >= len(slist):
					#this cycle is invalid, skip all the cycleLists
					curMap = None
					break

				cycleList = slist[idx]
				if not self.__is_list_valid( anlyList, cycleList ):
					#this cycle is invalid, skip all the cycleLists
					logging.debug( 'invalid cycle, skip list:\n'+\
							str_list(cycleList)+'anlyList:\n'+str_list(anlyList) )
					curMap = None
					break
				curMap[pid] = cycleList
				logging.debug( 'pid:'+str(pid)+',add valid cycleList:\n'+str_list(cycleList) )
			
			idx += 1 #to another cycle
			if curMap is not None:
				logging.debug( 'add valid file map:\n'+str_map(curMap) )
				mapList.append( curMap )

		return mapList

	def __parse_map_list( self, inDir, anlyList, pidList, mapList ):
		parseListList = list()
		for fileMap in mapList:
			idx = 0
			parseList = list()
			for anly in anlyList:
				ofileList = list()
				ntime = None
				for pid in pidList:
					cycleList = fileMap[pid]
					info = cycleList[idx]
					if ntime is None:
						ntime = info[0]

					fpath = os.path.join( inDir, info[3] )
					ofile = OutputFile( fpath, None )
					ofileList.append( ofile )
				
				idx += 1
				logging.debug( 'add ofileList:\n'+str_list(ofileList) )
				parseList.append( (anly, ntime, ofileList) )
			
			parseListList.append( parseList )

		return parseListList
	
	def __is_list_valid( self, anlyList, cycleList ):
		idx = 0
		for anly in anlyList:
			if idx >= len(cycleList):
				return False

			fileName = os.path.basename(anly.outPath) 
			ninfo = self.__parse_file_name( fileName )
			if ninfo is None:
				logging.error( 'ninfo None error with fileName:'+fileName )
				return False
			naid = ninfo[1]
			info = cycleList[idx]
			aid = info[1]
			if naid != aid:
				logging.info( 'aid mismatch, aid in xml:'+str(naid)+',aid in in dir:'+str(aid) )
				return False
			idx += 1

		return True


	def __parse_ofile_lists( self, inDir, anlyList, pidList, fileMap ):
		parseListList = list()
		pid = pidList[0]
		flist = fileMap[pid]
		totalSize = len(flist)

		idx = 0
		while idx < totalSize:
			curFail = False
			parseList = list()
			for anly in anlyList:
				ninfo = self.__parse_file_name( os.path.basename(anly.outPath) )
				naid = ninfo[1]
				ofileList = list()
				ntime = None
				#get ofileList
				for pid in pidList:
					flist = fileMap[pid]
					info = flist[idx]
					aid = info[1]
					if ntime is None:
						ntime = info[0]
					if naid == aid:
						fpath = os.path.join( inDir, info[3] )
						ofile = OutputFile( fpath, None )
						ofileList.append( ofile )
					else:
						#any failure will cause skipping this cycle
						curFail = True
				
				idx += 1
				parseList.append( (anly, ntime, ofileList) )

			if not curFail:
				parseListList.append( parseList )

		return parseListList

	def __parse_file_name( self, fileName ):
		try:
			logging.debug( 'parse fileName:'+fileName )
			res = self.regex.match( fileName )
			grps = res.groups()
			if len(grps) < 5:
				return none

			timeStr = grps[0]
			aid = grps[1]
			cid = grps[2]
			name = grps[3]
			pid = grps[4]

			fmt = self.timeFmt
			dtime = strptime( timeStr, fmt )
			time = total_seconds( dtime )
			anlyId = aid 
			anlyName = aid + '_'
			if cid is not None:
				anlyId += cid
				anlyName += cid
			else:
				anlyId += '0'
			anlyName += name
			anlyId = anlyId.rstrip( '_' )
			anlyId = int(anlyId)
			if pid is None:
				pid = 0
			else:
				pid = int(pid)

			info = (time, anlyName, pid, fileName, anlyId)
			logging.debug( 'parsed info:'+str(info) )
			return info
		except:
			logging.error( '\n'+traceback.format_exc() )
			return None

	def __merge_task( self, merger ):
		stime = time.time()
		try:
			logging.debug( 'begin mergeing with:'+str(merger) )
			merger.merge()
		except:
			logging.error( '\n'+traceback.format_exc() )
		spent = time.time() - stime
		return (merger.get_out_path(), spent)

	def merge_ofiles( self, mergerList ):
		logging.debug( 'merge all files, mergerList:'+str(mergerList) )
		size = len(mergerList)
		results = pprocess.Map( limit=size )
		handler = results.manage( pprocess.MakeParallel(MergerHelper.__merge_task) )

		#perform the tasks
		tid = 0
		for merger in mergerList:
			tid += 1
			handler( self, merger )

		total = 0
		for (outPath, spent) in results:
			total += 1
			logging.debug( 'merged output file:'+outPath+' in '+\
					str(spent)+' seconds. total merged now:'+str(total) )



class AnlyHandler( BaseObject ):
	def __init__( self, parser, anlyList, args ):
		super( AnlyHandler, self ).__init__( )
		self.parser = parser
		self.anlyList = anlyList
		self.args = args
		self.enableParallel = args.enableParallel
		numCores = args.numCores
		self.numCores = numCores

	#used for none multiprocessing
	def parse_log( self, line, force = False ):
		logInfo = self.__parse_line( line )
		if logInfo is None:
			return False
		for anly in self.anlyList:
			anly.analyse_log( logInfo )

		return True

	def __parse_line( self, line ):
		try:
			parser = self.parser
			logInfo = parser.parse_line( line )
			logging.debug( 'parsed info:'+str(logInfo) )
			if parser.formatter is not None:
				logInfo = parser.formatter.fmt_log( logInfo )
				logging.debug( 'formatted info:'+str(logInfo) )
			return logInfo
		except:
			logging.debug( line )
			logging.debug( '\n'+traceback.format_exc() )

		return None

	def close( self ):
		for anly in self.anlyList:
			anly.close()

	def flush( self ):
		pass

	def analyse_files( self, files ):
		(infoList, totalSize ) = self.__get_file_info( files )
		logging.info( 'infoList, len:'+str(len(infoList)) )
		segList = self.__split_file_list( infoList, totalSize, self.numCores )
		logging.debug( 'segList:'+self.__dump_list(segList) )
		taskList = self.__get_input_tasks( segList )
		logging.debug( 'taskList:'+self.__dump_list(taskList) )
		self.__close_output_files()
		#if is_new_version():
			#self.__map_reduce2( taskList, totalSize )
		#else:
		self.__map_reduce1( taskList, totalSize )

	def do_task( self, tid, task, args, curTimeStr ):
		logging.info( '========tid:'+str(tid)+str(args) )

		args.curTimeStr = curTimeStr
		worker = AnlyWorker( tid, task, args )
		(lineCount, ofileList) = worker.run()

		return (tid, lineCount, ofileList)

	def __map_reduce1( self, taskList, totalSize ):
		startTime = time.time()

		results = pprocess.Map( limit=self.numCores )
		handler = results.manage( pprocess.MakeParallel(AnlyHandler.do_task) )

		#perform the tasks
		curTimeStr = cur_timestr()
		tid = 0
		for task in taskList:
			tid += 1
			handler( self, tid, task, self.args, curTimeStr )

		ofileMap = dict()
		logging.info( '\n\n**************output results********************' )

		totalLineCount = 0
		idx = 0
		firstExitTime = -1
		for (tid, lineCount, ofileList) in results:
			totalLineCount += lineCount
			idx += 1
			if firstExitTime < 0:
				firstExitTime = time.time()
			logging.info( 'got the '+str(idx)+'th result, tid:'+str(tid)+',ofileList len:'+str(len(ofileList)) )
			ofileMap[tid] = ofileList

		mstime = time.time()

		mergerList = list()
		idx = 0
		for anly in self.anlyList:
			ofileList = list()
			tid = 1
			while tid <= self.numCores:
				olist = ofileMap[tid]
				ofileList.append( olist[idx] )
				tid += 1
			idx += 1

			anly.parser = self.parser
			merger = OfileMerger( anly, ofileList )
			mergerList.append( merger )

		waitTime = time.time() - firstExitTime
		self.merge_ofiles( mergerList )

		mspent = time.time() - mstime
		logging.info( '=====================merge spent time:'+str(mspent)+' seconds' )
		spent = time.time() - startTime

		logging.info( '=====================spent time:'+str(spent)+' seconds, wait time:'+ \
				str(waitTime)+' seconds. total parsed line count:'+str(totalLineCount)+ \
				',file size:'+str(totalSize/1024/1024)+'MBytes' )

	def __merge_task( self, merger ):
		stime = time.time()
		try:
			merger.merge()
		except:
			logging.error( '\n'+traceback.format_exc() )
		spent = time.time() - stime
		return (merger.get_out_path(), spent)

	def merge_ofiles( self, mergerList ):
		size = len(mergerList)
		results = pprocess.Map( limit=size )
		handler = results.manage( pprocess.MakeParallel(AnlyHandler.__merge_task) )

		#perform the tasks
		tid = 0
		for merger in mergerList:
			tid += 1
			handler( self, merger )

		total = 0
		for (outPath, spent) in results:
			total += 1
			logging.info( 'merged output file:'+outPath+' in'+str(spent)+' seconds. total merged now:'+str(total) )

	def __map_reduce2( self, taskList, totalSize ):
		stime = time.time()

		rqueue = multiprocessing.Queue()

		workerList = list()
		workerMap = dict()
		idx = 0
		for task in taskList:
			idx += 1
			logging.info( 'add task process, tid:'+str(idx) )
			worker = AnlyProcess( idx, task, self.args, rqueue )
			workerList.append( worker )
			workerMap[idx] = worker

		for worker in workerList:
			worker.start()
	
		totalLineCount = 0
		firstExitTime = -1
		idx = 0
		for worker in workerList:
			idx += 1
			(tid, lineCount, ofileList) = rqueue.get()
			totalLineCount += lineCount
			if firstExitTime < 0:
				firstExitTime = time.time()
			logging.info( 'got the '+str(idx)+'th result, tid:'+str(tid)+',ofileList len:'+str(len(ofileList)) )
			worker = workerMap[tid]
			worker.ofileList = ofileList

		mstime = time.time()
		#get the ofileList of the same type
		mergerList = list()
		idx = 0
		for anly in self.anlyList:
			ofileList = list()
			for worker in workerList:
				ofile = worker.ofileList[idx]
				ofileList.append( ofile )
			idx += 1

			anly.parser = self.parser
			merger = OfileMerger( anly, ofileList )
			mergerList.append( merger )

		waitTime = time.time() - firstExitTime
		self.merge_ofiles( mergerList )

		mspent = time.time() - mstime
		logging.info( '=====================merge spent time:'+str(mspent)+' seconds' )

		for worker in workerList:
			worker.join()
		spent = time.time() - stime

		logging.info( '=====================spent time:'+str(spent)+' seconds, wait time:'+ \
				str(waitTime)+' seconds. total parsed line count:'+str(totalLineCount)+ \
				'file size:'+str(totalSize/1024/1024)+'MBytes' )

	def __close_output_files( self ):
		for anly in self.anlyList:
			anly.close_output_files()

	def __dump_list( self, ilist ):
		ss = 'len:' + str(len(ilist))
		return ss

	def __get_file_info( self, files ):
		infoList = list()
		totalSize = 0
		for item in files:
			startTime = item[0]
			path = item[1]
			try:
				size = os.path.getsize( path )
				totalSize += size
			except:
				logging.debug( '\n'+traceback.format_exc() )
				continue

			infoList.append( (startTime, path, size) )

		return (infoList, totalSize)

	def __split_file_list( self, infoList, totalSize, numSegs ):
		asize = totalSize / numSegs
		logging.info( 'average size for each segment:'+str(asize)+\
				',numSegs:'+str(numSegs)+',totalSize:'+str(totalSize) )
		segList = list()
		curList = list()
		if len(infoList) == 0:
			return None

		curSize = 0
		infoIdx = 0
		offset = 0
		restSize = 0
		(startTime, path, size) = infoList[0]
		lastTime = startTime
		segIdx = 1
		while True:
			restSize = asize - curSize
			fileRest = size - offset
			if restSize <= fileRest:
				#the current file satisfies the current list
				(endOffset, ntime) = self.__align_file_offset( path, offset+restSize )
				ifile = InputFile( lastTime, path, offset, endOffset )
				curList.append( ifile )
				curSize += (endOffset-offset)
				offset = endOffset
				lastTime = ntime
				logging.debug( 'segment id:'+str(segIdx)+',size:'+str(curSize) )
				segList.append( curList )
				segIdx += 1
				self.__dump_list( curList )
				#reset curList
				curList = list()
				curSize = 0
			else:
				#include the whole file to curList
				if fileRest > 0:
					ifile = InputFile( lastTime, path, offset, -1 )
					curList.append( ifile )
					curSize += fileRest

				#get the next file
				infoIdx += 1
				if infoIdx < len(infoList):
					(startTime, path, size) = infoList[infoIdx]
					offset = 0
					lastTime = startTime
				else:
					#not enough file available
					break

		#check the last list
		if len(curList) > 0:
			self.__dump_list( curList )
			segList.append( curList )
			logging.debug( 'segment id:'+str(segIdx)+',size:'+str(curSize) )

		return segList

	def __get_input_tasks( self, segList ):
		taskList = list()
		size = len(segList)
		idx = 1
		while idx < size:
			lastList = segList[idx-1]
			curList = segList[idx]
			startTime = lastList[0].startTime
			endTime = curList[0].startTime
			task = InputTask( lastList, startTime, endTime )
			taskList.append( task )
			idx += 1

		#get the last tasks
		if idx <= size:
			curList = segList[idx-1]
			startTime = curList[0].startTime
			endTime = -1
			task = InputTask( curList, startTime, endTime )
			taskList.append( task )

		return taskList

	def __align_file_offset( self, path, offset ):
		fin = open( path, 'r' )
		fin.seek( offset, 0 )
		ntime = -1

		for line in fin:
			try:
				logInfo = self.parser.parse_line( line )
				ntime = logInfo.recvdTime
				break
			except:
				offset += len(line)

		fin.close()
		return (offset, ntime )




