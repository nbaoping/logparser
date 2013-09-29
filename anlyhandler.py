import threading
#import multiprocessing
import time
import os
import traceback
from StringIO import StringIO

import pprocess
from base import *
from anlyworker import *

class OfileMerger( BaseObject ):
	def __init__( self, anly, ofileList ):
		self.anly = anly 
		self.ofileList = ofileList
		self.bufTime = 1800

	def merge( self ):
		print func_name(), '>> begin to merge files, total size:', len(self.ofileList), 'anly:', self.anly
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
		print func_name(), '>> ofile list len:', len(ofileList)
		for ofile in ofileList:
			self.__update_ofile_info( anly, ofile )
			if ofile.isValid:
				newOfileList.append( ofile )
			else:
				print func_name(), '>> ofile not valid', ofile

		print func_name(), '>> ofile new list len:', len(newOfileList)
		#print func_name(), '>> begin to update offset' 
		headChanged = self.__update_ofile_list_offset( anly, newOfileList )

		return (newOfileList, headChanged)

	def __update_ofile_info( self, anly, ofile ):
		print func_name(), '>>', ofile
		ofile.isValid = True

		fileSize = os.path.getsize( ofile.outPath )
		ofile.fileSize = fileSize

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
			if headChanged:
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
		print func_name(), '>>', ofile.fin
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
			print func_name(), '>>', 'minStartTime:', str_seconds(minStartTime), minStartTime, \
					'offset:', offset, str_seconds(seekTime), seekTime, 'seekLineLen:', seekLineLen
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
			print func_name(), '>>', 'maxEndTime:', str_seconds(maxEndTime), maxEndTime, \
					'offset:', offset, str_seconds(seekTime), seekTime, 'seekLineLen:', seekLineLen
			if offset >= ofile.contentOffset:
				ofile.headOffset = offset + seekLineLen
			else:
				#no head part
				ofile.headOffset = ofile.contentOffset
		elif index == 0:
			#for the first file, should not have headOffset
			ofile.headOffset = ofile.contentOffset

	def __get_file_end_time( self, anly, fin, offset ):
		print offset
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

		print func_name(), '>>merge output file:', ofile
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
		print pos
		if headOffset > ofile.contentOffset:
			print func_name(), '>> merge head part, headOffset:', headOffset
			for line in fin:
				vstr = line.strip()
				#print vstr
				(vtime, value) = anly.decode_output_value( vstr )
				#print func_name(), '>>in head merge', vtime, value
				anly.anly_outut_value( vtime, value )

				pos += len(line)
				if pos >= headOffset:
					break


		#check if need to dump the middle part
		if headOffset < tailOffset:
			#we need first flush before outputing the middle part
			anly.flush()

			size = tailOffset - headOffset
			print func_name(), '>> begin to output file data, len:', size, '[', headOffset, '-', tailOffset, ']'
			fin.seek( headOffset, 0 )
			self.__output_file_data( anly, fin, size )

		#check if needs to merge the tail part;(when the startTime changes)
		#if need to merge tail part, then the anly needs to be reset
		if tailOffset < fileSize:
			print func_name(), '>> merge tail part, tailOffset:', tailOffset
			fin.seek( tailOffset, 0 )
			vstr = fin.readline().strip()
			(vtime, value) = anly.decode_output_value( vstr )
			#print func_name(), '>>in tail merge', vtime, value
			self.__reset_anly( anly, vtime, self.bufTime )

			#begin to merge the tail part
			anly.anly_outut_value( vtime, value )
			for line in fin:
				vstr = line.strip()
				(vtime, value) = anly.decode_output_value( vstr )
				#print func_name(), '>>in tail merge', vtime, value
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


class AnlyHandler( BaseObject ):
	def __init__( self, parser, anlyList, args ):
		self.parser = parser
		self.anlyList = anlyList
		self.args = args
		self.enableParallel = args.enableParallel
		numCores = args.numCores
		if numCores < 0:
			numCores = multiprocessing.cpu_count()
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
			logInfo = self.parser.parse_line( line )
			return logInfo
		except:
			traceback.print_exc()

		return None

	def close( self ):
		for anly in self.anlyList:
			anly.close()

	def flush( self ):
		pass

	def analyse_files( self, files ):
		(infoList, totalSize ) = self.__get_file_info( files )
		print 'infoList, len:', str(len(infoList))
		segList = self.__split_file_list( infoList, totalSize, self.numCores )
		print 'segList', self.__dump_list(segList)
		taskList = self.__get_input_tasks( segList )
		print 'taskList', self.__dump_list(taskList)
		self.__close_output_files()
		if NEW_VERSION:
			self.__map_reduce2( taskList )
		else:
			self.__map_reduce1( taskList )

	def do_task( self, tid, task, args ):
		print func_name(), '>>========tid:', tid, args

		worker = AnlyWorker( tid, task, args )
		ofileList = worker.run()

		return (tid, ofileList)

	def __map_reduce1( self, taskList ):
		startTime = time.time()

		results = pprocess.Map( limit=self.numCores )
		handler = results.manage( pprocess.MakeParallel(AnlyHandler.do_task) )

		#perform the tasks
		tid = 0
		for task in taskList:
			tid += 1
			handler( self, tid, task, self.args )

		ofileMap = dict()
		print '\n\n**************output results********************'
		idx = 0
		for (tid, ofileList) in results:
			idx += 1
			print func_name(), '>> got the', str(idx)+'th result, tid:', tid, 'ofileList len:', len(ofileList)
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

		for merger in mergerList:
			merger.merge()

		mspent = time.time() - mstime
		print func_name(), '>> =====================merge spent time:', mspent, 'seconds'
		spent = time.time() - startTime

		print '=====================spent time:', spent, 'seconds'

	def __map_reduce2( self, taskList ):
		stime = time.time()

		rqueue = multiprocessing.Queue()

		workerList = list()
		workerMap = dict()
		idx = 0
		for task in taskList:
			idx += 1
			print func_name(), '>> add task process, tid:', idx
			worker = AnlyProcess( idx, task, self.args, rqueue )
			workerList.append( worker )
			workerMap[idx] = worker

		for worker in workerList:
			worker.start()

		idx = 0
		for worker in workerList:
			idx += 1
			(tid, ofileList) = rqueue.get()
			print func_name(), '>> got the', str(idx)+'th result, tid:', tid, 'ofileList len:', len(ofileList)
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

		for merger in mergerList:
			merger.merge()

		mspent = time.time() - mstime
		print func_name(), '>> =====================merge spent time:', mspent, 'seconds'

		for worker in workerList:
			worker.join()
		spent = time.time() - stime
		print func_name(), '>> =====================spent time:', spent, 'seconds'

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
				traceback.print_exc()
				continue

			infoList.append( (startTime, path, size) )

		return (infoList, totalSize)

	def __split_file_list( self, infoList, totalSize, numSegs ):
		asize = totalSize / numSegs
		print 'average size for each segment:', asize, 'numSegs:', numSegs, 'totalSize:', totalSize
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
				print 'segment id:', segIdx, 'size:', curSize
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
			print 'segment id:', segIdx, 'size:', curSize

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
		#print func_name(), path, offset
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




