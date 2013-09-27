import threading
import multiprocessing
import time
import os
import traceback
from StringIO import StringIO

from base import *
from anlyworker import *

class OfileMerger( BaseObject ):
	def __init__( self, anly, ofileList ):
		self.anly = anly 
		self.ofileList = ofileList
		self.bufTime = 1800

	def merge( self ):
		print func_name(), '>>++++++++++++++++begin to merge files', self.ofileList
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
		for ofile in ofileList:
			self.__update_ofile_info( anly, ofile )
			if ofile.isValid:
				newOfileList.append( ofile )

		#print func_name(), '>> begin to update offset' 
		headChanged = self.__update_ofile_list_offset( anly, newOfileList )

		return (newOfileList, headChanged)

	def __update_ofile_info( self, anly, ofile ):
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

		ofile.fin = fin
		ofile.head = hstr
		ofile.contentOffset = pos

		#get the first content line time
		if ofile.fileStartTime < 0:
			vstr = fin.readline().strip()
			if vstr == '':
				#no content in the file, set the ofile invalid
				ofile.isValid = False
	
			(vtime, value) = anly.decode_output_value( vstr )
			ofile.fileStartTime = vtime

		#get the last content line time
		if ofile.isValid and ofile.fileEndTime < 0:
			hlen = len(ofile.head)
			rate = 2
			vtime = -1
			while offset > hlen and vtime < 0:
				offset = fileSize - rate * hlen
				vtime = self.__get_file_end_time( anly, fin, offset )
				rate += 1
			
			if vtime <= 0:
				ofile.isValid = False
			else:
				ofile.fileEndTime = vtime

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
		#by default, we will use the whole file
		ofile.headOffset = ofile.fileSize
		ofile.tailOffset = ofile.contentOffset

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
			(offset, seekTime) = self.__seek_time_in_file( anly, ofile, 
					ofile.contentOffset, ofile.fileSize, minStartTime )
			if offset > 0:
				ofile.tailOffset = offset
		elif index+1 == size:
			#for the last file, should not have the tailOffset
			ofile.tailOffset = ofile.fileSize

		#update the tailOffset
		#first find the maxEndTime of the front ofileList
		idx = 0
		maxEndTime = -1
		while idx < index:
			tfile = ofileList[idx]
			if tfile.fileStartTime > maxEndTime:
				maxEndTime = tfile.fileStartTime
			idx += 1

		#seek the maxEndTime in file
		if maxEndTime > 0:
			(offset, seekTime) = self.__seek_time_in_file( anly, ofile, 
					ofile.contentOffset, ofile.fileSize, maxEndTime )
			if offset > 0:
				ofile.headOffset = offset
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
			vstr = lastline.strip()
			try:
				(vtime, value) = anly.decode_output_value( vstr )
			except:
				pass

		return vtime

	def __seek_time_in_file( self, anly, ofile, startOffset, endOffset, seekTime ):
		if seekTime <= ofile.fileStartTime:
			return (ofile.contentOffset, ofile.fileStartTime)
		if seekTime > ofile.fileEndTime:
			return (ofile.fileSize, ofile.fileEndTime)

		fin = ofile.fin
		head = startOffset
		tail = endOffset
		
		timeOffset = -1
		offset = -1
		curTime = -1
		lastOffset = -1
		lastTime = -1
		while head < tail:
			mid = (head+tail) / 2
			fin.seek( mid, 0 )
			(curTime, roff) = self.__parse_curline_time( anly, fin )
			offset = mid + roff
			if curTime > 0:
				lastOffset = offset
				lastTime = curTime

			if curTime == seekTime:
				timeOffset = offset
				print func_name(), '>>------------', str_seconds(curTime), offset
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

		return (timeOffset, seekTime)

	def __parse_curline_time( self, anly, fin ):
		offset = 0
		for line in fin:
			vstr = line.strip()
			try:
				(vtime, value) = anly.decode_output_value( vstr )
				return (vtime, offset)
			except:
				pass
			offset += len(line)

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
		#if headOffset is 0, no need to merge the head part
		print pos
		if not firstFile and headOffset > 0 and pos < headOffset:
			for line in fin:
				vstr = line.strip()
				print vstr
				(vtime, value) = anly.decode_output_value( vstr )
				print vtime, value
				anly.anly_outut_value( vtime, value )

				pos += len(line)
				print pos, headOffset
				if pos >= headOffset:
					break

		mergeTail = False
		#check if need to dump the middle part
		if headOffset < tailOffset:
			size = tailOffset - headOffset
			print headOffset, tailOffset
			print func_name(), '>> begin to output file data, len:', size, pos, fin.tell(), ofile.fileSize
			fin.seek( pos, 0 )
			self.__output_file_data( anly, fin, size )
			print func_name(), '>> output file data, len:', size
			if tailOffset < fileSize:
				mergeTail = True

		#check if needs to merge the tail part;(when the startTime changes)
		#if need to merge tail part, then the anly needs to be reset
		if not mergeTail:
			if firstFile:
				mergeTail = True

		if mergeTail:
			vstr = fin.readline().strip()
			print vstr
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

		#make sure all offset no less than zero
		if headOffset < 0:
			headOffset = fileSize
		if tailOffset < ofile.contentOffset:
			tailOffset = fileSize
		
		if tailOffset <= headOffset:
			headOffset = fileSize
			tailOffset = ofile.contentOffset

		ofile.headOffset = headOffset
		ofile.tailOffset = tailOffset
		ofile.fileSize = fileSize

	def __output_file_data( self, anly, fin, size ):
		paceSize = 1024*1024	#1M bytes
		while size > 0:
			if paceSize > size:
				paceSize = size
			print 'read data:', paceSize
			data = fin.read( paceSize )
			rsize = len(data)
			if rsize == 0:
				break
			print func_name(), '>> read data, len:', rsize
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
		print 'infoList', infoList
		segList = self.__split_file_list( infoList, totalSize, self.numCores )
		print 'segList', self.__dump_list(segList)
		taskList = self.__get_input_tasks( segList )
		print 'taskList', self.__dump_list(taskList)
		self.__close_output_files()
		outAllList = self.__map_reduce( taskList )

	def __map_reduce( self, taskList ):
		tqueue = multiprocessing.JoinableQueue()
		rqueue = multiprocessing.Queue()

		workerList = list()
		workerMap = dict()
		idx = 0
		for task in taskList:
			idx += 1
			print func_name(), task
			worker = AnlyWorker( idx, task, self.args, tqueue, rqueue )
			workerList.append( worker )
			workerMap[idx] = worker

		for worker in workerList:
			worker.start()
	
		stime = time.time()
		for worker in workerList:
			(tid, ofileList) = rqueue.get()
			print func_name(), tid, ofileList
			worker = workerMap[tid]
			worker.ofileList = ofileList

		#get the ofileList of the same type
		mergerList = list()
		idx = 0
		for anly in self.anlyList:
			ofileList = list()
			for worker in workerList:
				ofile = worker.ofileList[idx]
				ofileList.append( ofile )
			idx += 1

			merger = OfileMerger( anly, ofileList )
			mergerList.append( merger )

		for merger in mergerList:
			merger.merge()

		for worker in workerList:
			worker.join()
		spent = time.time() - stime
		print '=====================spent time:', spent, 'seconds'

		for woker in workerList:
			print func_name(), worker.ofileList

	def __close_output_files( self ):
		for anly in self.anlyList:
			anly.close_output_files()

	def __dump_list( self, ilist ):
		ss = ''
		for item in ilist:
			ss += str(item) + ' '
		print ss

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
		print 'average size for each segment:', asize
		segList = list()
		curList = list()
		if len(infoList) == 0:
			return None

		curSize = 0
		infoIdx = 0
		offset = 0
		restSize = 0
		(startTime, path, size) = infoList[infoIdx]
		lastTime = startTime
		while True:
			restSize = asize - curSize
			fileRest = size - offset + 1
			if restSize <= fileRest:
				#the current file satisfies the current list
				(endOffset, ntime) = self.__align_file_offset( path, offset+restSize )
				ifile = InputFile( lastTime, path, offset, endOffset )
				curList.append( ifile )
				offset = endOffset
				lastTime = ntime
				segList.append( curList )
				self.__dump_list( curList )
				#reset curList
				curList = list()
				curSize = 0
			else:
				#include the whole file to curList
				if fileRest > 0:
					ifile = InputFile( lastTime, path, offset, -1 )
					curList.append( ifile )

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




