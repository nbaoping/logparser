import threading
import multiprocessing
import time
import traceback
import pickle
from StringIO import StringIO

import pprocess
from base import *
from logparser import *
from anlyworker import *

def create_parser_from_type( args ):
	parser = None
	logType = args.type
	if logType == 'translog':
		parser = WELogParser( fmt, args.fieldParser, args.fmtType )
	elif logType == 'errorlog':
		parser = ErrorlogParser()
	
	return parser


class ListNode( object ):
	def __init__( self, val ):
		self.val = val
		self.next = None


class LinkedList( object ):
	def __init__( self ):
		self.__head = None
		self.__tail = None
		self.size = 0

	def append( self, val ):
		node = ListNode( val )
		if self.__head is None:
			self.__head = self.__tail = node
		else:
			self.__tail.next = node
			self.__tail = node
		self.size += 1
	
	def pop( self ):
		if self.__head is None:
			return None

		self.size -= 1
		val = self.__head.val
		self.__head = self.__head.next
		return val


class AnlyThread( threading.Thread ):
	def __init__( self, anly ):
		super(AnlyThread, self).__init__()
		self.anly = anly
		self.__running = True

	def run( self ):
		print 'thread begin to run', self.anly
		while self.__running:
			self.step_once()

		print 'thread exit', self.anly

	def step_once( self ):
		raise_virtual()

	def stop_thread( self ):
		self.__running = False


class AnlyWorker_thread( AnlyThread ):
	def __init__( self, anly ):
		super(AnlyWorker, self).__init__( anly )
		self.logsList = LinkedList()
		self.listLock = threading.Lock()

	def step_once( self ):
		#pop the current logsList
		self.listLock.acquire()
		logsList = self.logsList
		self.logsList = LinkedList()
		self.listLock.release()

		#process all the logsLists
		while True:
			loglist = logsList.pop()
			if loglist is None:
				break
			start = time.time()
			for logInfo in loglist:
				self.anly.analyse_log( logInfo )
			cost = time.time() - start
			print '***********cost', cost, 'seconds for', len(loglist), 'lines'

		time.sleep( 0 )

	def add_loglist( self, loglist ):
		self.listLock.acquire()
		self.logsList.append( loglist )
		self.listLock.release()


class AnlyHandler_nouse( object ):
	def __init__( self, anlylist, enableParallel = False ):
		self.anlylist = anlylist
		self.enableParallel = enableParallel
		self.numThres = 10000
		self.loglist = list()
		self.lastTime = time.time()

	def open( self ):
		if self.enableParallel:
			self.workerList = list()
			for anly in self.anlylist:
				worker = AnlyWorker( anly )
				self.workerList.append( worker )

			self.__start_workers()

	def close( self ):
		if self.enableParallel:
			self.__stop_workeres()
			print 'wait for workers exit'
			for worker in self.workerList:
				worker.join()

	def analyse_log( self, logInfo ):
		if self.enableParallel:
			self.loglist.append( logInfo )
			if len(self.loglist) >= self.numThres:
				cost = time.time() - self.lastTime
				self.lastTime = time.time()
				print 'add loglist, cost', cost, 'seconds for', self.numThres, 'lines'
				loglist = self.loglist
				self.loglist = list()

				#add list to workers
				for worker in self.workerList:
					worker.add_loglist( loglist )
				print 'loglist added'
		else:
			for anly in self.anlylist:
				anly.analyse_log( logInfo )

	def __start_workers( self ):
		for worker in self.workerList:
			worker.start()

	def __stop_workeres( self ):
		for worker in self.workerList:
			worker.stop_thread()


class AnlyHandler_nouse2( BaseObject ):
	def __init__( self, parser, anlylist, enableParallel = False, numCores = 8, tasksPerCore = 10000 ):
		self.parser = parser
		self.anlylist = anlylist
		self.enableParallel = enableParallel
		self.numCores = numCores
		self.tasksPerCore = tasksPerCore
		self.totalTask = numCores * tasksPerCore
		self.lineList = list()

	def parse_log( self, line, force = False ):
		if self.enableParallel:
			self.lineList.append( line )
			if force or len(self.lineList) > self.totalTask:
				logList = self.__map_reduce( self.lineList )
				self.__anly_loglist( logList )
				self.lineList = list()
		else:
			logInfo = self.__parse_line( line )
			if logInfo is None:
				return False
			for anly in self.anlylist:
				anly.analyse_log( logInfo )

		return True

	def close( self ):
		print 'close handler', self
		self.flush()
	
	def flush( self ):
		if self.enableParallel:
			logList = self.__map_reduce( self.lineList )
			self.__anly_loglist( logList )
			self.lineList = list()

	def __parse_line( self, line ):
		try:
			logInfo = self.parser.parse_line( line )
			return logInfo
		except:
			traceback.print_exc()

		return None


	def __anly_loglist( self, logList ):
		for logInfo in logList:
			for anly in self.anlylist:
				anly.analyse_log( logInfo )

	def __map_reduce( self, lineList ):
		results = pprocess.Map( limit=self.numCores )#, reuse = 1 )
		handler = results.manage( pprocess.MakeParallel(AnlyHandler.__parse_line) )

		#perform the tasks

		for line in lineList:
			handler( self, line )

		logList = list()
		for logInfo in results:
			if logInfo is not None:
				logList.append( logInfo )

		return logList



class AnlyHandler( BaseObject ):
	def __init__( self, parser, anlylist, args, enableParallel = False, numCores = -1 ):
		self.parser = parser
		self.anlylist = anlylist
		self.args = args
		self.enableParallel = args.enableParallel
		if numCores < 0:
			numCores = multiprocessing.cpu_count()
		self.numCores = numCores

	def parse_log( self, line, force = False ):
		logInfo = self.__parse_line( line )
		if logInfo is None:
			return False
		for anly in self.anlylist:
			anly.analyse_log( logInfo )

		return True

	def close( self ):
		pass

	def flush( self ):
		pass

	def __parse_line( self, line ):
		try:
			logInfo = self.parser.parse_line( line )
			return logInfo
		except:
			traceback.print_exc()

		return None

	def analyse_files( self, files ):
		(infoList, totalSize ) = self.__get_file_info( files )
		print 'infoList', infoList
		segList = self.__split_file_list( infoList, totalSize, self.numCores )
		print 'segList', self.__dump_list(segList)
		taskList = self.__get_input_tasks( segList )
		print 'taskList', self.__dump_list(taskList)
		self.__close_output_files()
		outAllList = self.__map_reduce( taskList )

	def __close_output_files( self ):
		for anly in self.anlylist:
			anly.close_output_files()

	def __dump_list( self, ilist ):
		ss = ''
		for item in ilist:
			ss += str(item) + ' '
		print ss

	def __map_reduce( self, taskList ):
		tqueue = multiprocessing.JoinableQueue()
		rqueue = multiprocessing.Queue()

		workerList = list()
		workerMap = dict()
		idx = 0
		for task in taskList:
			idx += 1
			print func_name(), task
			if task.startTime < 0:
				break
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

		for worker in workerList:
			worker.join()
		spent = time.time() - stime
		print '=====================spent time:', spent, 'seconds'

		for woker in workerList:
			print func_name(), worker.ofileList


	def __map_reduce2( self, taskList ):
		results = pprocess.Map( limit=self.numCores )
		handler = results.manage( pprocess.MakeParallel(do_task) )

		#perform the tasks
		tid = 0
		for task in taskList:
			tid += 1
			handler( tid, task, self.args )

		outAllList = list()
		print '\n\n**************output results********************'
		for outputList in results:
			print outputList
			outAllList.append( outputList )

		return outAllList

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




