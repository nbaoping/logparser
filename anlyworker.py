import multiprocessing
import time

from base import *
from factory import *
from anlyhandler import *
from logparser import *

def create_parser_from_type( args ):
	parser = None
	logType = args.type
	fmt = args.fmt
	if logType == 'translog':
		parser = WELogParser( fmt, args.fieldParser, args.fmtType )
	elif logType == 'errorlog':
		parser = ErrorlogParser()
	
	return parser



class InputFile( BaseObject ):
	def __init__( self, startTime, path, offset, endOffset = -1 ):
		self.startTime = startTime
		self.path = path
		self.offset = offset
		self.endOffset = endOffset

class InputTask( BaseObject ):
	def __init__( self, inputList, startTime, endTime ):
		self.inputList = inputList
		self.startTime = startTime
		self.endTime = endTime
		self.startBufTime = 0

class OutputFile( BaseObject ):
	def __init__( self, outPath, errPath ):
		self.outPath = outPath
		self.errPath = errPath
		self.headOffset = -1
		self.tailOffset = 0
		self.fileStartTime = -1
		self.fileEndTime = -1



class AnlyWorker( multiprocessing.Process ):
	def __init__( self, tid, task, args, taskQueue, resultQueue ):
		super(AnlyWorker, self).__init__()
		self.taskQueue = taskQueue
		self.resultQueue = resultQueue
		self.tid = tid
		self.task = task
		self.args = args

	def run( self ):
		print '=====================process', self.tid, 'started========================'
		ofileList = self.do_task( self.tid, self.task, self.args )
		self.resultQueue.put( (self.tid, ofileList) )

		print 'process', self.tid, 'exit'

	def do_task( self, tid, task, args ):
		print '%%%%%%%%%%%%%%%%%%%%%%%%%%%', tid, args, task
		
		anlyFactory = AnalyserFactory()
		parser = create_parser_from_type( args )
		self.parser = parser
		anlyList = anlyFactory.create_from_args( args, task.startTime, task.endTime )
		self.anlyList = anlyList
	
		self.update_anlylist_info( tid, task, anlyList )

		while True:
			#time.sleep(0.2)
			break
		print func_name(), '>>', anlyList

		#parse all the files
		for ifile in task.inputList:
			startTime = time.time()
			lineCount = self.anly_file( tid, ifile )
			elapsed = time.time() - startTime
			print '===============================:', 'process', tid, 'elapsed:', elapsed * 1000, 'ms,', lineCount, 'lines in', ifile.path
	
		outputList = list()
		for anly in anlyList:
			ofile = OutputFile( anly.outPath, anly.errPath )
			outputList.append( ofile )
			anly.close()
			#must assign after anly closed, since the file flush happens in the closing
			ofile.fileStartTime = anly.fileStartTime
			ofile.fileEndTime = anly.fileEndTime
	
		return outputList

	def update_anlylist_info( self, tid, task, anlyList ):
		stime = int(task.startTime)
		etime = int(task.endTime)
		for anly in anlyList:
			anly.tid = tid
			anly.startBufTime = task.startBufTime
			pace = anly.pace

			#for the tasks, must abey to the startTime setting in the config files
			#so, no need to adjust the startTime end endTime

			#open the output files
			anly.outPath += '.' + str(tid)
			anly.open_output_files()
			print func_name(), '>>tid:', tid, anly
	
	def __parse_line( self, line ):
		try:
			logInfo = self.parser.parse_line( line )
			return logInfo
		except:
			traceback.print_exc()
	
		return None
	
	def __parse_log( self, line ):
		logInfo = self.__parse_line( line )
		if logInfo is None:
			return False
		for anly in self.anlyList:
			anly.analyse_log( logInfo )
	
		return True
	
	def anly_file( self, tid, ifile):
		print func_name(), 'tid:', tid, ifile
		path = ifile.path
		offset = ifile.offset
		endOffset = ifile.endOffset
	
		fin = open( path, 'r' )
		if offset > 0:
			fin.seek( offset, 0 )
		lineCount = 0
		for line in fin:
			if endOffset >= 0:
				pos = fin.tell()
				if pos > endOffset:
					break
			lineCount += 1
			self.__parse_log( line )
		fin.close()
	
		return lineCount
	
	def __str__( self ):
		return str( self.__dict__ )
	
	
	
	
	
	
	
	
