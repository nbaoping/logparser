from base import *
from logparser import *


LOG_TFMT = '%m/%d/%Y %H:%M:%S'

def parse_errlog_time( line ):
	try:
		segs = line.split( '.' )
		tstr = segs[0]
		segs = segs[1].split( '(' )
		msec = int( segs[0] )
		dtime = strptime( tstr, LOG_TFMT )
		time = total_seconds( dtime ) + msec/1000.0
		return time
	except:
		pass
	return -1

def parse_errlog_workerid( line ):
	idx = line.find( 'WorkerPid' )
	if idx < 0:
		return -1
	idx = line.find( '[', idx+9 )
	if idx < 0:
		return -1
	nidx = line.find( ']', idx+1 )
	if nidx < 0:
		return -1
	idstr = line[idx+1:nidx]
	return int(idstr)

class ErrorlogParser( LogParser ):
	def __init__( self ):
		self.lastTime = 0
		pass

	def parse_line( self, line ):
		logInfo = LogInfo()
		logInfo.originLine = line
		logInfo.servTime = 0
		recvdTime = self.__parse_line_time( line )
		if recvdTime < 0:
			recvdTime = self.lastTime
		else:
			self.lastTime = recvdTime
		logInfo.recvdTime = recvdTime

		return logInfo

	def __parse_line_time( self, line ):
		return parse_errlog_time( line )
