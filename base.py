from datetime import datetime
from datetime import timedelta
import os

RES_DIR = 'output'

__TIME_FMT = '%Y/%m/%d-%H:%M:%S'
__START_TIME = datetime( 1970, 1, 1 )

def total_seconds( dtime ):
	delta = dtime - __START_TIME
	return delta.total_seconds()

def to_datetime( seconds ):
	return __START_TIME + timedelta( 0, seconds )

def delta_time( dtime1, dtime2 ):
	delta = dtime2 - dtime1
	return delta.total_seconds()

def str_time( dtime ):
	return datetime.strftime( dtime, __TIME_FMT )

def mkdir( dname ):
	if not os.path.exists( dname ):
		os.makedirs( dname )

class InputArgs:
	def __init__( self ):
		self.path = ''
		self.pace = 0
		self.type = ''
		self.analyseType = 0
		self.fmt = None

