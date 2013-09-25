#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from datetime import datetime
from datetime import timedelta
import os
import inspect
from  xml.dom import  minidom

import recipe
from customer import *

NEW_VERSION = True

RES_DIR = 'output'

__TIME_FMT = '%Y/%m/%d-%H:%M:%S'
__START_TIME = datetime( 1970, 1, 1 )

def strptime( tstr, fmt ):
	if NEW_VERSION:
		return datetime.strptime( tstr, fmt )
	else:
		time = recipe.strptime( tstr, fmt )
		dtime = datetime( time[0], time[1], time[2], time[3], time[4], time[5] )
		return dtime

def total_seconds( dtime ):
	delta = dtime - __START_TIME
	if NEW_VERSION:
		return delta.total_seconds()
	else:
		return delta.seconds + delta.days*24*3600

def to_datetime( seconds ):
	return __START_TIME + timedelta( 0, seconds )

def delta_time( dtime1, dtime2 ):
	delta = dtime2 - dtime1
	return delta.total_seconds()

def str_time( dtime ):
	return datetime.strftime( dtime, __TIME_FMT )

def time_str( tstr ):
	return strptime( tstr, __TIME_FMT )

def seconds_str( tstr ):
	dtime = strptime( tstr, __TIME_FMT )
	return total_seconds( dtime )

def str_seconds( seconds ):
	dtime = to_datetime( seconds )
	return str_time( dtime )

def cur_timestr( ):
	now = datetime.now()
	return str_time( now )

def mkdir( dname ):
	if not os.path.exists( dname ):
		os.makedirs( dname )

def func_name():
	#print inspect.stack()
	return inspect.stack()[1][3]

def raise_virtual( func ):
	raise Exception( 'derived must implement '+func+' virtual function' )

def get_attrvalue(node, attrname):
     return node.getAttribute(attrname)

def get_nodevalue(node, index = 0):
    return node.childNodes[index].nodeValue.encode('utf-8','ignore')

def get_xmlnode(node, name):
    return node.getElementsByTagName(name)

class BaseObject( object ):
	def __init__( self ):
		pass

	def set_member( self, mname, value ):
		self.__dict__[mname] = value

	def get_member( self, mname ):
		return self.__dict__[mname]

	def exist( self, member ):
		return member in self.__dict__

	def exist_member( self, mname ):
		return mname in self.__dict__

	def copy_object( self, obj ):
		for mname in self.__dict__.keys():
			value = self.__dict__[mname]
			obj.set_member( mname, value )

	def __str__( self ):
		return str( self.__dict__ )

class InputArgs( BaseObject ):
	def __init__( self ):
		self.inputType = 'files'
		self.path = None
		self.type = 'translog'
		self.fmt = None
		self.configPath = None
		self.fieldParser = None
		self.customer = None
		self.sorted = False
		self.enableParallel = False

	def parse_argv( self, argv ):
		idx = 1
		size = len(argv)
		while idx < size:
			if idx >= size:
				print 'invalid input arguments'
				self.__print_usage()
				return False
			arg = argv[ idx ]
			if arg == '-i':
				self.path = argv[ idx+1 ]
				idx += 2
			elif arg == '-x':
				self.configPath = argv[ idx+1 ]
				idx += 2
			elif arg == '-f':
				self.fmt = argv[ idx+1 ]
				idx += 2
			elif arg == '-t':
				self.type = argv[ idx+1 ]
				idx += 2
			elif arg == '-c':
				self.customer = argv[ idx+1 ]
				idx += 2
			elif arg == '-o':
				self.enableParallel = True
				idx += 1
			else:
				idx += 2
		if not self.path or not self.configPath:
			print 'invalid input arguments'
			print '\tlogs path and config file must be setted\n'
			self.__print_usage()
			return False
		if self.path == '%stdin%':
			print 'will read lines from the stdin'
			self.path = './'
			self.inputType = 'stdin'

		if self.fmt is None and self.customer is not None:
			print 'format not set, use customer', self.customer, ' standard format'
			self.fmt = get_log_fmt( self.customer )
		if self.fmt is not None:
			self.fmtType = self.fmt
			mdfmt = get_module_fmt( self.fmt )
			if mdfmt is not None:
				print 'using module format:', mdfmt
				self.fmt = mdfmt
				self.sorted = is_log_sorted(mdfmt)
			else:
				if self.fmt[0] != '%':
					print 'the log format', self.fmt, 'not exist'
					return False
		return True
						
	def __print_usage( self ):
		print 'Usage:'
		print '\tmain.py -i <logs path> -x <config file> [ -t <log type>  -f <log format> ]'

