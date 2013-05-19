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

from customer import *

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

def time_str( tstr ):
	return datetime.strptime( tstr, __TIME_FMT )

def seconds_str( tstr ):
	dtime = datetime.strptime( tstr, __TIME_FMT )
	return total_seconds( dtime )

def str_seconds( seconds ):
	dtime = to_datetime( seconds )
	return str_time( dtime )

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

	def exist_member( self, mname ):
		return mname in self.__dict__

	def copy_object( self, obj ):
		for mname in self.__dict__.keys():
			value = self.__dict__[mname]
			obj.set_member( mname, value )

	def __str__( self ):
		return str( self.__dict__ )

class InputArgs:
	def __init__( self ):
		self.path = None
		self.type = 'extsqu'
		self.fmt = None
		self.configPath = None
		self.fieldParser = None
		self.customer = None

	def parse_argv( self, argv ):
		idx = 1
		size = len(argv)
		while idx < size:
			if idx+1 >= size:
				print 'invalid input arguments'
				self.__print_usage()
				return False
			arg = argv[ idx ]
			value = argv[ idx+1 ]
			if arg == '-i':
				self.path = value
			elif arg == '-x':
				self.configPath = value
			elif arg == '-f':
				self.fmt = value
			elif arg == '-t':
				self.type = value
			elif arg == '-c':
				self.customer = value
			idx += 2
		if not self.path or not self.configPath:
			print 'invalid input arguments'
			print '\tlogs path and config file must be setted\n'
			self.__print_usage()
			return False
		if self.fmt is None and self.customer is not None:
			print 'format not set, use customer', self.customer, ' standard format'
			self.fmt = get_log_fmt( self.customer )
		return True
						
	def __print_usage( self ):
		print 'Usage:'
		print '\tmain.py -i <logs path> -x <config file> [ -t <log type>  -f <log format> ]'

