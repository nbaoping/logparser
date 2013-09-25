#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#
import pickle
from StringIO import StringIO

from logparser import *
import inspect
from base import *

class TestObj:
	sname = 'class TestObj'
	print 'test ***********', sname

	def __init__( self ):
		self.func = TestObj.printout
		self.name = 'public TestObj'
		self.__name = 'TestObj'
		print 'test object'
		self.printout( 'fasdfa' )

	def printout( self, string ):
		print 'printout function', string
		print '======================'
		print func_name()
		print '**********************'

	def test( self ):
		self.newName = 'new name'
		self.func( self, 'ok' )

	def s_test():
		print 'static func'
	
	def set_value( self, mname, value ):
		self.__dict__[mname] = value

	def get_value( self, mname ):
		return self.__dict__[mname]

class Derived( TestObj ):
	pass


class StatusMap():
	testMap = None
	
	def __init__( self ):
		self.value = 'afdsfdsafsd'


StatusMap.testMap = {
		'test':1213,
		'fdsfds':5454
		}

print '=======================test pickle================'
status = StatusMap()
status.val2 = 13213131

parser = 
buf = StringIO()

pickle.dump( status, buf, 0 )

print buf.getvalue()
