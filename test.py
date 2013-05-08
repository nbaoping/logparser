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

__func = TestObj.printout

dd = {
		'fdfadsf':908080,
		'ljldsfjsd':423423
	}
print dd
to = TestObj()
to.test()
print TestObj.sname
print to.sname
print to.name
print to.newName
print '=============================================='
func = TestObj.printout
func( to, 'call outside' )


def test_parser():
	line = '[21/Apr/2013:00:54:59.848+0000] 161136 192.148.158.11 TCP_MEM_HIT/200 661052 GET http://smoothorigin.vos.bigpond.com/987_afl_c3004_tbox.isml/QualityLevels(3500000)/Fragments(video=17988522580555) video/mp4 '
	parser = WELogParser( WE_XACTLOG_EXT_SQUID_STR )
	logInfo = parser.parse_line( line )
	print logInfo
	total = total_seconds( logInfo.rtime )
	print total

#test_parser()
