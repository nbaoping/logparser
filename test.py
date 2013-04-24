

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

	def test( self ):
		self.newName = 'new name'
		self.func( self, 'ok' )

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
print to.__name

