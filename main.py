import sys
from parxact import *
from factory import *

class TestFactory( object ):
	def __init__( self ):
		pass

	def register( self ):
		register_anlyser( 'xactrate', TestFactory.__parse, TestFactory.__create, self )

	def __parse( self, node ):
		print 'parse TestFactory'

	def __create( self, config ):
		anly = XactRateAnalyser( config )
		return anly


#args.fmt = '%a %A %b %D %h %H %I %m %O %q %r %>s %t %T %U %V %X'
def parse_args():
	args = InputArgs()
	if not args.parse_argv( sys.argv ):
		print 'invalid input arguments'
		return None
	return args

def main():
	args = parse_args()
	if not args:
		return ;
	factory = TestFactory()
	factory.register()
	parser = XactParser( )
	parser.parse( args )

if __name__ == '__main__':
	main()
