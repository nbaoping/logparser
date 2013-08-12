#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


import sys
import signal 

from parxact import *
from factory import *
import cProfile
import pstats
from userdefined import *

class TestAnalyser( Analyser ):
	def __init__( self, config ):
		super( TestAnalyser, self ).__init__( config )
		pass

	def anly_zero_pace( self, logInfo ):
		#TODO
		return True

	def anly_negative_pace( self, logInfo ):
		#TODO
		return True

	def anly_pace( self, logInfo ):
		#TODO
		return True

	def close( self ):
		#TODO
		return True



class TestFactory( object ):
	def __init__( self ):
		pass

	def register( self ):
		register_anlyser( 'test', TestFactory.__parse, TestFactory.__create, self )

	def __parse( self, node ):
		print 'parse TestFactory'

	def __create( self, config ):
		anly = TestAnalyser( config )
		return anly

class TestFieldParser( IFieldParser ):
	def __init__( self ):
		pass

	def parse_field( self, logInfo, field, fmt ):
		#TODO add customized parsing here
		return True

#args.fmt = '%a %A %b %D %h %H %I %m %O %q %r %>s %t %T %U %V %X'
def parse_args():
	args = InputArgs()
	if not args.parse_argv( sys.argv ):
		return None
	return args

def main():
	args = parse_args()
	if not args:
		return ;
	factory = TestFactory()
	factory.register()
	userDefinedCtx = UserDefinedCtx()
	userDefinedCtx.register_user_defined()
	#use customized field parser
	args.fieldParser = TestFieldParser()
	parser = XactParser( )

	def signal_handler(signal, frame):
		print 'You pressed Ctrl+C!'
		if parser is not None:
			parser.close()
		else:
			print 'parser none'
		sys.exit(0)

	signal.signal(signal.SIGINT, signal_handler)
	print 'begin to parse ...'
	parser.parse( args )

def profile_main():
	args = InputArgs()
	#args.path = './logs'
	args.path = './logs'
	args.path = '/home/neil/customer/telstra/625703709/datas/SR-625703709_haydc-ca-10_logs_20130513/we_translogs'
	args.configPath = './basic.xml'
	args.customer = 'telstra'
	args.fmt = get_log_fmt( args.customer )
	factory = TestFactory()
	factory.register()
	#use customized field parser
	args.fieldParser = TestFieldParser()
	parser = XactParser( )

	def signal_handler(signal, frame):
		print 'You pressed Ctrl+C!'
		if parser is not None:
			parser.close()
		else:
			print 'parser none'
		sys.exit(0)

	signal.signal(signal.SIGINT, signal_handler)
	print 'begin to parse ...'
	parser.parse( args )

if __name__ == '__main__':
	main()
	#profile_main()
	#cProfile.run( 'profile_main()', 'profile.dat' )

