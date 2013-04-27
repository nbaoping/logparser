import sys
from parxact import *

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
	parser = XactParser( )
	parser.parse( args )

if __name__ == '__main__':
	main()
