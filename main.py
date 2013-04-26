import sys
from parxact import *

def parse_args():
	args = InputArgs()
	args.path = sys.argv[1]
	args.type = 'extsqu'
	args.pace = int( sys.argv[2] )
	args.fmt = '%a %A %b %D %h %H %I %m %O %q %r %>s %t %T %U %V %X'
	return args

def main():
	args = parse_args()
	parser = XactParser( )
	parser.parse( args )

if __name__ == '__main__':
	main()
