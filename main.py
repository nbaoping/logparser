import sys
from parxact import *

def parse_args():
	args = InputArgs()
	args.path = sys.argv[1]
	args.type = 'extsqu'
	return args

def main():
	args = parse_args()
	parser = XactParser( )
	parser.parse( args )

if __name__ == '__main__':
	main()
