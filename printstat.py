import pstats
import sys

if __name__ == '__main__':
	sname = sys.argv[1]
	stat = pstats.Stats( sname )
	stat.strip_dirs()
	stat.print_stats()

