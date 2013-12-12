import os
import sys
import logging

def get_run_cmd( lpath ):
	args = ''
	idx = 1
	while idx < len(sys.argv):
		arg = sys.argv[idx]
		idx += 1
		args += arg + ' '
		if arg == '-i':
			args += lpath + ' '
			idx += 1

	cmd = 'python main.py ' + args
	return cmd

def run_one_dir( lpath ):
	cmd = get_run_cmd( lpath )
	logging.info( cmd )
	os.system( cmd )

def walk_run( logDir ):
	for ldir in os.listdir(logDir):
		lpath = os.path.join( logDir, ldir )
		if os.path.isdir(lpath):
			run_one_dir( lpath )

def get_log_dir():
	idx = 0
	while idx < len(sys.argv):
		arg = sys.argv[idx]
		if arg == '-i':
			return sys.argv[idx+1]
		idx += 1

if __name__ == '__main__':
	logDir = get_log_dir()
	walk_run( logDir )
