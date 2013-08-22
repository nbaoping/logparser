import sys

def test_fmt( ifile ):
	fin = open( ifile, 'r' )
	tokenMap = dict()
	count = 0
	confict = False
	for line in fin:
		count += 1
		token = line.strip()
		if token in tokenMap:
			confict = True
			print 'exist token', token+':'+str(tokenMap[token]), count
			break
		tokenMap[token] = count
	if not confict:
		print 'no token confict'
	fin.close()

if __name__ == '__main__':
	ifile = sys.argv[1]
	test_fmt( ifile )

