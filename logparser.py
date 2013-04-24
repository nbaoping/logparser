

class LogParser:
	def __init__( self ):
		pass

	def parse_line( self, line ):
		raise 'derived class must implement function parse_line'


#======================================================================
#================implement parser for web-engine translog==============
#======================================================================
WE_XACTLOG_APACHE_STR = "%a %u %O %b %I %m %>s %t %D"
WE_XACTLOG_EXT_SQUID_STR = "%Z %D %a %R/%>s %O %m %u %M"


class WELogInfo:
	def __init__( self ):
		pass
	
	def setCip( self, ip ):
		self.cip = ip

	def setUri( self, uri ):
		self.uri = uri

	def setMethod( self, method ):
		self.method = method

	def setRtime( self, rtime ):
		self.rtime = rtime

	def setSent( self, sent ):
		self.sent = sent

	def setAllSent( self, sent ):
		self.allSent = sent

	def setStime( self, stime ):
		self.stime = stime			#in microsecond

	def setStatus( self, status ):
		self.status = status
	
	def setRequestDes( self, des ):
		self.requestDes = des

	def setMimeType( self, mimeType ):
		self.mimeType = mimeType

	def setDummy( self, dummy ):
		pass

#private global variables
__weStrFmtSet = ['%b', '%D', '%I', ]
__weIntFmtSet = ['%a', '%A', '%h', '%H', 'm']

#implement parser for extsqu format

def __parse_string( field ):
	return field

def __parse_integer( field ):
	return int(field)

class WELogParser( LogParser ):
	__fmtmap = {
			'%a':(WELogParser.__parseString, WELogInfo.setCip),
			'%A':(WELogParser.__parseString, WELogInfo.setDummy),
			'%b':(WELogParser.__parseInt, WELogInfo.setSent),
			'%D':(WELogParser.__parseInt, WELogInfo.setStime),
			'%h':(WELogParser.__parseString, WELogInfo.setDummy),
			'%H':(WELogParser.__parseString, WELogInfo.setDummy),
			'%I':(WELogParser.__parseInt, WELogInfo.setDummy),
			'%m':(WELogParser.__parseString, WELogInfo.setMethod),
			'%M':(WELogParser.__parseString, WELogInfo.setMimeType),
			'%O':(WELogParser.__parseInt, WELogInfo.setAllSent),
			'%q':(WELogParser.__parseString, WELogInfo.setDummy),
			'%r':(WELogParser.__parseString, WELogInfo.setDummy),
			'%R':(WELogParser.__parseString, WELogInfo.setRequestDes),
			'%>s':(WELogParser.__parseInt, WELogInfo.setStatus),
			'%t':(WELogParser.__parseStandardTime, WELogInfo.setDummy),
			'%T':(WELogParser.__parseFloat, WELogInfo.setDummy),
			'%u':(WELogParser.__parseString,WELogInfo.setUri),
			'%U':(WELogParser.__parseString, WELogInfo.setDummy),
			'%V':(WELogParser.__parseString, WELogInfo.setDummy),
			'%X':(WELogParser.__parseString, WELogInfo.setDummy),
			'%Z':(WELogParser.__parseRecvdTime, WELogInfo.setRtime)
			}


	def __init__( self, fmt = None ):
		self.__parsefuncs = None
		if fmt is not None:
			init_format( self, fmt )

	def init_format( self, fmt ):
		self.__parsefuncs = list()
		fmt = fmt.strip()
		segs = fmt.split()
		for seg in segs:
			pfuncs = (WELogParser.parseOthers, WELogInfo.setDummy)
			if seg in __fmtmap:
				pfuncs = __fmtmap[seg]
			self.__parsefuncs.append( pfuncs )

	def parse_line( self, line ):
		if self.__parsefuncs is None:
			print 'fatal error, format not setted'
			return None
		i = 0
		fields = line.split()
		logInfo = WELogInfo()
		for funcs in self.__parsefuncs:
			if not funcs[0]( field, logInfo, funcs[1] ):
				print 'parse line failed', line
				return None
			i += 1
		return logInfo

	def __parseString( field, logInfo, set_func ):
		set_func( logInfo, field )
		return True

	def __parseInt( field, logInfo, set_func ):
		try:
			value = int( field )
			set_func( logInfo, value )
		except:
			print 'not a integer string', field
			return False
		return True

	def __parseFloat( field, logInfo, set_func ):
		try:
			value = float( field )
			set_func( logInfo, value )
		except:
			print 'not a float string', field
			return False
		return True

	def __parseStandardTime( field, logInfo, set_func ):
		#set_func( logInfo, int(field) )
		return True

	def __parseServedTime( field, logInfo, set_func ):
		#set_func( logInfo, int(field) )
		return True

	def __parseRecvdTime( field, logInfo, set_func ):
		#set_func( logInfo, int(field) )
		return True

	def __parseOthers( field, logInfo, set_func ):
		return True

	def __parseDummy( field, logInfo, set_func ):
		return True
