from datetime import datetime
from base import *

class LogParser( object ):
	def __init__( self ):
		pass

	def parse_line( self, line ):
		raise Exception( 'derived class must implement function parse_line' )


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

	def setRtimeT( self, rtime ):
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

	def exist( self, member ):
		return member in self.__dict__

	def __str__( self ):
		return str(self.__dict__)
		#log = str_time( self.__dict__['rtime'] )
		#for key in self.__dict__.keys():
		#	if key != 'rtime':
		#		log += '\t' + key + ':' + str( self.__dict__[key] )
		#return log
#		print 'cip:', self.cip
#		print 'uri:', self.uri
#		print 'method:', self.method
#		print 'rtime:', self.rtime
#		print 'sent:', self.sent
#		print 'allSent:', self.allSent
#		print 'status:', self.status
#		print 'requestDes:', self.requestDes
#		print 'mimeType:', self.mimeType

#private global variables
__weStrFmtSet = ['%b', '%D', '%I', ]
__weIntFmtSet = ['%a', '%A', '%h', '%H', 'm']

#implement parser for extsqu format

class IFieldParser( object ):
	def __init__( self ):
		pass

	def parse_field( self, logInfo, field, fmt ):
		raise Exception( 'derived class must implement function parse_field' )


def __parse_string( field ):
	return field

def __parse_integer( field ):
	return int(field)

class WELogParser( LogParser ):
	fmtmap = None

	def __init_parser( self ):
		WELogParser.fmtmap = {
			'%a':(WELogParser.parseString, WELogInfo.setCip),
			'%A':(WELogParser.parseString, WELogInfo.setDummy),
			'%b':(WELogParser.parseInt, WELogInfo.setSent),
			'%D':(WELogParser.parseInt, WELogInfo.setStime),
			'%h':(WELogParser.parseString, WELogInfo.setDummy),
			'%H':(WELogParser.parseString, WELogInfo.setDummy),
			'%I':(WELogParser.parseInt, WELogInfo.setDummy),
			'%m':(WELogParser.parseString, WELogInfo.setMethod),
			'%M':(WELogParser.parseString, WELogInfo.setMimeType),
			'%O':(WELogParser.parseInt, WELogInfo.setAllSent),
			'%q':(WELogParser.parseString, WELogInfo.setDummy),
			'%r':(WELogParser.parseString, WELogInfo.setDummy),
			'%R':(WELogParser.parseString, WELogInfo.setRequestDes),
			'%>s':(WELogParser.parseInt, WELogInfo.setStatus),
			'%t':(WELogParser.parseStandardTime, WELogInfo.setRtimeT),
			'%T':(WELogParser.parseFloat, WELogInfo.setDummy),
			'%u':(WELogParser.parseString,WELogInfo.setUri),
			'%U':(WELogParser.parseString, WELogInfo.setDummy),
			'%V':(WELogParser.parseString, WELogInfo.setDummy),
			'%X':(WELogParser.parseString, WELogInfo.setDummy),
			'%Z':(WELogParser.parseRecvdTime, WELogInfo.setRtime)
			}

	def __init__( self, fmt = None, fieldParser = None ):
		if WELogParser.fmtmap is None:
			self.__init_parser()
		self.__parsefuncs = None
		self.timeFmt = '[%d/%b/%Y:%H:%M:%S'
		self.__fieldParser = fieldParser
		if fmt is not None:
			self.init_format( fmt )

	def init_format( self, fmt ):
		self.__parsefuncs = list()
		fmt = fmt.strip()
		segs = fmt.split()
		for seg in segs:
			pfuncs = (WELogParser.parseOthers, WELogInfo.setDummy, seg)
			if seg in WELogParser.fmtmap:
				pfuncs = WELogParser.fmtmap[seg]
				pfuncs = (pfuncs[0], pfuncs[1], seg)
			self.__parsefuncs.append( pfuncs )

	def parse_line( self, line ):
		if self.__parsefuncs is None:
			print 'fatal error, format not setted'
			return None
		i = 0
		fields = line.split( ' ' )
		if len(fields) < len(self.__parsefuncs):
			print 'invalid line:', line
			return None
		logInfo = WELogInfo()
		#print 'size:', len( self.__parsefuncs )
		for funcs in self.__parsefuncs:
			#print funcs, i
			if not funcs[0]( self, fields[i], logInfo, funcs[1], funcs[2] ):
				print 'parse line failed', line
				return None
			i += 1
		return logInfo

	def parseString( self, field, logInfo, set_func, fmt ):
		set_func( logInfo, field )
		return True

	def parseInt( self, field, logInfo, set_func, fmt ):
		try:
			value = int( field )
			set_func( logInfo, value )
		except:
			print 'not a integer string', field, set_func
			return False
		return True

	def parseFloat( self, field, logInfo, set_func, fmt ):
		try:
			value = float( field )
			set_func( logInfo, value )
		except:
			print 'not a float string', field, set_func
			return False
		return True

	def parseStandardTime( self, field, logInfo, set_func, fmt ):
		idx = field.rfind( '+' )
		tstr = field[ 0:idx ]
		timeFmt = '[%d/%b/%Y:%H:%M:%S'
		dtime = datetime.strptime( tstr, timeFmt )
		dtime = total_seconds( dtime )
		set_func( logInfo, dtime )
		return True

	def parseServedTime( self, field, logInfo, set_func, fmt ):
		#set_func( logInfo, int(field) )
		return True
	
	#[21/Apr/2013:00:54:59.848+0000]
	#%d/%b/%Y:%H:%M:%S.%f
	def parseRecvdTime( self, field, logInfo, set_func, fmt ):
		segs = field.split( '.' )
		dtime = datetime.strptime( segs[0], self.timeFmt )
		dtime = total_seconds( dtime )
		set_func( logInfo, dtime )
		return True

	def parseOthers( self, field, logInfo, set_func, fmt ):
		ret = False
		if fmt[0] == '%':
			ret = self.__parse_combined( field, logInfo, fmt )
		if not ret and self.__fieldParser:
			ret = self.__fieldParser.parse_field( logInfo, field, fmt )
		return ret

	def __parse_combined( self, field, logInfo, fmt ):
		fmtList = self.__split_fmt( fmt )
		fieldList = self.__split_fields( field, fmtList )
		ret = False
		for item in fieldList:
			if item[0] in WELogParser.fmtmap:
				funcs = WELogParser.fmtmap[ item[0] ]
				funcs[0]( self, item[1], logInfo, funcs[1], item[0] )
				ret = True
			else:
				if self.__fieldParser is not None:
					self.__fieldParser.parse_field( logInfo, item[1], item[0] )
		return ret


	def __split_fmt( self, fmt ):
		idx = 0
		fmtList = list()
		size = len(fmt)
		while idx < size:
			off = 2
			if fmt[idx] == '%':
				if idx+1 >= size:
					break
				item = None
				if fmt[idx+1] == '>':
					if idx+2 >= size:
						break
					item = fmt[idx:idx+3]
					off = 3
				else:
					item = fmt[idx:idx+2]
				if item:
					fmtList.append( item )
			else:
				print 'error char', fmt[idx]
				break
			nidx = fmt.find( '%', idx+off )
			if nidx == -1:
				item = fmt[idx+off:size]
				if len(item) > 0:
					fmtList.append( item )
				break
			item = fmt[idx+off:nidx]
			fmtList.append( item )
			idx = nidx
		return fmtList

	def __split_fields( self, field, fmtList ):
		idx = 1
		fsize = len(fmtList)
		fidx = 0
		fieldList = list()
		while idx < fsize:
			nidx = field.find( fmtList[idx], fidx )
			if nidx < 0:
				break
			fmt = fmtList[idx-1]
			item = field[ fidx:nidx ]
			fieldList.append( (fmt, item) )
			fidx = nidx + len(fmtList[idx])		#skip the split string
			idx += 2
		fieldSize = len(field)
		if fidx < fieldSize:
			item = field[ fidx:fieldSize]
			fmt = fmtList[ idx-1 ]
			fieldList.append( (fmt, item) )
		return fieldList


	def parseDummy( self, field, logInfo, set_func, fmt ):
		return True

