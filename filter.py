#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


import re

from base import *
from expression import *

__nameTypeMap = {
	'clientIp'		: 'string',
	'seIp'			: 'string',
	'bytesSent'		: 'value',
	'bitrate'		: 'string',
	'lookupTime'	: 'value',
	'servTime'		: 'value',
	'encType'		: 'string',
	'storageUrl'	: 'string',
	'sourceUrl'		: 'string',
	'remoteHost'	: 'string',
	'protocol'		: 'string',
	'sessionId'		: 'string',
	'bytesRecvd'	: 'string',
	'trackMethod'	: 'string',
	'method'		: 'string',
	'mimeType'		: 'string',
	'bytesSentAll'	: 'string',
	'queryString'	: 'string',
	'firstLine'		: 'string',
	'description'	: 'string',
	'status'		: 'value',
	'sessionStatus'	: 'string',
	'standardTime'	: 'string',
	'servSeconds'	: 'value',
	'urlAll'		: 'string',
	'url'			: 'string',
	'hostHeader'	: 'string',
	'abrType'		: 'string',
	'connStatus'	: 'string',
	'recvdTime'		: 'string',
}

def get_name_type( name ):
	if name in __nameTypeMap:
		return __nameTypeMap[name]
	return None

class Filter( Expression ):
	def __init__( self, fmtName ):
		self.fmtName = fmtName

	def is_true( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return self.filter( value )

	def filter( self, value ):
		raise_virtual( func_name() )


class ValueFilter( Filter ):
	def __init__( self, fmtName, low, high ):
		super(ValueFilter, self).__init__( fmtName )
		self.low = low
		self.high = high

	def filter( self, value ):
		value = float( value )
		if self.low == 'min':
			return value <= self.high
		elif self.high == 'max':
			return value >= self.low
		else:
			return value >= self.low and value <= self.high


class LengthExp( Expression ):
	def __init__( self, low, high ):
		super(LengthExp, self).__init__()
		self.low = low
		self.high = high

	def is_true( self, string ):
		value = len(string)
		if self.low == 'min':
			return value <= self.high
		elif self.high == 'max':
			return value >= self.low
		else:
			return value >= self.low and value <= self.high


class KeywordExp( Expression ):
	def __init__( self, kword ):
		self.kword = kword

	def is_true( self, string ):
		return string.find( self.kword ) >= 0


class PrefixExp( Expression ):
	def __init__( self, prefix ):
		self.prefix = prefix

	def is_true( self, string ):
		return string.startswith( self.prefix )


class SuffixExp( Expression ):
	def __init__( self, suffix ):
		self.suffix = suffix

	def is_true( self, string ):
		return string.endswith( self.suffix )


class MatchExp( Expression ):
	def __init__( self, pattern ):
		self.regular = re.compile( pattern )

	def is_true( self, string ):
		string = str(string)
		ret = self.regular.match( string )
		return ret is not None


class SearchExp( Expression ):
	def __init__( self, pattern ):
		self.regular = re.compile( pattern )

	def is_true( self, string ):
		string = str(string)
		ret = self.regular.search( string )
		return ret is not None


class StringFilter( Filter ):
	def __init__( self, fmtName, exp ):
		super(StringFilter, self).__init__( fmtName )
		self.exp = exp 

	def filter( self, string ):
		if self.exp is not None:
			return self.exp.is_true( string )
		return True


class BaseFilter( object ):
	def __init__( self ):
		self.exp = None

	def parse_xml( self, node ):
		expList = parse_exp_from_xml( node, BaseFilter.__parse_normal, self )
		if expList is not None and len(expList) > 0:
			if len(expList) == 1:
				self.exp = expList[0]
			else:
				andExp = AndExp( expList )
				self.exp = andExp
		return self.exp is not None

	def filter( self, logInfo ):
		if self.exp is None:
			return True
		return self.exp.is_true( logInfo )

	def __parse_normal( self, node ):
		name = node.nodeName
		if name == 'filter':
			return self.__parse_filter( node )
		return None

	def __parse_filter( self, node ):
		print 'paring filter', node.nodeName
		fmtNodeList = get_xmlnode( node, 'fmtName' )
		if fmtNodeList is None or len(fmtNodeList) == 0:
			print 'no fmtName in filter'
			return None
		fmtName = get_nodevalue( fmtNodeList[0] )

		typeNodeList = get_xmlnode( node, 'type' )
		if typeNodeList is not None and len(typeNodeList) > 0:
			ftype = get_nodevalue( typeNodeList[0] )
		else:
			ftype = get_name_type( fmtName )
			if ftype is None:
				print 'invalid fmtName:', fmtName
				return None
		expList = list()

		if ftype == 'value':
			(low, high) = self.__parse_low_high( node )
			valueFilter = ValueFilter( fmtName, low, high )
			print 'parsed one filter', valueFilter
			expList.append( valueFilter )
		elif ftype == 'string':
			slist = parse_exp_from_xml( node, BaseFilter.__parse_string_args, self )
			exp = None
			print slist
			if slist is not None and len(slist) > 0:
				if len(slist) == 1:
					exp = slist[0]
				else:
					exp = AndExp( slist )
			strFilter = StringFilter( fmtName, exp )
			print 'parsed one filter', strFilter
			expList.append( strFilter )
		return expList

	def __parse_low_high( self, node ):
		lowNodeList = get_xmlnode( node, 'low' )
		highNodeList = get_xmlnode( node, 'high' )
		low = 'min'
		high = 'max'
		if lowNodeList is not None and len(lowNodeList) > 0:
			lowNode = lowNodeList[0]
			low = float( get_nodevalue(lowNode) )
		if highNodeList is not None and len(highNodeList) > 0:
			highNode = highNodeList[0]
			high = float( get_nodevalue(highNode) )
		return (low, high)

	def __parse_string_args( self, node ):
		expList = list()
		name = node.nodeName
		print '******************', name
		if name == 'keyword':
			value = get_nodevalue( node )
			kwordExp = KeywordExp( value )
			expList.append( kwordExp )
			print kwordExp
		elif name == 'prefix':
			value = get_nodevalue( node )
			prefixExp = PrefixExp( value )
			expList.append( prefixExp )
		elif name == 'suffix':
			value = get_nodevalue( node )
			suffixExp = SuffixExp( value )
			expList.append( suffixExp )
		elif name == 'match':
			value = get_nodevalue( node )
			reExp = MatchExp( value )
			expList.append( reExp )
		elif name == 'search':
			value = get_nodevalue( node )
			reExp = SearchExp( value )
			expList.append( reExp )
		elif name == 'length':
			(low, high) = self.__parse_low_high( node )
			lenExp = LengthExp( low, high )
			expList.append( lenExp )
		else:
			return None
		print '=====================', expList
		return expList


