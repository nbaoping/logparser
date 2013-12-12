#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


import re
import traceback
import sys
import logging

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
	'originLine'	: 'string'
}

def register_filter( name, ftype ):
	__nameTypeMap[name] = ftype

def get_name_type( name ):
	if name in __nameTypeMap:
		return __nameTypeMap[name]
	return None

def check_name( name ):
	return name in __nameTypeMap

class Filter( Expression ):
	def __init__( self, fmtName ):
		self.fmtName = fmtName
		self.isCommonFmt = is_common_fmt_name( fmtName )

	def is_true( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		if not self.isCommonFmt:
			return self.filter( value )
		else:
			#for common fmtName, should handle exception since we don't know the value type
			try:
				return self.filter( value )
			except:
				logging.debug( '\n'+traceback.format_exc() )
				return False


	def filter( self, value ):
		raise_virtual( func_name() )

class SingleFilter( Filter ):
	def __init__( self, fmtName, exp ):
		super(SingleFilter, self).__init__( fmtName )
		self.exp = exp
	
	def filter( self, value ):
		if self.exp is None:
			return True
		return self.exp.is_true( value )


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


class LowValueExp( Expression ):
	def __init__( self, lowValue ):
		super(LowValueExp, self).__init__()
		self.lowValue = lowValue 

	def is_true( self, value ):
		if self.lowValue == 'min':
			return True
		
		value = float( value )
		return self.lowValue <= value


class HighValueExp( Expression ):
	def __init__( self, highValue ):
		super(HighValueExp, self).__init__()
		self.highValue = highValue 

	def is_true( self, value ):
		if self.highValue == 'max':
			return True
		value = float( value )
		return self.highValue >= value


class EqualExp( Expression ):
	def __init__( self, value ):
		super(EqualExp, self).__init__()
		self.value = value

	def is_true( self, value ):
		value = str( value )
		return self.value == value


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
		logging.debug( 'parsing filter xml '+str(self.exp) )
		return self.exp is not None

	def filter( self, logInfo ):
		if self.exp is None:
			return True
		return self.exp.is_true( logInfo )

	def __parse_normal( self, node ):
		name = node.nodeName
		if name == 'filter':
			return self.__parse_filter( node )
		elif name == 'id':
			idstr = get_nodevalue( node )
			self.idstr = idstr
		return None

	def __parse_filter( self, node ):
		fmtNodeList = get_xmlnode( node, 'fmtName' )
		if fmtNodeList is None or len(fmtNodeList) == 0:
			logging.warn( 'no fmtName in filter' )
			return None
		fmtName = get_nodevalue( fmtNodeList[0] )
		fmtName = std_fmt_name( fmtName )
		#if not check_name(fmtName) and not is_common_fmt_name(fmtName):
			#print 'invalid filter name:', fmtName
			#return None

		typeNodeList = get_xmlnode( node, 'type' )
		if typeNodeList is not None and len(typeNodeList) > 0:
			ftype = get_nodevalue( typeNodeList[0] )
		else:
			ftype = get_name_type( fmtName )
			if ftype is None:
				#if not is_common_fmt_name(fmtName):
					#print 'invalid fmtName:', fmtName
					#return None
				ftype = 'string'
		expList = list()

		slist = parse_exp_from_xml( node, BaseFilter.__parse_filter_args, self )
		exp = None
		if slist is not None and len(slist) > 0:
			if len(slist) == 1:
				exp = slist[0]
			else:
				exp = AndExp( slist )
		logging.debug( 'expression:'+str(exp) )
		filter = SingleFilter( fmtName, exp )
		expList.append( filter )
		logging.info( 'parsed one filter'+str(filter) )

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

	def __parse_filter_args( self, node ):
		expList = list()
		name = node.nodeName
		if name == 'keyword':
			value = get_nodevalue( node )
			kwordExp = KeywordExp( value )
			expList.append( kwordExp )
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
		elif name == 'equal':
			value = get_nodevalue( node )
			equalExp = EqualExp( value )
			expList.append( equalExp )
		elif name == 'low':
			value = get_nodevalue( node )
			if value != 'min':
				value = float( value )
			lowExp = LowValueExp( value )
			expList.append( lowExp )
		elif name == 'high':
			value = get_nodevalue( node )
			if value != 'max':
				value = float( value )
			highExp = HighValueExp( value )
			expList.append( highExp )
		else:
			return None
		return expList


