#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/11/13*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


import re

from base import *
from logparser import *


class RegexParser( LogParser ):
	def __init__( self, pattern ):
		self.logLevel = 0
		self.pattern = re.compile( pattern )
		if self.pattern is None:
			raise Exception( 'invalid pattern:'+pattern )
		print self.pattern, pattern

	def parse_line( self, line ):
		res = self.pattern.match( line )
		if res is None:
			if self.logLevel > 0:
				print '=====================res none', line
			return None

		logInfo = LogInfo()
		idx = 0
		for field in res.groups():
			idx += 1
			fmtName = form_common_fmt_name( idx )
			logInfo.set_member( fmtName, field )
		return logInfo

class TimeFmt( BaseObject ):
	def __init__( self ):
		self.regex = None
		self.fmt = None

	def is_valid( self ):
		return self.fmt is not None

	def fmt_time( self, timeStr ):
		millStr = None
		if self.regex != None:
			res = self.regex.match( timeStr )
			if res is None:
				return None

			grps = res.groups()
			timeStr = grps[0]
			if len(grps) > 1:
				millStr = grps[1]

		dtime = strptime( timeStr, self.fmt )
		secs = total_seconds( dtime )
		if millStr != None:
			secs += int(millStr) / 1000.0
		return secs


class FmtField( BaseObject ):
	def __init__( self ):
		self.isExp = False
		self.value = None
		self.fmtName = None
		self.fieldType = 'string'
		self.unitRate = 1

	def is_valid( self ):
		return self.value != None and self.fmtName != None

	def init_field( self ):
		if not self.is_valid():
			return

		value = self.value
		pattern = re.compile( '\([$%].*?\)' )
		strList = pattern.split( value )
		tokenList = pattern.findall( value )
		fmtList = self.__to_fmtlist( tokenList )

		self.strList = strList
		self.fmtList = fmtList

		if self.isExp and self.fieldType == 'string':
			self.fieldType = 'float'

	def fmt_field( self, logInfo ):
		valList = self.__to_value_list( self.fmtList, logInfo )
		fieldStr = ''
		idx = 0
		vsize = len(valList)
		for vstr in self.strList:
			if idx < vsize:
				fieldStr += vstr + str(valList[idx])
			else:
				fieldStr += vstr
			idx += 1
		
		if self.isExp:
			value = eval( fieldStr )
		else:
			value = fieldStr

		return self.__fmt_value( logInfo, self.fmtName, value )

	def __fmt_value( self, logInfo, fmtName, value ):
		ftype = self.fieldType
		if ftype == 'int':
			value = int(value) * self.unitRate
		elif ftype == 'float':
			value = float(value) * self.unitRate
		elif ftype == 'timeFmt':
			value = self.timeFmt.fmt_time( value )
		elif ftype == 'fieldFmt':
			fieldLogInfo = self.fieldParser.parse_field( value )
			if fieldLogInfo is not None:
				self.fieldParser.update_field( logInfo, fieldLogInfo )
				return True
			return False
		logInfo.set_member( fmtName, value )
		return True

	def __to_fmtlist( self, tokenList ):
		fmtList = list()
		for token in tokenList:
			token = token[1:len(token)-1]
			if token.startswith( '$' ):
				fmtName = std_fmt_name( token )
			else:
				fmtName = get_name_by_fmt( token )
			fmtList.append( fmtName )
		return fmtList

	def __to_value_list( self, fmtList, logInfo ):
		valList = list()
		for fmtName in fmtList:
			val = logInfo.get_member( fmtName )
			valList.append( val )
		return valList


class FieldParser( BaseObject ):
	def __init__( self ):
		self.logLevel = 0
		self.regex = None
		self.fieldList = None
		self.parser = None
		self.case = None

	def is_valid( self ):
		return self.parser is not None

	def init_parser( self ):
		if self.regex is None:
			return False

		self.parser = RegexParser( self.regex )
		return True

	def parse_field( self, fieldValue ):
		if not self.is_valid():
			return None
		if self.fieldList is None:
			return None

		case = self.case
		if case is not None:
			if case == 'lower':
				fieldValue = fieldValue.lower()
			elif case == 'upper':
				fieldValue = fieldValue.upper()

		logInfo = self.parser.parse_line( fieldValue )
		if logInfo is None:
			if self.logLevel > 0:
				print '&&&&&&&&&&&', fieldValue, self.regex
			return None
		
		for field in self.fieldList:
			field.fmt_field( logInfo )

		return logInfo

	def update_field( self, orgLogInfo, fieldLogInfo ):
		for field in self.fieldList:
			fmtName = field.fmtName
			value = fieldLogInfo.get_member( fmtName )
			orgLogInfo.set_member( fmtName, value )


class LogFormatter( BaseObject ):
	def __init__( self ):
		self.logTimeFiled = None
		self.servTimeField = None
		self.fieldList = None
		self.beStrict = False

	def fmt_log( self, logInfo ):
		if logInfo is None or self.fieldList is None:
			return logInfo

		ret = True
		for field in self.fieldList:
			rc = field.fmt_field( logInfo )
			if not rc:
				ret = False

		if self.beStrict and not ret:
			return None
		return logInfo

	def parse_xml( self, node ):
		fieldList = self.__parse_all_fields( node )
		if len(fieldList) > 0:
			self.fieldList = fieldList

	def __parse_all_fields( self, node ):
		fieldList = list()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			if name == 'logTime':
				field = self.__parse_field_node( cnode )
				field.fmtName = 'recvdTime'
				self.logTimeFiled = field
			elif name == 'servTime':
				field = self.__parse_field_node( cnode )
				field.fmtName = 'servTime'
				self.servTimeField = field
			elif name == 'field':
				field = self.__parse_field_node( cnode )
			elif name == 'strict':
				nodeValue = get_nodevalue( cnode )
				self.beStrict = (int( nodeValue ) != 0)
				continue

			print field
			fieldList.append( field )
			
		return fieldList

	def __parse_field_node( self, node ):
		field =  FmtField()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			nodeValue = get_nodevalue( cnode )
			if name == 'value':
				field.isExp = False
				field.value = nodeValue
			elif name == 'expValue':
				field.isExp = True
				field.value = nodeValue
			elif name == 'fmtName':
				field.fmtName = nodeValue
			elif name == 'type':
				field.fieldType = nodeValue
			elif name == 'timeFmt':
				timeFmt = self.__parse_time_fmt_node( cnode )
				if timeFmt is not None:
					field.fieldType = 'timeFmt'
					field.timeFmt = timeFmt
			elif name == 'fieldFmt':
				parser = self.__parse_field_parser_node( cnode )
				if parser is not None:
					field.fmtName = '__fieldFmt__'
					field.fieldType = 'fieldFmt'
					field.fieldParser = parser
			elif name == 'unitRate':
				field.unitRate = float(nodeValue)

		field.init_field()
		return field

	def __parse_time_fmt_node( self, node ):
		timeFmt = TimeFmt()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			nodeValue = get_nodevalue( cnode )
			if name == 'match':
				timeFmt.regex = re.compile(nodeValue)
			elif name == 'fmt':
				timeFmt.fmt = nodeValue

		if timeFmt.is_valid():
			return timeFmt
		return None

	def __parse_field_parser_node( self, node ):
		parser = FieldParser()
		fieldList = list()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			nodeValue = get_nodevalue( cnode )
			if name == 'match':
				parser.regex = nodeValue
			elif name == 'case':
				parser.case = nodeValue
			elif name == 'field':
				field = self.__parse_field_node( cnode )
				if field.is_valid():
					fieldList.append( field )
				else:
					print 'invalid field,', field

		if len(fieldList) > 0:
			parser.fieldList = fieldList

		parser.init_parser()
		if parser.is_valid():
			return parser
		return None


