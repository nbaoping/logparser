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
		self.pattern = re.compile( pattern )
		print self.pattern, pattern

	def parse_line( self, line ):
		res = self.pattern.match( line )
		if res is None:
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

		self.__fmt_value( logInfo, self.fmtName, value )

	def __fmt_value( self, logInfo, fmtName, value ):
		ftype = self.fieldType
		if ftype == 'int':
			value = int(value) * self.unitRate
		elif ftype == 'float':
			value = float(value) * self.unitRate
		elif ftype == 'timeFmt':
			value = self.timeFmt.fmt_time( value )
		logInfo.set_member( fmtName, value )

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


class LogFormatter( BaseObject ):
	def __init__( self ):
		self.logTimeFiled = None
		self.servTimeField = None
		self.fieldList = None

	def fmt_log( self, logInfo ):
		if logInfo is None or self.fieldList is None:
			return

		for field in self.fieldList:
			field.fmt_field( logInfo )

	def parse_xml( self, node ):
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

			field.init_field()
			print field
			fieldList.append( field )
		
		if len(fieldList) > 0:
			self.fieldList = fieldList

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
			elif name == 'unitRate':
				field.unitRate = float(nodeValue)

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


