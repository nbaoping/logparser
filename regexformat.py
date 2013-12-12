#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/11/13*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


import re
import traceback
import logging

from base import *
from logparser import *
from filter import *


class RegexParser( LogParser ):
	def __init__( self, pattern ):
		self.logLevel = 0
		self.pattern = re.compile( pattern )
		if self.pattern is None:
			raise Exception( 'invalid pattern:'+pattern )
		logging.info( str(self.pattern)+' with pattern:'+pattern )

	def parse_line( self, line ):
		res = self.pattern.match( line )
		if res is None:
			logging.debug( '=====================res none'+line )
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
		super(TimeFmt, self).__init__()
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

class FmtFilters( BaseObject ):
	def __init__( self, parent ):
		super(FmtFilters, self).__init__()
		self.filters = list()
		self.filterMap = dict()
		self.parent = parent

	def add_filter( self, bfilter ):
		idstr = bfilter.idstr
		self.filters.append( bfilter )
		self.filterMap[idstr] = bfilter

	def add_parent( self, parentFilters ):
		self.parent = parentFilters

	def get_filter( self, fid ):
		if fid in self.filterMap:
			return self.filterMap[fid]

		if self.parent is not None:
			return self.parent.get_filter( fid )

		return None


class FieldValue( BaseObject ):
	def __init__( self ):
		super(FieldValue, self).__init__()
		self.isExp = False
		pass

	def form_value( self, logInfo ):
		raise_virtual( func_name() )


class BasicValue( FieldValue ):
	def __init__( self, value, isExp ):
		super(BasicValue, self).__init__()
		self.isExp = isExp
		self.__init_value( value )
		logging.debug( 'BasicValue:'+str(self) )

	def __init_value( self, value ):
		#% is used for translog
		#$ is used for regex
		#@ is used for fmtName
		pattern = re.compile( '\([$%@].*?\)' )
		strList = pattern.split( value )
		tokenList = pattern.findall( value )
		fmtList = self.__to_fmtlist( tokenList )

		self.strList = strList
		self.fmtList = fmtList

	def form_value( self, logInfo ):
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
			try:
				value = eval( fieldStr )
			except:
				logging.debug( '\n'+traceback.format_exc() )
				logging.debug( 'fieldStr:'+fieldStr )
		else:
			value = fieldStr
		
		return value

	def __to_fmtlist( self, tokenList ):
		fmtList = list()
		for token in tokenList:
			#rid the '()'
			token = token[1:len(token)-1]
			if token.startswith( '$' ):
				fmtName = std_fmt_name( token )
			elif token.startswith( '@' ):
				fmtName = token[1:len(token)]
			else:
				fmtName = get_name_by_fmt( token )
			
			if fmtName is None:
				raise Exception( 'fmtName none with token:' + token )
			fmtList.append( fmtName )
		return fmtList

	def __to_value_list( self, fmtList, logInfo ):
		valList = list()
		for fmtName in fmtList:
			val = logInfo.get_member( fmtName )
			valList.append( val )
		return valList


class ConClause( BaseObject ):
	def __init__( self, filters, con, fieldValue ):
		super(ConClause, self).__init__()
		self.con = con
		self.filt = None
		self.bvalue = None
		self.fieldValue = fieldValue
		self.__init_con( filters, con )
		logging.debug( 'ConClause:'+str(self) )

	def form_value( self, logInfo ):
		if self.is_true( logInfo ):
			return self.fieldValue.form_value( logInfo )
		return None

	def is_true( self, logInfo ):
		if self.filt is not None:
			return self.filt.filter( logInfo )

		if self.bvalue is not None:
			ret = self.bvalue.form_value( logInfo )
			if ret:
				return True
			return False

		logging.debug( 'error, no condition available in clause:'+str(self) )
		return False 

	def __init_con( self, filters, con ):
		if con.startswith( '@' ):
			fid = con[1:len(con)]
			filt = filters.get_filter( fid )
			if filt is None:
				raise Exception( 'wrong con:'+con+', no such filter:'+fid )
			self.filt = filt
		else:
			#use the expValue to be the condition
			bvalue = BasicValue( con, True )
			self.bvalue = bvalue


class IfBlockValue( FieldValue ):
	def __init__( self, filters ):
		super(IfBlockValue, self).__init__()
		self.filters = filters
		self.ifClause = None
		self.elifClauseList = None
		self.elseClause = None

	def form_value( self, logInfo ):
		if self.ifClause is None:
			logging.error( 'invalid IfBlockValue:'+str(self) )
			return None

		bvalue = self.ifClause.form_value( logInfo )
		if bvalue is None and self.elifClauseList is not None:
			for clause in self.elifClauseList:
				bvalue = clause.form_value( logInfo )
				if bvalue is not None:
					break

		if bvalue is None and self.elseClause is not None:
			bvalue = self.elseClause.fieldValue.form_value( logInfo )
			
		return bvalue

	def add_if( self, clause ):
		self.ifClause = clause

	def add_elif( self, clause ):
		if self.elifClauseList is None:
			self.elifClauseList = list()

		self.elifClauseList.append( clause )

	def add_else( self, clause ):
		self.elseClause = clause


class BlockParser( BaseObject ):
	def __init__( self, filters ):
		super(BlockParser, self).__init__()
		self.filters = filters

	def parse_block( self, node ):
		fieldValue = None
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			if name == 'if':
				fieldValue = self.__parse_if_block( node )
				break

		return fieldValue

	def __parse_if_block( self, node ):
		ifBlock = None
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			if name == 'if':
				ifBlock = IfBlockValue( self.filters )
				clause = self.__parse_clause( cnode )
				if clause is not None:
					ifBlock.add_if( clause )
			elif name == 'elif':
				if ifBlock is None:
					raise Exception( 'elif appears before the if in'+str(cnode) )
				clause = self.__parse_clause( cnode )
				if clause is not None:
					ifBlock.add_elif( clause )
			elif name == 'else':
				if ifBlock is None:
					raise Exception( 'else appears before the if in'+str(cnode) )
				clause = self.__parse_clause( cnode )
				if clause is not None:
					ifBlock.add_else( clause )

		return ifBlock

	def __parse_clause( self, node ):
		con = get_attrvalue( node, 'con' )
		fieldValue = self.__parse_clause_value( node )
		clause = None
		if fieldValue is not None:
			clause = ConClause( self.filters, con, fieldValue )
		return clause	

	def __parse_clause_value( self, node ):
		fieldValue = None
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			if name == 'value':
				nodeValue = get_nodevalue( cnode )
				fieldValue = BasicValue( nodeValue, False )
			elif name == 'expValue':
				nodeValue = get_nodevalue( cnode )
				fieldValue = BasicValue( nodeValue, True )
			elif name == 'if':
				fieldValue = self.__parse_if_block( cnode )
			break

		return fieldValue


class FmtField( BaseObject ):
	def __init__( self ):
		super(FmtField, self).__init__()
		self.filters = FmtFilters(None)	#make sure all filters can be linked
		self.fieldValue = None
		self.fmtName = None
		self.fieldType = 'string'
		self.unitRate = 1

	def is_valid( self ):
		return self.fieldValue != None and self.fmtName != None

	def init_field( self ):
		if not self.is_valid():
			return

		if self.fieldValue.isExp and self.fieldType == 'string':
			self.fieldType = 'float'

	def fmt_field( self, logInfo ):
		value = self.fieldValue.form_value( logInfo )
		if value is None:
			logging.debug( 'error, failed to format field' )
			return False
		return self.__fmt_value( logInfo, self.fmtName, value )

	def __fmt_value( self, logInfo, fmtName, value ):
		ftype = self.fieldType
		if ftype == 'int':
			value = int(value) * self.unitRate
		elif ftype == 'float':
			value = float(value) * self.unitRate
		elif ftype == 'lowercase':
			value = value.lower()
		elif ftype == 'uppercase':
			value = value.upper()
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


class FieldParser( BaseObject ):
	def __init__( self ):
		super(FieldParser, self).__init__()
		self.filters = FmtFilters(None)
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
			logging.debug( 'parse fieldValue failed, fieldValue:'+fieldValue+',regex:'+str(self.regex) )
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
		super(LogFormatter, self).__init__()
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
				#raise Exception( str(field) )

		if self.beStrict and not ret:
			return None
		return logInfo

	def parse_xml( self, node ):
		(fieldList, filters) = self.__parse_all_fields( node )
		if len(fieldList) > 0:
			self.fieldList = fieldList
			self.filters = filters

		#raise Exception( 'parse end' )

	def __parse_all_fields( self, node ):
		filters = FmtFilters( None )
		fieldList = list()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			if name == 'logTime':
				field = self.__parse_field_node( cnode, filters )
				field.fmtName = 'recvdTime'
				self.logTimeFiled = field
			elif name == 'servTime':
				field = self.__parse_field_node( cnode, filters )
				field.fmtName = 'servTime'
				self.servTimeField = field
			elif name == 'field':
				field = self.__parse_field_node( cnode, filters )
			elif name == 'strict':
				nodeValue = get_nodevalue( cnode )
				self.beStrict = (int( nodeValue ) != 0)
				continue
			elif name == 'filters':
				bfilter = BaseFilter()
				ret = bfilter.parse_xml( cnode )
				if ret:
					filters.add_filter( bfilter )
					continue

			logging.info( 'parsed one field:'+str(field) )
			fieldList.append( field )
		
		return (fieldList, filters)

	def __link_filters( self, parent, fieldList ):
		for field in fieldList:
			if field.filters is not None:
				field.filters.add_parent( parent )

	def __parse_field_node( self, node, parentFilters ):
		filters = FmtFilters( parentFilters )
		field =  FmtField()
		fieldValue = None
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			nodeValue = get_nodevalue( cnode )
			if name == 'value':
				fieldValue = BasicValue( nodeValue, False )
			elif name == 'expValue':
				fieldValue = BasicValue( nodeValue, True )
			elif name == 'blockValue':
				bparser = BlockParser( filters )
				fieldValue = bparser.parse_block( cnode )
			elif name == 'filters':
				bfilter = BaseFilter()
				ret = bfilter.parse_xml( cnode )
				if ret:
					filters.add_filter( bfilter )
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
				parser = self.__parse_field_parser_node( cnode, filters )
				if parser is not None:
					field.fmtName = '__fieldFmt__'
					field.fieldType = 'fieldFmt'
					field.fieldParser = parser
			elif name == 'unitRate':
				field.unitRate = float(nodeValue)

		field.fieldValue = fieldValue
		field.filters = filters
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

	def __parse_field_parser_node( self, node, parentFilters ):
		filters = FmtFilters( parentFilters )
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
			elif name == 'filters':
				bfilter = BaseFilter()
				ret = bfilter.parse_xml( cnode )
				if ret:
					filters.add_filter( bfilter )
			elif name == 'field':
				field = self.__parse_field_node( cnode, filters )
				if field.is_valid():
					fieldList.append( field )
				else:
					logging.error( 'invalid field,'+str(field) )

		if len(fieldList) > 0:
			parser.fieldList = fieldList
			self.__link_filters( filters, fieldList )

		parser.init_parser()
		if parser.is_valid():
			parser.filters = filters
			return parser
		return None


