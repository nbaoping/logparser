from analyser import *
from filter import *

from operator import itemgetter
#======================================================================
#===========parse xml config and create the analysers==================
#======================================================================

class DesHelper( AnalyserHelper ):
	def __init__( self ):
		super(DesHelper, self).__init__()
	
	#return the statistics value
	def get_value( self, logInfo ):
		if not logInfo.exist( 'requestDes' ):
			logInfo.requestDes = 'null'
		value = dict()
		value[logInfo.requestDes] = 1
		return value

	def init_value( self, value ):
		return dict()
	
	def update_value( self, oldValue, sampleValue ):
		for des in sampleValue.keys():
			count = sampleValue[ des ]
			if des in oldValue:
				count += oldValue[ des ]
			oldValue[des] = count
		return oldValue

	def exclude_value( self, value ):
		return len(value) == 0

	def str_value( self, value ):
		return str( value )


class ConsumedHelper( AnalyserHelper ):
	def __init__( self ):
		super(ConsumedHelper, self).__init__()
	
	#return the statistics value
	def get_value( self, logInfo ):
		if not logInfo.exist( 'stime' ):
			logInfo.stime = 0
		return logInfo.stime / 1000000

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		return oldValue + sampleValue

	def exclude_value( self, value ):
		return value <= 0

	def str_value( self, value ):
		value = round( value, 3 )
		return str( value )

class TmpconnHelper( AnalyserHelper ):
	def __init__( self, config ):
		super(TmpconnHelper, self).__init__()
		self.servTime = config.servTime
		if config.exist( 'clientMap' ):
			self.clientMap = config.clientMap
		else:
			self.clientMap = None
		print self.clientMap
		if config.exist( 'thresCount' ):
			self.thresCount = config.thresCount
		else:
			self.thresCount = -1
	
	#return the statistics value
	def get_value( self, logInfo ):
		cip = logInfo.clientIp
		count = 0
		if self.__is_valid_response(logInfo):
			servTime = self.servTime + 1
			if logInfo.exist( 'stime' ):
				servTime = int( logInfo.stime / 1000 )		#in millisecond
			if servTime <= self.servTime:
				count = 1
		if self.clientMap is not None:
			if self.__is_target_client(cip):
				return (cip, count)
			return None
		return count

	def __is_valid_response( self, logInfo ):
		status = logInfo.status
		return status == 200 or status == 206 or status == 304

	def __is_target_client( self, cip ):
		if 'all' in self.clientMap or cip in self.clientMap:
			return True
		return False

	def init_value( self, value ):
		if self.clientMap is not None:
			return dict()
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		if self.clientMap is None:
			return oldValue + sampleValue
		if sampleValue is not None:
			count = sampleValue[1]
			cip = sampleValue[0]
			if cip in oldValue:
				count += oldValue[cip]
			oldValue[cip] = count
		return oldValue

	def exclude_value( self, value ):
		if self.clientMap is None:
			return value <= 0
		return len(value) == 0

	def str_value( self, value ):
		if self.clientMap is None:
			return str( value )
		vstr = None
		for cip in value.keys():
			count = value[ cip ]
			if self.thresCount > 0 and count < self.thresCount:
				continue
			cstr = cip + ':' + str( count )
			if vstr is None:
				vstr = cstr
			else:
				vstr += ', ' + cstr
		return vstr


class AssembleHelper( AnalyserHelper ):
	def __init__( self ):
		super(AssembleHelper, self).__init__()
		self.sampleThres = 1000
	
	#return the statistics value
	def get_value( self, logInfo ):
		return (logInfo.recvdTime, logInfo.originLine)

	def init_value( self, value ):
		return list()
	
	def update_value( self, oldValue, sampleValue ):
		oldValue.append( sampleValue )
		return oldValue

	def exclude_value( self, value ):
		return len(value) == 0

	def str_value( self, value ):
		itemList = sorted( value, key=itemgetter(0) )
		bufio = StringIO()
		for item in itemList:
			line = item[1]
			bufio.write( line )
			bufio.write( '\n' )
		return bufio.getvalue()

	def get_split( self ):
		return None

class CounterHelper( AnalyserHelper ):
	def __init__( self ):
		super(CounterHelper, self).__init__()
	
	#return the statistics value
	def get_value( self, logInfo ):
		return 1

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		return oldValue + sampleValue

	def exclude_value( self, value ):
		return value <= 0

	def str_value( self, value ):
		return str( value )

class AnalyserFactory:
	anlyMap = dict()

	def __init__( self ):
		self.__standardMap = {
				'bandwidth' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_bandwidth),
				'status' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_status),
				'xactrate' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_xactrate),
				'requestdes' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_requestdes),
				'consumed' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_common),
				'tmpconn' : (AnalyserFactory.__parse_tmpconn, AnalyserFactory.__create_common),
				'assemble' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_common),
				'counter' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_common)
				}

	def __get_parse_func( self, anlyType ):
		if anlyType in self.__standardMap:
			return ( self, self.__standardMap[ anlyType ][0] )
		if anlyType in self.anlyMap:
			return ( self.anlyMap[anlyType][2], self.anlyMap[anlyType][0] )
		return None

	def __get_create_func( self, anlyType ):
		if anlyType in self.__standardMap:
			return ( self, self.__standardMap[ anlyType ][1] )
		if anlyType in self.anlyMap:
			return ( self.anlyMap[anlyType][2], self.anlyMap[anlyType][1] )
		return None

	def create_from_args( self, args, startTime, endTime ):
		outdir = os.path.join( args.path, RES_DIR )
		mkdir( outdir )
		if args.configPath is not None:
			args.outdir = outdir
			return self.__create_from_config( args )
		analysers = list()
		num = 0
		if args.analyseType == 0:		#bandwidth 
			path = os.path.join( outdir, 'bandwidth_' + str(args.pace) + '_' + str(num) )
			anly = BandwidthAnalyser( path, startTime, endTime, args.pace )
			analysers.append( anly )
		return analysers;

	def __create_from_config( self, args ):
		analysers = list()
		configList = self.__parse_xml( args.outdir, args.configPath )
		for config in configList:
			funcItem = self.__get_create_func( config.type )
			if funcItem is not None:
				anly = funcItem[1]( funcItem[0], config )
				analysers.append( anly )
			else:
				print 'no create function for analyser', config 
		return analysers

	def __parse_xml( self, inputPath, xmlfile ):
		configList = list()
		doc = minidom.parse( xmlfile )
		root = doc.documentElement
		anlyNodes = get_xmlnode( root, 'analyser' )
		count = 0
		for node in anlyNodes:
			config = AnalyConfig()
			nodeType = get_xmlnode( node, 'type' )
			if nodeType is None:
				print 'invalid node', node
				continue
			count += 1
			nodePace = get_xmlnode( node, 'pace' )
			nodeStime = get_xmlnode( node, 'startTime' )
			nodeEtime = get_xmlnode( node, 'endTime' )
			nodePath = get_xmlnode( node, 'outPath' )

			config.type = get_nodevalue( nodeType[0] )
			if nodePace:
				config.pace = int( get_nodevalue( nodePace[0] ) )
			if nodeStime:
				config.startTime = seconds_str( get_nodevalue( nodeStime[0] ) )
			if nodeEtime:
				config.endTime = seconds_str( get_nodevalue(nodeEtime[0]) )
			if nodePath:
				config.outPath = get_nodevalue( nodePath[0] )
			else:
				fname = config.type + '_' + str(config.pace) + '_' + str(count) + '.txt'
				config.outPath = os.path.join( inputPath, fname )

			filtersList = get_xmlnode( node, 'filters' )
			config.filter = None
			if filtersList is not None and len(filtersList) > 0:
				filtersNode= filtersList[0]
				baseFilter = BaseFilter()
				if baseFilter.parse_xml( filtersNode ):
					config.filter = baseFilter

			funcItem = self.__get_parse_func( config.type )
			if funcItem is not None:
				funcItem[1]( funcItem[0], config, node )
			print 'parsed anlyser', config
			configList.append( config )
		print 'total ', count, 'Analysers parsed'
		return configList

	def __create_bandwidth( self, config ):
		anly = BandwidthAnalyser( config )
		return anly
	
	def __create_status( self, config ):
		anly = StatusAnalyser( config )
		return anly

	def __create_xactrate( self, config ):
		anly = XactRateAnalyser( config )
		return anly

	def __create_requestdes( self, config ):
		helper = DesHelper()
		anly = SingleAnalyser( config, helper )
		return anly

	def __parse_dummy( self, config, node ):
		pass

	def __parse_tmpconn( self, config, node ):
		snode = get_xmlnode( node, 'servTime' )
		servTime = int( get_nodevalue(snode[0]) )
		config.servTime = servTime

		cnodeList = get_xmlnode( node, 'client' )
		cmap = None
		if cnodeList is not None:
			for cnode in cnodeList:
				cip = get_nodevalue( cnode )
				if cip == 'all':
					cmap = dict()
					cmap['all'] = 0
					break
				else:
					if cmap is None:
						cmap = dict()
					cmap[cip] = 0
		if cmap is not None:
			config.clientMap = cmap

		tnode = get_xmlnode( node, 'thresCount' )
		if len(tnode) > 0:
			config.thresCount = get_nodevalue( tnode[0] )


	def __create_common( self, config ):
		atype = 'single'
		helper = None
		anly = None
		if config.type == 'consumed':
			helper = ConsumedHelper()
		elif config.type == 'tmpconn':
			helper = TmpconnHelper( config )
		elif config.type == 'assemble':
			helper = AssembleHelper( )
		elif config.type == 'counter':
			helper = CounterHelper()

		if atype == 'single':
			anly = SingleAnalyser( config, helper )
		return anly

def register_anlyser( anlyType, parse_func, create_func, obj ):
	AnalyserFactory.anlyMap[anlyType] = ( parse_func, create_func, obj )





