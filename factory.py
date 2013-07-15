#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

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
		if not logInfo.exist( 'description' ):
			logInfo.description = 'null'
		value = dict()
		value[logInfo.description] = 1
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
		self.sampleThres = 10000
	
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
		first = True
		for item in itemList:
			line = item[1]
			if first:
				bufio.write( line )
				first = False
			else:
				bufio.write( '\n' )
				bufio.write( line )
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
				'counter' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_common),
				'activeSessions' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_active_sessions)
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

	def __parse_gloabl_config( self, rootNode ):
		paceNode = stimeNode = etimeNode = None
		for cnode in rootNode.childNodes:
			name = cnode.nodeName
			if name == 'pace':
				paceNode = cnode
			elif name == 'startTime':
				stimeNode = cnode 
			elif name == 'endTime':
				etimeNode = cnode

		pace = None
		stime = None
		etime = None
		if paceNode:
			pace = int( get_nodevalue(paceNode) )
		if stimeNode:
			stime = seconds_str( get_nodevalue(stimeNode) )
			print stime, str_seconds( stime )
		if etimeNode:
			etime = seconds_str( get_nodevalue(etimeNode) )
			print etime, str_seconds( etime )
		return (pace, stime, etime)

	def __parse_xml( self, inputPath, xmlfile ):
		configList = list()
		doc = minidom.parse( xmlfile )
		root = doc.documentElement
		anlyNodes = get_xmlnode( root, 'analyser' )
		(gpace, gstime, getime) = self.__parse_gloabl_config( root )
		print 'global config', gpace, gstime, getime
		count = 0
		total = 0
		curTimeStr = cur_timestr() + '_'
		curTimeStr = curTimeStr.replace( '/', '' )
		curTimeStr = curTimeStr.replace( ':', '' )
		for node in anlyNodes:
			config = AnalyConfig()
			#load the default global config
			if gpace is not None:
				config.pace = gpace
			if gstime is not None:
				config.startTime = gstime
			if getime is not None:
				config.endTime = getime

			nodeTypeList = get_xmlnode( node, 'type' )
			if nodeTypeList is None or len(nodeTypeList) == 0:
				print 'invalid node', node
				continue
			count += 1
			nodePaceList = get_xmlnode( node, 'pace' )
			nodeStimeList = get_xmlnode( node, 'startTime' )
			nodeEtimeList = get_xmlnode( node, 'endTime' )
			nodePath = get_xmlnode( node, 'outPath' )

			config.type = get_nodevalue( nodeTypeList[0] )
			if nodePaceList:
				config.pace = int( get_nodevalue( nodePaceList[0] ) )
			if nodeStimeList:
				config.startTime = seconds_str( get_nodevalue( nodeStimeList[0] ) )
				print config.startTime
			if nodeEtimeList:
				config.endTime = seconds_str( get_nodevalue(nodeEtimeList[0]) )
				print config.endTime
			if nodePath:
				config.outPath = get_nodevalue( nodePath[0] )
			else:
				if len(nodeTypeList) == 1:
					fname = curTimeStr + config.type + '_' + str(config.pace) + '_' + str(count) + '.txt'
					config.outPath = os.path.join( inputPath, fname )

			filtersList = get_xmlnode( node, 'filters' )
			config.filter = None
			if filtersList is not None and len(filtersList) > 0:
				filtersNode= filtersList[0]
				baseFilter = BaseFilter()
				if baseFilter.parse_xml( filtersNode ):
					config.filter = baseFilter
			
			ownFile = len(nodeTypeList) > 1
			incount = 0
			for typeNode in nodeTypeList:
				nconfig = AnalyConfig()
				config.copy_object( nconfig )
				incount += 1
				ntype = get_nodevalue( typeNode )
				nconfig.type = ntype
				funcItem = self.__get_parse_func( ntype )
				if funcItem is not None:
					funcItem[1]( funcItem[0], nconfig, node )
				if ownFile:
					fname = curTimeStr + ntype + '_' + str(nconfig.pace) + '_' + str(count) + '_' + str(incount) + '.txt'
					nconfig.outPath = os.path.join( inputPath, fname )
				print 'parsed anlyser', nconfig
				configList.append( nconfig )
			total += incount
		print 'total ', total, 'Analysers parsed'
		return configList

	def __create_bandwidth( self, config ):
		anly = BandwidthAnalyser( config )
		return anly

	def __create_active_sessions( self, config ):
		anly = ActiveSessionsAnalyser( config )
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





