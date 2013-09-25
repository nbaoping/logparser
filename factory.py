#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from analyser import *
from filter import *
from anlyhelper import *

from operator import itemgetter
#======================================================================
#===========parse xml config and create the analysers==================
#======================================================================

class OutputCfg( BaseObject ):
	def __init__( self ):
		self.fmtName = None
		self.exptype = 'raw'
		self.split = True
		self.unitrate = 1
		self.insertValue = None
		self.outList = None


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
		if not logInfo.exist( 'servTime' ):
			logInfo.servTime = 0
		return (logInfo.servTime / 1000000.0, 1)

	def init_value( self, value ):
		return (0, 0)
	
	def update_value( self, oldValue, sampleValue ):
		consumed = oldValue[0] + sampleValue[0]
		total = oldValue[1] + sampleValue[1]
		return (consumed, total)

	def exclude_value( self, value ):
		return value[0] < 0

	def str_value( self, value ):
		consumed = value[0]
		total = value[1]
		if total <= 0:
			return '0'
		average = consumed / total
		average = round( average, 3 )
		return str( average )

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
		itemList = value
		if not self.sorted:
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
		return value < 0

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
				'activeSessions' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_active_sessions),
				'output' : (AnalyserFactory.__parse_dummy, AnalyserFactory.__create_common)
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
			config.sorted = args.sorted
			funcItem = self.__get_create_func( config.type )
			if funcItem is not None:
				anly = funcItem[1]( funcItem[0], config )
				anly.sorted = args.sorted
				if config.insertValue is not None:
					anly.insertValue = config.insertValue
				analysers.append( anly )
			else:
				print 'no create function for analyser', config 
		return analysers

	def __parse_gloabl_config( self, rootNode ):
		paceNode = stimeNode = etimeNode = insertNode = outNode = None
		for cnode in rootNode.childNodes:
			name = cnode.nodeName
			if name == 'pace':
				paceNode = cnode
			elif name == 'startTime':
				stimeNode = cnode 
			elif name == 'endTime':
				etimeNode = cnode
			elif name == 'insertValue':
				insertNode = cnode
			elif name == 'outPath':
				outNode = cnode

		pace = None
		stime = None
		etime = None
		insertValue = None
		outPath = None
		if paceNode:
			pace = int( get_nodevalue(paceNode) )
		if stimeNode:
			stime = seconds_str( get_nodevalue(stimeNode) )
			print stime, str_seconds( stime )
		if etimeNode:
			etime = seconds_str( get_nodevalue(etimeNode) )
			print etime, str_seconds( etime )
		if insertNode:
			insertValue = get_nodevalue(insertNode)
			print 'global insertValue:', insertValue
		if outNode:
			outPath = get_nodevalue( outNode )
		return (pace, stime, etime, insertValue, outNode)

	def __parse_xml( self, inputPath, xmlfile ):
		configList = list()
		doc = minidom.parse( xmlfile )
		root = doc.documentElement
		anlyNodes = get_xmlnode( root, 'analyser' )
		(gpace, gstime, getime, insertValue, outPath) = self.__parse_gloabl_config( root )
		print 'global config', gpace, gstime, getime, insertValue, outPath
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
			if insertValue is not None:
				config.insertValue = insertValue
			if outPath is not None:
				inputPath = outPath

			mkdir( inputPath )
			outsList = self.__parse_outputs_list( node )

			nodeTypeList = get_xmlnode( node, 'type' )
			if (nodeTypeList is None or len(nodeTypeList) == 0) and len(outsList) == 0:
				print 'invalid node', node
				continue
			count += 1
			nodePaceList = get_xmlnode( node, 'pace' )
			nodeStimeList = get_xmlnode( node, 'startTime' )
			nodeEtimeList = get_xmlnode( node, 'endTime' )
			nodeInsertList = get_xmlnode( node, 'insertValue' )
			nodePath = get_xmlnode( node, 'outPath' )

			if len(nodeTypeList) > 0:
				config.type = get_nodevalue( nodeTypeList[0] )
			if nodePaceList:
				config.pace = int( get_nodevalue( nodePaceList[0] ) )
			if nodeStimeList:
				config.startTime = seconds_str( get_nodevalue( nodeStimeList[0] ) )
				print config.startTime
			if nodeEtimeList:
				config.endTime = seconds_str( get_nodevalue(nodeEtimeList[0]) )
				print config.endTime
			if nodeInsertList:
				config.insertValue = get_nodevalue(nodeInsertList[0])
			if nodePath:
				config.outPath = get_nodevalue( nodePath[0] )
			else:
				if len(nodeTypeList) == 1:
					fname = curTimeStr + '_' + str(count) + '_' + config.type + '_' + str(config.pace) + '.txt'
					config.outPath = os.path.join( inputPath, fname )

			filtersList = get_xmlnode( node, 'filters' )
			config.filter = None
			if filtersList is not None and len(filtersList) > 0:
				filtersNode= filtersList[0]
				baseFilter = BaseFilter()
				if baseFilter.parse_xml( filtersNode ):
					config.filter = baseFilter
			
			incount = 0

			#add config for output list
			for ilist in outsList:
				nconfig = AnalyConfig()
				config.copy_object( nconfig )
				nconfig.outList = ilist
				incount += 1
				ntype = 'output'
				nconfig.type = ntype
				funcItem = self.__get_parse_func( ntype )
				if funcItem is not None:
					funcItem[1]( funcItem[0], nconfig, node )
				fname = curTimeStr + '_' + str(count) + '_' + str(incount) + '_' + ntype + '_' + str(nconfig.pace) + '_out_.txt'
				nconfig.outPath = os.path.join( inputPath, fname )
				print 'parsed anlyser', nconfig
				configList.append( nconfig )

			ownFile = len(nodeTypeList) > 1
			#add config for type list
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
					fname = curTimeStr + '_' + str(count) + '_' + str(incount) + '_' + ntype + '_' + str(nconfig.pace) + '.txt'
					nconfig.outPath = os.path.join( inputPath, fname )
				print 'parsed anlyser', nconfig
				configList.append( nconfig )

			total += incount
		print 'total ', total, 'Analysers parsed'
		return configList

	def __parse_outputs_list( self, node ):
		outsList = list()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name == 'outputs':
				outList = self.__parse_outputs( cnode )
				print outList[0]
				print outList[1]
				outsList.append( outList )
		
		print outsList
		return outsList

	def __parse_outputs( self, node ):
		outList = list()
		for cnode in node.childNodes:
			name = cnode.nodeName
			if name == 'output':
				ocfg = self.__parse_output( cnode )
				outList.append( ocfg )

		outList = self.__check_raw_in_list( outList )

		return outList


	def __parse_output( self, node ):
		ocfg = OutputCfg()
		outList = list()

		for cnode in node.childNodes:
			name = cnode.nodeName
			if name.startswith( '#' ):
				continue

			print name, cnode, cnode.childNodes
			value = get_nodevalue( cnode )
			print '\t', value
			if name == 'fmtName':
				ocfg.fmtName = value
			elif name == 'expType':
				ocfg.exptype = value
			elif name == 'split':
				ocfg.split = int( value )
			elif name == 'insertValue':
				ocfg.insertValue = value
			elif name == 'unitrate':
				ocfg.unitrate = float( value )
			elif name == 'output':
				cocfg = self.__parse_output( cnode )
				outList.append( cocfg )

		if len(outList) > 0:
			print '**************&&&&&&&&&&&&&', outList
			outList = self.__check_raw_in_list( outList )
			ocfg.outList = outList
			print '**************&&&&&&&&&&&&&', outList

		return ocfg

	#in raw mode, only raw exptype is allowed
	def __check_raw_in_list( self, outList ):
		rawList = list()
		ridList = list()
		for ocfg in outList:
			if ocfg.exptype == 'raw':
				rawList.append( ocfg )
			else:
				ridList.append( ocfg )

		if len(rawList) > 0:
			for ocfg in ridList:
				print '***********only raw ouput is allowed, will engore the output type', ocfg.exptype
			return rawList

		return outList

	def __parse_outputs_old( self, node ):
		nlist = get_xmlnode( node, 'outputs' )
		outList = list()
		for onode in nlist:
			inodeList = get_xmlnode( onode, 'output' )
			ilist = list()
			hasRaw = False
			for inode in inodeList:
				ocfg = OutputCfg()
				fnodeList = get_xmlnode( inode, 'fmtName' )
				if len(fnodeList) > 0:
					fnode = fnodeList[0]
					ocfg.fmtName = get_nodevalue( fnode )
				tlist = get_xmlnode( inode, 'expType' )
				if len(tlist) > 0:
					ocfg.exptype = get_nodevalue( tlist[0] )
				else:
					ocfg.exptype  = 'raw'

				slist = get_xmlnode( inode, 'split' )
				if len(slist) > 0:
					ocfg.split = int( get_nodevalue(slist[0]) )
				else:
					ocfg.split = True
				insertList = get_xmlnode( inode, 'insertValue' )
				if len(insertList) > 0:
					ocfg.insertValue = get_nodevalue(insertList[0])
				else:
					ocfg.insertValue = None

				rateList = get_xmlnode( inode, 'unitrate' )
				if len(rateList) > 0:
					ocfg.unitrate = float( get_nodevalue(rateList[0]) )
				else:
					ocfg.unitrate = 1

				print ocfg
				if ocfg.exptype == 'raw':
					hasRaw = True
				elif hasRaw:
					print '***********only raw ouput is allowed, will engore the output type', ocfg.exptype
					continue
				ilist.append( ocfg )
			
			if len(ilist) > 0:
				outList.append( ilist )

		return outList

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
		elif config.type == 'output':
			ocfg = config.outList[0]
			if ocfg.exptype == 'raw':
				#for raw type, all the others are raw types
				helper = RawOutputHelper( config.outList )
			else:
				if len(config.outList) > 1:
					helper = OutputsHelper( config.outList, config.pace )
				else:
					helper = OutputHelper( config.outList[0] )
		helper.sorted = config.sorted

		if atype == 'single':
			anly = SingleAnalyser( config, helper )
		return anly

def register_anlyser( anlyType, parse_func, create_func, obj ):
	AnalyserFactory.anlyMap[anlyType] = ( parse_func, create_func, obj )





