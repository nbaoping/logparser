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

class OutputCfg( BaseObject ):
	def __init__( self ):
		pass

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

class OutputHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutputHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		if self.exptype == 'map':
			self.keyMap = dict()
			self.split = ocfg.split
	
	def get_value( self, logInfo ):
		return logInfo.get_member( self.fmtName )

	def init_value( self, value ):
		exptype = self.exptype
		if exptype == 'sum':
			return 0
		elif exptype == 'average':
			return (0, 0)
		elif exptype == 'map':
			return dict()
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		exptype = self.exptype
		value = oldValue
		if exptype == 'sum':
			value = oldValue + sampleValue
		elif exptype == 'average':
			total = oldValue[0] + sampleValue
			count = oldValue[1] + 1
			value = (total, count)
		elif exptype == 'map':
			count = 1
			if sampleValue in oldValue:
				count += oldValue[sampleValue]
			else:
				self.keyMap[sampleValue] = 1
			oldValue[sampleValue] = count
			value = oldValue
		else:
			value = sampleValue

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		if self.exptype == 'map' and self.split:
			split = self.get_split()
			hstr = ''
			idx = 0
			keyList = self.keyMap.keys()
			for key in keyList:
				if idx > 0:
					hstr += split
				hstr += str(key)
				idx += 1
			return hstr
		return None

	def str_value( self, value ):
		exptype = self.exptype
		if exptype == 'sum':
			return str(value)
		elif exptype == 'average':
			count = value[1]
			total = value[0]
			if count == 0:
				return '0'
			avg = round(total * 1.0 / count, 3)
			return str(avg)
		elif exptype == 'map':
			if not self.split:
				return str(value)
			else:
				idx = 0
				ss = ''
				split = self.get_split()
				keyList = self.keyMap.keys()
				for key in keyList:
					if idx > 0:
						ss += split
					count = 0
					if key in value:
						count = value[key]
					ss += str(count)
					idx += 1
				return ss

		return str( value )

class OutputsHelper( AnalyserHelper ):
	def __init__( self, outList ):
		super(OutputsHelper, self).__init__()
		helperList = list()
		for ocfg in outList:
			helper = OutputHelper( ocfg )
			helperList.append( helper )
		self.size = len(helperList)
		self.helperList = helperList
	
	#return the statistics value
	def get_value( self, logInfo ):
		vlist = list()
		for helper in self.helperList:
			val = helper.get_value( logInfo )
			vlist.append( val )
		return vlist

	def init_value( self, value ):
		vlist = list()
		for helper in self.helperList:
			val = helper.init_value( value )
			vlist.append( val )
		return vlist
	
	def update_value( self, oldValue, sampleValue ):
		idx = 0
		for helper in self.helperList:
			val = helper.update_value( oldValue[idx], sampleValue[idx] )
			oldValue[idx] = val
			idx += 1
		return oldValue

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		hstr = ''
		has = False
		idx = 0
		split = self.get_split()
		for helper in self.helperList:
			tstr = helper.head_str()
			if tstr is None:
				if helper.exptype != 'raw':
					tstr = helper.fmtName + '_' + helper.exptype
				else:
					tstr = helper.fmtName
			else:
				has = True
			
			if idx > 0:
				hstr += split
			hstr += tstr
			idx += 1

		if has:
			return hstr
		return None


	def str_value( self, value ):
		idx = 0
		vstr = ''
		split = self.get_split()
		for helper in self.helperList:
			if idx > 0:
				vstr += split
			vstr += helper.str_value( value[idx] )
			idx += 1

		return vstr

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

			outList = self.__parse_outputs( node )

			nodeTypeList = get_xmlnode( node, 'type' )
			if (nodeTypeList is None or len(nodeTypeList) == 0) and len(outList) == 0:
				print 'invalid node', node
				continue
			count += 1
			nodePaceList = get_xmlnode( node, 'pace' )
			nodeStimeList = get_xmlnode( node, 'startTime' )
			nodeEtimeList = get_xmlnode( node, 'endTime' )
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
			for ilist in outList:
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

	def __parse_outputs( self, node ):
		nlist = get_xmlnode( node, 'outputs' )
		outList = list()
		for onode in nlist:
			inodeList = get_xmlnode( onode, 'output' )
			ilist = list()
			for inode in inodeList:
				ocfg = OutputCfg()
				fnode = get_xmlnode( inode, 'fmtName' )[0]
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
			if len(config.outList) > 1:
				helper = OutputsHelper( config.outList )
			else:
				helper = OutputHelper( config.outList[0] )

		if atype == 'single':
			anly = SingleAnalyser( config, helper )
		return anly

def register_anlyser( anlyType, parse_func, create_func, obj ):
	AnalyserFactory.anlyMap[anlyType] = ( parse_func, create_func, obj )





