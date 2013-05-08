from analyser import *

#======================================================================
#===========parse xml config and create the analysers==================
#======================================================================

class DesHelper( AnalyserHelper ):
	def __init__( self ):
		pass
	
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

	def is_empty( self, value ):
		return len(value) == 0

	def str_value( self, value ):
		return str( value )


class ConsumedHelper( AnalyserHelper ):
	def __init__( self ):
		pass
	
	#return the statistics value
	def get_value( self, logInfo ):
		if not logInfo.exist( 'stime' ):
			logInfo.stime = 0
		return logInfo.stime / 1000000

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		return oldValue + sampleValue

	def is_empty( self, value ):
		return value <= 0

	def str_value( self, value ):
		value = round( value, 3 )
		return str( value )



def get_attrvalue(node, attrname):
     return node.getAttribute(attrname)

def get_nodevalue(node, index = 0):
    return node.childNodes[index].nodeValue.encode('utf-8','ignore')

def get_xmlnode(node, name):
    return node.getElementsByTagName(name)

class AnalyserFactory:
	anlyMap = dict()

	def __init__( self ):
		self.__standardMap = {
				'bandwidth' : (AnalyserFactory.__parse_bandwidth, AnalyserFactory.__create_bandwidth),
				'status' : (AnalyserFactory.__parse_status, AnalyserFactory.__create_status),
				'xactrate' : (AnalyserFactory.__parse_xactrate, AnalyserFactory.__create_xactrate),
				'requestdes' : (AnalyserFactory.__parse_requestdes, AnalyserFactory.__create_requestdes),
				'consumed' : (AnalyserFactory.__parse_consumed, AnalyserFactory.__create_others)
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
			funcItem = self.__get_parse_func( config.type )
			if funcItem is not None:
				funcItem[1]( funcItem[0], node )
			print 'parsed anlyser', config
			configList.append( config )
		print 'total ', count, 'Analysers parsed'
		return configList

	def __parse_bandwidth( self, node ):
		pass

	def __parse_status( self, node ):
		pass

	def __create_bandwidth( self, config ):
		anly = BandwidthAnalyser( config )
		return anly
	
	def __create_status( self, config ):
		anly = StatusAnalyser( config )
		return anly

	def __parse_xactrate( self, node ):
		pass

	def __create_xactrate( self, config ):
		anly = XactRateAnalyser( config )
		return anly

	def __parse_requestdes( self, node ):
		pass

	def __create_requestdes( self, config ):
		helper = DesHelper()
		anly = SingleAnalyser( config, helper )
		return anly

	def __parse_consumed( self, node ):
		pass

	def __create_others( self, config ):
		atype = 'single'
		helper = None
		anly = None
		if config.type == 'consumed':
			helper = ConsumedHelper()
		if atype == 'single':
			anly = SingleAnalyser( config, helper )
		return anly

def register_anlyser( anlyType, parse_func, create_func, obj ):
	AnalyserFactory.anlyMap[anlyType] = ( parse_func, create_func, obj )





