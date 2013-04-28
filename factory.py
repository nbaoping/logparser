from analyser import *


#parse analysers xml config


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
				'status' : (AnalyserFactory.__parse_status, AnalyserFactory.__create_status)
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


def register_anlyser( anlyType, parse_func, create_func, obj ):
	AnalyserFactory.anlyMap[anlyType] = ( parse_func, create_func, obj )





