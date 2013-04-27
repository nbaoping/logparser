from base import *
from datetime import timedelta
import os
from cStringIO import StringIO
from  xml.dom import  minidom

BUF_TIME = 36000		#in seconds
NUM_THRES = 36000

class Sampler( object ):
	def __init__( self, startTime, pace, bufTime, numThres ):
		if pace == 0:
			raise Exception( 'sampler not support zero pace' )
		print 'create sample bufTime', bufTime, 'numThres', numThres, 'pace', pace
		if startTime < 0:
			startTime = 0
		self.startTime = startTime
		self.pace = pace
		num = bufTime / pace
		if num > numThres:
			num = numThres
		self.__total = num * 2
		self.__buf1 = [0] * num
		self.__buf2 = [0] * num
		self.slist1 = self.__buf1
		self.slist2 = self.__buf2

	#@return:
	#	-1: failed
	#	 0: success
	#	 1: failed but full
	def add_sample( self, time, value ):
		idx = (time - self.startTime) / self.pace
		if idx < 0:
			return -1
		return self.__add_sample( idx, value )

	def add_samples( self, startTime, value, num ):
		#print 'add samples', startTime, value, num
		idx = int( (startTime - self.startTime) / self.pace )
		tail = num + idx
		if idx < 0:
			if tail < 0:
				return -1
			idx = 0
		count = 0
		while idx < tail:
			if self.__add_sample( idx, value ) == 1:
				return count
			idx += 1
			count += 1
		return 0

	def __add_sample( self, idx, value ):
		#print 'add sample', idx, value
		if idx >= self.__total:
			return 1
		size = self.__total / 2
		if idx >= size:
			idx -= size
			self.slist2[idx] = self.slist2[idx] + value
		else:
			self.slist1[idx] = self.slist1[idx] + value
		return 0

	def flash_buffer( self ):
		curList = self.slist1
		self.startTime += self.pace * self.__total / 2
		self.slist1 = self.slist2
		self.slist2 = curList
		return curList

	def clear_buffer( self, blist ):
		self.__init_list( blist )

	def __init_list( self, blist ):
		idx = 0
		size = len(blist)
		while idx < size:
			blist[idx] = 0
			idx += 1


class Analyser( object ):
	def __init__( self, startTime, endTime, pace ):
		self.startTime = startTime
		self.endTime = endTime
		self.pace = pace

	#@return:
	#	-1: failed
	#	 0: success but not finish one pace
	#	 1: success and finish one pace
#	def do_pace( self, logInfo ):
#		if self.pace == 0:
#			return 1
#		elif self.pace > 0:
#			lastTime = self.__lastTime
#			self.__lastTime = logInfo.rtime
#			curTime = logInfo.rtime
#			if self.paceTime < curTime:
#				#self.servTime = delta_time( self.curTime, lastTime )
#				self.servTime = self.pace
#				self.sampleTime = self.curTime + self.servTime / 2
#				self.curTime = logInfo.rtime
#				self.paceTime = self.curTime + self.pace
#				if self.servTime == 0:
#					print 'zero serv time cur pace>>', self.curTime, self.servTime, self.sampleTime, self.__lastTime
#					self.servTime = self.pace
#				return 1
#			return 0
#		return -1

	def analyse_log( self, logInfo ):
		raise Exception( 'derived must implement analyse_log virtual function' )

	def close( self ):
		raise Exception( 'derived must implement close virtual function' )

class BandwidthAnalyser( Analyser ):
	def __init__( self, outPath, startTime, endTime, pace ):
		super( BandwidthAnalyser, self ).__init__( startTime, endTime, pace )
		self.outPath = outPath
		self.fout = open( outPath, 'w' )
		print self.fout
		self.totalSent = 0
		self.sampler = None
		self.hasWritten = False

	def analyse_log( self, logInfo ):
		if self.pace == 0:
			return self.__anly_zero_pace( logInfo )
		elif self.pace > 0:
			return self.__anly_pace( logInfo )

	def __anly_zero_pace( self, logInfo ):
		servTime = logInfo.stime / 1000000.0		#to second
		sampleTime = logInfo.rtime
		totalSent = logInfo.allSent
		#print 'total sent:', self.totalSent, 'serv time:', self.servTime
		band = totalSent * 8.0 / servTime / 1024 / 1024
		band = round( band, 3 )
		dtime = to_datetime( sampleTime )
		log = str_time( dtime ) + '\t' + str( band ) + '\t' + str(servTime) + '\t' + str(totalSent) + '\n'
		self.fout.write( log )
		return True

	def __anly_pace( self, logInfo ):
		if self.sampler is None:
			if self.startTime <= 0:
				self.startTime = logInfo.rtime
			self.sampler = Sampler( self.startTime - BUF_TIME, self.pace, BUF_TIME, NUM_THRES )
		servTime = logInfo.stime / 1000000
		if servTime == 0:
			servTime = 1
		num = servTime / self.sampler.pace + 1
		value = logInfo.allSent / float(num)
		#print 'servTime', servTime, 'num', num, logInfo.stime, value
		ret = self.sampler.add_samples( logInfo.rtime, value, num )
		if ret < 0:
			print 'old log', logInfo
			return False	#TODO
		elif ret > 0:		#need to flash the buffer to the file
			ctime = logInfo.rtime
			while ret > 0:
				self.__flash_buffer()
				ctime += ret * self.sampler.pace
				num -= ret
				ret = self.sampler.add_samples( ctime, value, num )
		return True

	def __flash_buffer( self ):
		curTime = self.sampler.startTime
		blist = self.sampler.flash_buffer()
		toAdd = self.sampler.pace / 2
		bufio = StringIO()
		print 'flash buffer', curTime, 'size', len(blist)
		for value in blist:
			#print 'flash value', value
			if value != 0:
				#print 'dump log', curTime, value
				dtime = to_datetime( curTime + toAdd )
				tstr = str_time( dtime )
				band = round( value * 8 / float(self.sampler.pace) / 1024 / 1024, 3 )
				bufio.write( tstr )
				bufio.write( '\t' )
				bufio.write( str(band) )
				bufio.write( '\t' )
				bufio.write( str(curTime) )
				bufio.write( '\n' )
			curTime += self.sampler.pace
		self.sampler.clear_buffer( blist )
		ostr = bufio.getvalue()
		#print ostr
		self.fout.write( ostr )
		self.fout.flush()

	def close( self ):
		print 'close BandwidthAnalyser'
		if self.sampler is not None:
			#print self.sampler.slist1
			#print self.sampler.slist2
			self.__flash_buffer()
			self.__flash_buffer()
			self.sampler = None
		self.fout.close()

class AnalyConfig( object ):
	def __init__( self ):
		self.type = ''
		self.startTime = 0
		self.endTime = -1
		self.pace = 0
		self.outPath = ''

	def __str__( self ):
		return str( self.__dict__ )



def get_attrvalue(node, attrname):
     return node.getAttribute(attrname)

def get_nodevalue(node, index = 0):
    return node.childNodes[index].nodeValue.encode('utf-8','ignore')

def get_xmlnode(node, name):
    return node.getElementsByTagName(name)

class AnalyserFactory:
	def __init__( self ):
		self.parseMap = {
				'bandwidth' : AnalyserFactory.__parse_bandwidth
				}
		self.createMap = {
				'bandwidth' : AnalyserFactory.__create_bandwidth
				}

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
			if config.type in self.createMap:
				cfunc = self.createMap[ config.type ]
				anly = cfunc( self, config )
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
			if config.type in self.parseMap:
				parfunc = self.parseMap[ config.type ]
				parfunc( self, node )
			print 'parsed ', config
			configList.append( config )
		print 'total ', count, 'Analysers parsed'
		return configList

	def __parse_bandwidth( self, node ):
		pass

	def __create_bandwidth( self, config ):
		anly = BandwidthAnalyser( config.outPath, config.startTime, config.endTime, config.pace )
		return anly














