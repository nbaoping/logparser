#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/08/09*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from StringIO import StringIO
from operator import itemgetter

from base import *
from analyser import *
from factory import *
from errorlog import *

SUCC_STATUS = [ 200, 206, 304 ]

class BurstHelper( AnalyserHelper ):
	def __init__( self ):
		super(BurstHelper, self).__init__()
		self.lastReqMap = dict()
		self.hitMap = dict()
		self.totalHit = 0

	def get_value( self, logInfo ):
		return logInfo 

	def init_value( self, value ):
		return (0, 0)
	
	def update_value( self, oldValue, sampleValue ):
		logInfo = sampleValue
		url = self.__get_url_no_query( logInfo.urlAll )
		description = logInfo.description
		servTime = logInfo.servTime / 1000000.0
		lastLogInfo = None
		lastServTime = None
		if url in self.lastReqMap:
			lastLogInfo = self.lastReqMap[url]
			lastServTime = lastLogInfo.servTime / 1000000.0
		hitIssue = False
		if url in self.hitMap:
			hitIssue = self.hitMap[url]
		isFirstHit = False
		#print hitIssue, lastServTime, servTime

		if hitIssue:
			#check if issue end
			endTime = lastLogInfo.recvdTime + lastServTime
			curTime = logInfo.recvdTime
			#print curTime, endTime
			#print str_seconds(curTime), str_seconds(endTime)
			if curTime >= endTime or description.find('HIT') >= 0:
				hitIssue = False
		elif description.startswith( 'TCP_REFRESH_MISS' ):
			#check if we hit issue the first time
			if lastLogInfo is not None:
				endTime = lastLogInfo.recvdTime + lastServTime
				curTime = logInfo.recvdTime
				if curTime < endTime:
					self.totalHit += 1
					if (self.totalHit%100) == 0:
						print '*****************hit ' + str(self.totalHit) + 'th time*****************'
						print lastLogInfo.originLine
						print logInfo.originLine
						print ''
					isFirstHit = True
					hitIssue = True

		#update last log info if the current logInfo has greater end time
		if not hitIssue:
			if not self.__is_err_response(logInfo.status):
				if lastLogInfo is None:
					self.lastReqMap[url] = logInfo
				else:
					endTime = lastLogInfo.recvdTime + lastServTime
					curEndTime = logInfo.recvdTime + servTime
					if curEndTime > endTime:
						self.lastReqMap[url] = logInfo
				#print self.lastReqMap[url]
				#print ''
		else:
			#update the sample value
			hitCount = oldValue[1] + 1
			marks = oldValue[0]
			if isFirstHit:
				marks += 1
			oldValue = (marks, hitCount)

		#update the issue map
		self.hitMap[url] = hitIssue

		return oldValue

	def exclude_value( self, value ):
		return False

	def str_value( self, value ):
		split = self.get_split()
		return str(value[0]) + split + str(value[1])

	def __get_url_no_query( self, urlAll ):
		idx = urlAll.find( '?' )
		if idx < 0:
			return urlAll
		return urlAll[0:idx]

	def __is_err_response( self, status ):
		return status not in SUCC_STATUS


class ThressSecHelper( AnalyserHelper ):
	def __init__( self ):
		super(ThressSecHelper, self).__init__()
		self.workerMap = dict()

	def get_value( self, logInfo ):
		return logInfo 

	def init_value( self, value ):
		return list()
	
	def update_value( self, oldValue, sampleValue ):
		logInfo = sampleValue
		line = logInfo.originLine
		urllog = 'HTTPRequestReader.cpp:516'
		cancellog = 'Cancelling original feed'
		time = parse_errlog_time( line )
		wid = parse_errlog_workerid( line )
		wid = str(wid)
		if line.find(urllog) > 0:
			url = self.__parse_url( line )
			if url is None:
				return oldValue
			if url.find( '.m3u8' ) > 0:
				self.__add_url_item( time, wid, url )
		elif line.find(cancellog) > 0:
			item = self.__find_url_item( time, wid )
			if item is not None:
				oldValue.append( item )
				#print time, item
		return oldValue

	def __parse_url( self, line ):
		idx = line.find( 'http://' )
		if idx < 0:
			return None
		nidx = line.find( ']', idx )
		if nidx < 0:
			return None
		return line[idx:nidx]

	def __add_url_item( self, time, wid, url ):
		itemList = None
		if wid not in self.workerMap:
			itemList = list()
		else:
			itemList = self.workerMap[wid]
		itemList.append( (time, url) )
		self.workerMap[wid] = itemList

	def __find_url_item( self, time, wid ):
		if wid not in self.workerMap:
			return None

		itemList = self.workerMap[wid]
		size = len(itemList)
		idx = size - 1
		while idx >= 0:
			(urlTime, url) = itemList[idx]
			#print time, urlTime, url
			diff = abs(time - urlTime - 3)
			if diff <= 0.5:
				#we find it 
				break
			idx -= 1
		item = None
		if idx >= 0:
			item  = itemList.pop( idx )
		return item	

	def exclude_value( self, value ):
		if value is None:
			return True
		return False

	def str_value( self, value ):
		ilist = value
		bufio = StringIO()
		bufio.write( '\n' )
		for item in ilist:
			(time, url) = item
			split = self.get_split()
			nline = '\t' + str(time) + split + url + '\n'
			bufio.write( nline )
		return bufio.getvalue()


class Xact3secHelper( AnalyserHelper ):
	def __init__( self, urlFilePath ):
		super(Xact3secHelper, self).__init__()
		self.urlMap = dict()
		self.markMap = dict()
		self.__load_url_map( urlFilePath )
		self.totalHit = 0

	def __load_url_map( self, path ):
		#path = '/home/neil/customer/telstra/626750547/0813/SR-626879315_haydc-cdn220-ca-8_20130805/errorlogs/web/reordered/output/20130814-160942__1_3secissue_60.txt'
		print 'load urls from file:', path
		fin = open( path, 'r' )
		split = self.get_split()
		for line in fin:
			if line[0] != '\t':
				continue
			line = line.strip()
			segs = line.split( split )
			time = float(segs[0])
			url = segs[1]
			url = self.__parse_url_path( url )
			tlist = None
			if url in self.urlMap:
				tlist = self.urlMap[url]
			else:
				tlist = list()
			tlist.append( time )
			self.urlMap[url] = tlist

		for path in self.urlMap.keys():
			item = self.urlMap[path]
			self.markMap[path] = 1
			#print path, item
		fin.close()

	def get_value( self, logInfo ):
		return logInfo 

	def init_value( self, value ):
		return list()
	
	def update_value( self, oldValue, sampleValue ):
		logInfo = sampleValue
		url = logInfo.urlAll
		path = self.__parse_url_path( url )
		time = self.__is_path_in_map( path, logInfo.recvdTime )
		if time is not None:
			if path in self.markMap:
				del self.markMap[path]
			self.totalHit += 1
			if (self.totalHit%1000) == 0:
				print 'got the', str(self.totalHit), 'th one'
				print logInfo.originLine
				print '\t', str_seconds(time)
			oldValue.append( (logInfo.recvdTime, logInfo.originLine, time) )
		return oldValue

	def __parse_url_path( self, url ):
		idx = url.find( '://' )
		idx = url.find( '/', idx+3 )
		return url[idx:len(url)]

	def __is_path_in_map( self, path, rtime ):
		if path in self.urlMap:
			tlist = self.urlMap[path]
			for time in tlist:
				diff = time - rtime
				if diff >= 0 and diff < 0.2:
					return time

		return None

	def exclude_value( self, value ):
		if len(value) == 0:
			return True
		return False

	def str_value( self, value ):
		itemList = sorted( value, key=itemgetter(0) )
		bufio = StringIO()
		bufio.write( '\n' )
		for item in itemList:
			(rtime, oline, time) = item
			split = self.get_split()
			bufio.write( oline )
			bufio.write( '\t' )
			tstr = str_seconds( time )
			bufio.write(tstr)
			bufio.write(',')
			bufio.write( '\n' )
		return bufio.getvalue()

	def on_close( self ):
		for path in self.markMap:
			print 'not used'
			print path


class UserDefinedCtx:
	def __init__( self ):
		pass

	def register_user_defined( self ):
		register_anlyser( 'burstissue', UserDefinedCtx.__parse_dummy, UserDefinedCtx.__create_analyser, self )
		register_anlyser( '3secissue', UserDefinedCtx.__parse_dummy, UserDefinedCtx.__create_analyser, self )
		register_anlyser( 'xact3sec', UserDefinedCtx.__parse_xact3sec, UserDefinedCtx.__create_analyser, self )

	def __parse_dummy( self, config, node ):
		pass

	def __parse_xact3sec( self, config, node ):
		urlfileList = get_xmlnode( node, 'urlfile' )
		if urlfileList is not None and len(urlfileList) > 0:
			config.urlFilePath = get_nodevalue( urlfileList[0] )
		else:
			print 'error for paring xact3sec analyser, no urlfile configured'

	def __create_analyser( self, config ):
		atype = 'single'
		xtype = config.type
		helper = None
		anly = None
		if xtype == 'burstissue':
			helper = BurstHelper()
		elif xtype == '3secissue':
			helper = ThressSecHelper()
		elif xtype == 'xact3sec':
			helper = Xact3secHelper( config.urlFilePath )

		if atype == 'single':
			anly = SingleAnalyser( config, helper )
		return anly




