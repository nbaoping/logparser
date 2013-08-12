#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/08/09*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from base import *
from analyser import *
from factory import *

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



class UserDefinedCtx:
	def __init__( self ):
		pass

	def register_user_defined( self ):
		register_anlyser( 'burstissue', UserDefinedCtx.__parse_dummy, UserDefinedCtx.__create_analyser, self )

	def __parse_dummy( self, config, node ):
		pass

	def __create_analyser( self, config ):
		atype = 'single'
		helper = None
		anly = None
		if config.type == 'burstissue':
			helper = BurstHelper()

		if atype == 'single':
			anly = SingleAnalyser( config, helper )
		return anly




