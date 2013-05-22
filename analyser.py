#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


from datetime import timedelta
import os
from cStringIO import StringIO
from  xml.dom import  minidom

from base import *
from sampler import *

BUF_TIME = 36000		#in seconds
NUM_THRES = 36000


class AnalyConfig( BaseObject ):
	def __init__( self ):
		self.type = ''
		self.startTime = 0
		self.endTime = -1
		self.pace = 0
		self.outPath = ''

	def __str__( self ):
		return str( self.__dict__ )

	def exist( self, member ):
		return member in self.__dict__


class Analyser( object ):
	def __init__( self, config, toFile = True ):
		self.startTime = config.startTime
		self.endTime = config.endTime
		self.pace = config.pace
		if toFile:
			self.outPath = config.outPath
			self.fout = open( config.outPath, 'w' )
			self.ferr = open( config.outPath + '.errlog', 'w' )
			print self.fout
		self.filter = config.filter

	def __str__( self ):
		buf = 'startTime:' + str(self.startTime) + ','
		buf += 'endTime:' + str(self.endTime) + ','
		buf += 'pace:' + str(self.pace)
		if 'outPath' in self.__dict__:
			buf += ',outPath:' + self.outPath
		return buf

	def get_sample_start_time( self, logInfo ):
		startTime = self.startTime
		if startTime <= 0:
			startTime = logInfo.recvdTime - BUF_TIME
		return startTime

	def get_sample_end_time( self, logInfo ):
		return self.endTime

	def analyse_log( self, logInfo ):
		#control the startTime
		if self.startTime > 0:
			etime = logInfo.recvdTime + logInfo.servTime / 1000000
			if etime < self.startTime:
				return False
		#control the endTime
		if self.endTime > 0:
			if logInfo.recvdTime > self.endTime:
				return False
		if self.filter is not None:
			if not self.filter.filter(logInfo):
				return False
		ret = False
		if self.pace == 0:
			ret = self.anly_zero_pace( logInfo )
		elif self.pace < 0:
			ret = self.anly_negative_pace( logInfo )
		else:
			ret = self.anly_pace( logInfo )
		if not ret:
			self.ferr.write( logInfo.originLine + '\n' )

	def anly_zero_pace( self, logInfo ):
		raise_virtual( func_name() )

	def anly_negative_pace( self, logInfo ):
		raise_virtual( func_name() )

	def anly_pace( self, logInfo ):
		raise_virtual( func_name() )

	def close( self ):
		self.ferr.close()
		raise_virtual( func_name() )

	def exist( self, member ):
		return member in self.__dict__


class BandwidthAnalyser( Analyser ):
	def __init__( self, config ):
		super( BandwidthAnalyser, self ).__init__( config )
		self.totalSent = 0
		self.sampler = None
		self.hasWritten = False

	def anly_zero_pace( self, logInfo ):
		servTime = logInfo.servTime / 1000000.0		#to second
		sampleTime = logInfo.recvdTime
		totalSent = logInfo.bytesSentAll
		#print 'total sent:', self.totalSent, 'serv time:', self.servTime
		band = totalSent * 8.0 / servTime / 1024 / 1024
		band = round( band, 3 )
		dtime = to_datetime( sampleTime )
		log = str_time( dtime ) + '\t' + str( band ) + '\t' + str(servTime) + '\t' + str(totalSent) + '\n'
		self.fout.write( log )
		return True

	def anly_negative_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		servTime = logInfo.servTime / 1000000
		if servTime == 0:
			servTime = 1
		value = logInfo.bytesSentAll
		#print 'servTime', servTime, 'num', num, logInfo.servTime, value
		if self.sampler.add_sample( logInfo.recvdTime, value ) != 0:
			return False
		return True

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		servTime = logInfo.servTime / 1000000
		if servTime == 0:
			servTime = 1
		num = servTime / self.sampler.pace + 1
		value = logInfo.bytesSentAll / float(num)
		#print 'servTime', servTime, 'num', num, logInfo.servTime, value
		ret = self.sampler.add_samples( logInfo.recvdTime, value, num )
		if ret < 0:
			if self.hasWritten:
				print 'old log', logInfo
			return False	#TODO
		elif ret > 0:		#need to flash the buffer to the file
			ctime = logInfo.recvdTime
			while ret > 0:
				ctime += ret * self.sampler.pace
				num -= ret
				ret = self.sampler.add_samples( ctime, value, num )
		return True

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, BandwidthAnalyser.__flush_callback, self )
		self.sampler = Sampler( sargs )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		bufio = StringIO()
		print 'flush buffer, curTime:', str_seconds(curTime), 'size:', len(blist)
		for value in blist:
			#print 'flash value', value
			if value != 0:
				#print 'dump log', curTime, value
				dtime = to_datetime( curTime + toAdd )
				tstr = str_time( dtime )
				band = round( value * 8 / float(sampler.pace) / 1024 / 1024, 3 )
				bufio.write( tstr )
				bufio.write( '\t' )
				bufio.write( str(band) )
				bufio.write( '\t' )
				bufio.write( str(curTime) )
				bufio.write( '\n' )
			curTime += sampler.pace
		ostr = bufio.getvalue()
		self.fout.write( ostr )
		self.hasWritten = True

	def close( self ):
		print 'close', self.__class__, self
		if self.sampler is not None:
			#print self.sampler.slist1
			#print self.sampler.slist2
			self.sampler.flush()
			self.sampler = None
		self.fout.close()



class StatusAnalyser( Analyser ):
	def __init__( self, config ):
		super( StatusAnalyser, self ).__init__( config )
		self.sampler = None

	def anly_zero_pace( self, logInfo ):
		tstr = str_seconds( logInfo.recvdTime )
		log = tstr + ',' + str(logInfo) + '\n'
		self.fout.write( log )
		return True

	def anly_negative_pace( self, logInfo ):
		return self.anly_pace( logInfo )

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		value = dict()
		value[logInfo.status] = 1
		servTime = logInfo.servTime / 1000000 + 1
		ret = self.sampler.add_sample( logInfo.recvdTime + servTime, value )
		if ret != 0:
			return False
		return True

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, StatusAnalyser.__flush_callback, self )
		self.sampler = MutableSampler( sargs, StatusAnalyser.__init_value, StatusAnalyser.__update_value )

	def __init_value( self, value ):
		return dict()
	
	def __update_value( self, oldValue, sampleValue ):
		for status in sampleValue.keys():
			count = sampleValue[ status ]
			if status in oldValue:
				count += oldValue[ status ]
			oldValue[status] = count
		return oldValue

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		bufio = StringIO()
		print 'flush buffer, curTime:', str_seconds(curTime), 'size:', len(blist)
		for item in blist:
			if len(item) != 0:
				sampleTime = curTime + toAdd
				tstr = str_seconds( sampleTime )
				bufio.write( tstr )
				bufio.write( ';' )
				bufio.write( str(item) )
				bufio.write( '\n' )
			curTime += sampler.pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def close( self ):
		print 'close', self.__class__, self
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None
		self.fout.close()


class XactRateAnalyser( Analyser ):
	def __init__( self, config ):
		super( XactRateAnalyser, self ).__init__( config )
		self.sampler = None

	def anly_zero_pace( self, logInfo ):
		tstr = str_seconds( logInfo.recvdTime )
		log = tstr + ',' + str(logInfo) + '\n'
		self.fout.write( log )
		return True

	def anly_negative_pace( self, logInfo ):
		return self.anly_pace( logInfo )

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		ret = self.sampler.add_sample( logInfo.recvdTime, 1 )
		if ret != 0:
			return False
		return True

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo)
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, XactRateAnalyser.__flush_callback, self )
		self.sampler = Sampler( sargs )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		bufio = StringIO()
		print 'flush buffer, curTime:', str_seconds(curTime), 'size:', len(blist)
		for item in blist:
			if item > 0:
				sampleTime = curTime + toAdd
				tstr = str_seconds( sampleTime )
				bufio.write( tstr )
				bufio.write( ',' )
				bufio.write( str(item) )
				bufio.write( '\n' )
			curTime += sampler.pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def close( self ):
		print 'close', self.__class__, self
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None
		self.fout.close()


class DescAnalyser( Analyser ):
	def __init__( self, config ):
		super( DescAnalyser, self ).__init__( config )
		self.sampler = None

	def anly_zero_pace( self, logInfo ):
		tstr = str_seconds( logInfo.recvdTime )
		log = tstr + ',' + str(logInfo) + '\n'
		self.fout.write( log )
		return True

	def anly_negative_pace( self, logInfo ):
		return self.anly_pace( logInfo )

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		if not logInfo.exist( 'requestDes' ):
			logInfo.requestDes = 'null'
		value = dict()
		value[logInfo.requestDes] = 1
		servTime = logInfo.servTime / 1000000 + 1
		ret = self.sampler.add_sample( logInfo.recvdTime + servTime, value )
		if ret != 0:
			return False
		return True

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, DescAnalyser.__flush_callback, self )
		self.sampler = MutableSampler( sargs, DescAnalyser.__init_value, DescAnalyser.__update_value )

	def __init_value( self, value ):
		return dict()
	
	def __update_value( self, oldValue, sampleValue ):
		for des in sampleValue.keys():
			count = sampleValue[ des ]
			if des in oldValue:
				count += oldValue[ des ]
			oldValue[des] = count
		return oldValue

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		bufio = StringIO()
		print 'flush buffer, curTime:', str_seconds(curTime), 'size:', len(blist)
		for item in blist:
			if len(item) != 0:
				sampleTime = curTime + toAdd
				tstr = str_seconds( sampleTime )
				bufio.write( tstr )
				bufio.write( ';' )
				bufio.write( str(item) )
				bufio.write( '\n' )
			curTime += sampler.pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def close( self ):
		print 'close', self.__class__, self
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None
		self.fout.close()

class AnalyserHelper( object ):
	def __init__( self ):
		self.sampleThres = NUM_THRES
	
	#return the statistics value
	def get_value( self, logInfo ):
		raise_virtual( func_name() )

	def init_value( self, value ):
		raise_virtual( func_name() )
	
	def update_value( self, oldValue, sampleValue ):
		raise_virtual( func_name() )

	def exclude_value( self, value ):
		raise_virtual( func_name() )

	def str_value( self, value ):
		raise_virtual( func_name() )

	def get_split( self ):
		return ';'


class SingleAnalyser( Analyser ):
	def __init__( self, config, helper ):
		super( SingleAnalyser, self ).__init__( config )
		self.sampler = None
		self.__helper = helper

	def anly_zero_pace( self, logInfo ):
		tstr = str_seconds( logInfo.recvdTime )
		log = tstr + ',' + str(logInfo) + '\n'
		self.fout.write( log )
		return True

	def anly_negative_pace( self, logInfo ):
		return self.anly_pace( logInfo )

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		if not logInfo.exist( 'requestDes' ):
			logInfo.requestDes = 'null'
		value = self.__helper.get_value( logInfo )
		servTime = logInfo.servTime / 1000000 + 1
		ret = self.sampler.add_sample( logInfo.recvdTime + servTime, value )
		if ret != 0:
			print 'add sample failed', servTime, value, ret
			print self.sampler
			return False
		return True

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, self.__helper.sampleThres, SingleAnalyser.__flush_callback, self )
		self.sampler = MutableSampler( sargs, SingleAnalyser.__init_value, SingleAnalyser.__update_value )

	def __init_value( self, value ):
		return self.__helper.init_value( value )
	
	def __update_value( self, oldValue, sampleValue ):
		return self.__helper.update_value( oldValue, sampleValue )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		bufio = StringIO()
		print 'flush buffer, curTime:', str_seconds(curTime), 'size:', len(blist)
		print '\tanalyser:', self
		split = self.__helper.get_split()
		for item in blist:
			if not self.__helper.exclude_value( item ):
				vstr = self.__helper.str_value( item )
				if vstr is not None:
					if split is not None:
						sampleTime = curTime + toAdd
						tstr = str_seconds( sampleTime )
						bufio.write( tstr )
						bufio.write( split )
					bufio.write( vstr )
					bufio.write( '\n' )
			curTime += sampler.pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def close( self ):
		print 'close', self.__class__, self
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None
		self.fout.close()

