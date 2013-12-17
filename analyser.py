#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#


from datetime import timedelta
import os
import sys
#from cStringIO import StringIO
from StringIO import StringIO
from  xml.dom import  minidom
import logging

from base import *
from sampler import *
from anlyhelper import *


time_offset = False

class AnalyConfig( BaseObject ):
	def __init__( self ):
		self.type = ''
		self.startTime = 0
		self.endTime = -1
		self.pace = 0
		self.insertValue = None
		self.outPath = ''

	def __str__( self ):
		return str( self.__dict__ )

	def exist( self, member ):
		return member in self.__dict__


class Analyser( BaseObject ):
	def __init__( self, config, toFile = True ):
		self.parser = None	#this may be assigned outside
		self.oneProcessMode = 1
		self.atype = ''
		self.startTime = config.startTime
		self.endTime = config.endTime
		self.startBufTime = 0
		self.pace = config.pace
		self.bufTime = BUF_TIME
		self.sampler = None
		self.tid = -1

		self.fileStartTime = -1
		self.fileEndTime = -1
		self.firstFlush = True
		if toFile:
			self.outPath = config.outPath
			self.errPath = config.outPath + '.errlog' 
			self.fout = None
			self.ferr = None
		self.filter = config.filter

	def set_one_process_mode( self, mode ):
		self.oneProcessMode = mode

	def close_output_files( self ):
		if self.fout is not None:
			logging.info( 'close file:'+str(self.fout) )
			self.fout.close()
			self.fout = None
		if self.ferr is not None:
			logging.info( 'close err file:'+str(self.ferr) )
			self.ferr.close()
			self.ferr = None

	def open_output_files( self ):
		self.close_output_files()
		self.fout = open( self.outPath, 'w' )
		self.errPath = self.outPath + '.errlog' 
		#self.ferr = open( self.errPath, 'w' )
		self.ferr = None
		logging.info( 'open file:'+str(self.fout) )

	def restart( self, startTime, endTime, bufTime ):
		self.startTime = startTime
		self.endTime = endTime
		self.bufTime = bufTime
		if self.sampler is not None:
			args = self.sampler.args
			args.startTime = startTime
			args.endTime = endTime
			self.sampler.reset( args )

	def align_time_to_pace( self, otime ):
		if otime > 0 and self.pace > 0:
			otime = int(otime) / self.pace * self.pace

		return otime

	def get_sample_start_time( self, logInfo ):
		returnTime = self.startTime

		if logInfo is not None:
			startTime = self.startTime
			if self.sorted:
				#for sorted logs, make sure we use the proper start time
				if startTime <= 0 or startTime < logInfo.recvdTime:
					returnTime = logInfo.recvdTime
				else:
					returnTime = startTime
			else:
				if startTime <= 0:
					startTime = logInfo.recvdTime - BUF_TIME
				startTime = self.align_time_to_pace( startTime )
				returnTime = startTime
		elif self.startTime > 0:
			startBufTime = int(self.startBufTime)
			if self.pace > 0:
				startBufTime = startBufTime / self.pace * self.pace
			self.startBufTime = startBufTime
			self.startTime -= startBufTime
			returnTime = self.startTime

		logging.info( 'sampler startTime:'+str_seconds(returnTime) + ',tid:'+ str(self.tid) )
		return returnTime

	def get_sample_end_time( self, logInfo ):
		endTime = self.align_time_to_pace( self.endTime )
		logging.info( 'sampler endTime:'+str_seconds(endTime)+',tid:'+str(self.tid) )
		return endTime

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

		if not ret and self.sampler is not None and self.sampler.startTime > self.startTime:
			if self.ferr is None:
				self.ferr = open( self.errPath, 'w' )
			self.ferr.write( logInfo.originLine + '\n' )

	def anly_zero_pace( self, logInfo ):
		raise_virtual( func_name() )

	def anly_negative_pace( self, logInfo ):
		raise_virtual( func_name() )

	def anly_pace( self, logInfo ):
		raise_virtual( func_name() )

	def decode_output_head( self, hstr ):
		return True

	def encode_output_head( self ):
		return 'time;' + self.atype

	def decode_output_value( self, vstr ):
		raise_virtual( func_name() )

	def anly_outut_value( self, valList ):
		raise_virtual( func_name() )
	
	def prepare_output( self ):
		pass

	def output_head( self ):
		if self.fout is not None and self.firstFlush:
			#someone may need do some init work before output
			self.prepare_output()

			head = self.encode_output_head()
			if head is not None:
				self.fout.write( head+'\n' )
				self.firstFlush = False

	def output_data( self, data, size ):
		self.output_head()

		if self.fout is not None:
			self.output_head()
			self.fout.write( data )

	def flush( self ):
		if self.sampler is not None:
			self.sampler.flush()

	def close( self ):
		self.on_close()

		if self.ferr is not None:
			self.ferr.close()
			self.ferr = None
		if self.fout is not None:
			self.fout.close()
			self.fout = None

	def on_close( self ):
		raise_virtual( func_name() )

	def exist( self, member ):
		return member in self.__dict__

	def __str__( self ):
		ss = 'anly type:' + self.atype + ', output file:' +str(self.fout)
		return ss


class BandwidthAnalyser( Analyser ):
	def __init__( self, config ):
		super( BandwidthAnalyser, self ).__init__( config )
		self.totalSent = 0
		self.sampler = None
		self.hasWritten = False
		self.zeroValueCount = 0
		self.zeroStartTime = 0
		self.hasNoneZero = False

	def anly_zero_pace( self, logInfo ):
		servTime = logInfo.servTime / 1000000.0		#to second
		sampleTime = logInfo.recvdTime
		totalSent = logInfo.bytesSentAll
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
		if self.sampler.add_sample( logInfo.recvdTime, value ) != 0:
			return False
		return True

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		servTime = logInfo.servTime / 1000000.0
		if servTime == 0:
			servTime = 1
		value = logInfo.bytesSentAll
		logTime = logInfo.recvdTime
		startTime = self.sampler.startTime
		pace = self.sampler.pace
		valtul = time_average( value, logTime, servTime, startTime, pace )
		(hval, (mstime, mval, midNum), (metime, endVal)) = valtul
		
		ret = self.sampler.add_sample( logTime, hval)
		if mstime > 0:
			ret = self.sampler.add_samples( mstime, mval, midNum)
			if ret > 0:		#need to flash the buffer to the file
				ctime = mstime
				num = midNum
				while ret > 0:
					ctime += ret * self.sampler.pace
					num -= ret
					ret = self.sampler.add_samples( ctime, value, num )

		if metime > 0:
			ret = self.sampler.add_sample( metime, endVal)

		if ret < 0:
			return False
		return True
	

	def anly_pace_old( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		servTime = logInfo.servTime / 1000000.0
		if servTime == 0:
			servTime = 1
		num = servTime / self.sampler.pace
		if num <= 1:
			value = logInfo.bytesSentAll
		else:
			value = logInfo.bytesSentAll / float(num)
		num = int( num+0.9999 )
		ret = self.sampler.add_samples( logInfo.recvdTime, value, num )
		if ret < 0:
			if self.hasWritten:
				logging.debug( 'old log:'+str(logInfo) )
			return False	#TODO
		elif ret > 0:		#need to flash the buffer to the file
			ctime = logInfo.recvdTime
			while ret > 0:
				logging.debug( '%%%%%%%%%%%%%%%%%%%'+str(ret) )
				ctime += ret * self.sampler.pace
				num -= ret
				ret = self.sampler.add_samples( ctime, value, num )

		if ret < 0:
			return False
		return True


	def anly_outut_value( self, vtime, value ):
		if self.sampler is None:
			startTime = self.get_sample_start_time( None )
			endTime = self.get_sample_end_time( None )
			if startTime <= 0:
				startTime = vtime
			sargs = SamplerArgs( startTime, endTime, self.pace,
				self.bufTime, NUM_THRES, BandwidthAnalyser.__flush_callback, self )
			self.sampler = Sampler( sargs )
		
		ret = self.sampler.add_sample( vtime, value )
		
		return ret

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, BandwidthAnalyser.__flush_callback, self )
		self.sampler = Sampler( sargs )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		pace = sampler.pace
		if pace > 0:
			toAdd = pace / 2
		if not time_offset:
			toAdd = 0
		bufio = StringIO()
		minTime = self.align_time_to_pace( sampler.minTime )
		maxTime = sampler.maxTime
		if pace < 0:
			minTime = maxTime = -1

		logging.info( 'flush data,'+str(self) )

		self.output_head()
		for value in blist:
			if curTime < minTime:
				if value != 0:
					logging.warn( '************************wrong filtering'+\
							'curTime:'+str_seconds(curTime)+',minTime:'+str_seconds(minTime) )
				curTime += pace
				continue
			if maxTime > 0 and curTime > maxTime:
				break
			
			self.__write_line( bufio, value, curTime, toAdd, pace )
			curTime += pace
		ostr = bufio.getvalue()
		self.fout.write( ostr )
		self.hasWritten = True

	def __write_line( self, bufio, value, curTime, toAdd, pace ):
		dtime = to_datetime( curTime + toAdd )
		tstr = str_time( dtime )
		band = round( value * 8 / float(pace) / 1024 / 1024, 3 )
		bufio.write( tstr )
		bufio.write( ';' )
		bufio.write( str(band) )
		bufio.write( '\n' )

	def decode_output_value( self, vstr ):
		segs = vstr.split( ';' )
		seconds = seconds_str( segs[0] )
		pace = self.pace
		if pace < 0:
			pace = 1
		band = float(segs[1]) * 1024 * 1024 / 8 * pace

		return (seconds, band)

	def on_close( self ):
		logging.info( 'close'+str(self.__class__)+str(self) )
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None



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

	def anly_outut_value( self, vtime, value ):
		if self.sampler is None:
			startTime = self.get_sample_start_time( None )
			endTime = self.get_sample_end_time( None )
			if startTime <= 0:
				startTime = int(vtime)
			sargs = SamplerArgs( startTime, endTime, self.pace,
				self.bufTime, NUM_THRES, StatusAnalyser.__flush_callback, self )
			self.sampler = MutableSampler( sargs, StatusAnalyser.__init_value, StatusAnalyser.__update_value )
		
		ret = self.sampler.add_sample( vtime, value )
		
		return ret

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
		pace = sampler.pace
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		if not time_offset:
			toAdd = 0
		bufio = StringIO()
		logging.info( 'flush data,'+str(self) )

		minTime = self.align_time_to_pace( sampler.minTime )
		maxTime = sampler.maxTime
		if pace < 0:
			minTime = maxTime = -1

		self.output_head()
		for item in blist:
			if curTime < minTime:
				curTime += pace
				continue
			if maxTime > 0 and curTime > maxTime:
				break

			if len(item) != 0:
				sampleTime = curTime + toAdd
				tstr = str_seconds( sampleTime )
				bufio.write( tstr )
				bufio.write( ';' )
				bufio.write( str(item) )
				bufio.write( '\n' )
			curTime += pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def decode_output_value( self, vstr ):
		segs = vstr.split( ';' )
		seconds = seconds_str( segs[0] )
		pace = self.pace
		if pace < 0:
			pace = 1
		value = segs[1]

		#decode the map value
		vmap = dict()
		value = value[1:len(value)-1]
		segs = value.split( ',' )
		for seg in segs:
			isegs = seg.split( ':' )
			status = int( isegs[0].strip() )
			count = int( isegs[1].strip() )
			vmap[status] = count

		return (seconds, vmap)

	def on_close( self ):
		logging.info( 'close'+str(self.__class__)+str(self) )
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None


class XactRateAnalyser( Analyser ):
	def __init__( self, config ):
		super( XactRateAnalyser, self ).__init__( config )
		self.sampler = None
		self.zeroValueCount = 0
		self.zeroStartTime = 0
		self.hasNoneZero = False

	def anly_zero_pace( self, logInfo ):
		tstr = str_seconds( logInfo.recvdTime )
		log = tstr + ',' + str(logInfo) + '\n'
		self.fout.write( log )
		return True

	def anly_negative_pace( self, logInfo ):
		return self.anly_pace( logInfo )

		if self.sampler is None:
			self.__create_sampler( logInfo )
		if self.sampler.add_sample( self.sampler.startTime, 1 ) != 0:
			return False
		return True

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		ret = self.sampler.add_sample( logInfo.recvdTime, 1 )
		if ret != 0:
			return False
		return True

	def anly_outut_value( self, vtime, value ):
		if self.sampler is None:
			startTime = self.get_sample_start_time( None )
			endTime = self.get_sample_end_time( None )
			if startTime <= 0:
				startTime = int(vtime)
			sargs = SamplerArgs( startTime, endTime, self.pace,
				self.bufTime, NUM_THRES, XactRateAnalyser.__flush_callback, self )
			self.sampler = Sampler( sargs )
		
		ret = self.sampler.add_sample( vtime, value )
		
		return ret

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo)
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, XactRateAnalyser.__flush_callback, self )
		self.sampler = Sampler( sargs )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		pace = sampler.pace
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		if not time_offset:
			toAdd = 0
		bufio = StringIO()
		logging.info( 'flush data,'+str(self) ) 

		minTime = self.align_time_to_pace( sampler.minTime )
		maxTime = sampler.maxTime
		if pace < 0:
			minTime = maxTime = -1

		self.output_head()
		for value in blist:
			if curTime < minTime:
				curTime += pace
				continue
			if maxTime > 0 and curTime > maxTime:
				break

			self.__write_line( bufio, value, curTime, toAdd, pace )
			curTime += pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def __write_line( self, bufio, value, time, toAdd, pace ):
		sampleTime = time + toAdd
		tstr = str_seconds( sampleTime )
		bufio.write( tstr )
		bufio.write( ';' )
		if pace > 0:
			value /= float(pace)
		value = int( value + 0.5 )
		bufio.write( str(value) )
		bufio.write( '\n' )

	def decode_output_value( self, vstr ):
		segs = vstr.split( ';' )
		seconds = seconds_str( segs[0] )
		pace = self.pace
		if pace < 0:
			pace = 1
		value = float( segs[1] ) * pace

		return (seconds, value)

	def on_close( self ):
		logging.info( 'close'+str(self.__class__)+str(self) )
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None


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

	def anly_outut_value( self, vtime, value ):
		if self.sampler is None:
			startTime = self.get_sample_start_time( None )
			endTime = self.get_sample_end_time( None )
			if startTime <= 0:
				startTime = int(vtime)
			sargs = SamplerArgs( startTime, endTime, self.pace,
				self.bufTime, NUM_THRES, DescAnalyser.__flush_callback, self )
			self.sampler = MutableSampler( sargs, DescAnalyser.__init_value, DescAnalyser.__update_value )
		
		ret = self.sampler.add_sample( vtime, value )
		
		return ret

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
		pace = sampler.pace
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		if not time_offset:
			toAdd = 0
		bufio = StringIO()
		logging.info( 'flush data,'+str(self) )

		minTime = self.align_time_to_pace( sampler.minTime )
		maxTime = sampler.maxTime
		if pace < 0:
			minTime = maxTime = -1

		self.output_head()
		for item in blist:
			if curTime < minTime:
				curTime += pace
				continue
			if maxTime > 0 and curTime > maxTime:
				break

			if len(item) != 0:
				sampleTime = curTime + toAdd
				tstr = str_seconds( sampleTime )
				bufio.write( tstr )
				bufio.write( ';' )
				bufio.write( str(item) )
				bufio.write( '\n' )
			curTime += pace
		logs = bufio.getvalue()
		self.fout.write( logs )

	def decode_output_value( self, vstr ):
		segs = vstr.split( ';' )
		seconds = seconds_str( segs[0] )
		pace = self.pace
		if pace < 0:
			pace = 1
		value = segs[1]

		return (seconds, value)

	def on_close( self ):
		logging.info( 'close'+str(self.__class__)+str(self) )
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None

class FilterAnalyser( Analyser ):
	def __init__( self, config ):
		super( FilterAnalyser, self ).__init__( config )

	def anly_pace( self, logInfo ):
		print logInfo.originLine

	def on_close( self ):
		pass


class SingleAnalyser( Analyser ):
	def __init__( self, config, helper ):
		super( SingleAnalyser, self ).__init__( config )
		self.sampler = None
		self.__helper = helper
		self.firstFlush = True

	def set_one_process_mode( self, mode ):
		self.oneProcessMode = mode
		self.__helper.set_one_process_mode( mode )

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

		sampleTime = self.__helper.get_sample_time( logInfo )
		value = self.__helper.get_value( logInfo )

		if self.__helper.isSingleType:
			ret = self.sampler.add_sample( sampleTime, value )
		else:
			sampleValList = value
			for (avalue, num) in sampleValList:
				if num < 2:
					ret = self.sampler.add_sample( sampleTime, avalue )
				else:
					ret = self.sampler.add_samples( sampleTime, avalue, num )
					if ret > 0:		#need to flash the buffer to the file
						ctime = sampleTime
						logging.warn( '>>'+'******************need to try again*****************'+str(ret) )
						while ret > 0:
							ctime += ret * self.sampler.pace
							num -= ret
							ret = self.sampler.add_samples( ctime, avalue, num )

		if ret != 0:
			logging.debug( 'add sample failed, log time:'+str_seconds(logInfo.recvdTime)+\
					str(sampleTime)+'ret:'+str(ret)+str(self.sampler)+str(self.__helper.isSingleType) )
			return False
		return True

	def anly_outut_value( self, vtime, valList ):
		if self.sampler is None:
			startTime = self.get_sample_start_time( None )
			endTime = self.get_sample_end_time( None )
			if startTime <= 0:
				startTime = int(vtime)
			sargs = SamplerArgs( startTime, endTime, self.pace,
				self.bufTime, self.__helper.sampleThres, SingleAnalyser.__flush_callback, self )
			self.sampler = MutableSampler( sargs, SingleAnalyser.__init_value, SingleAnalyser.__update_value )

		ret = 0
		for value in valList:
			ret = self.sampler.add_sample( vtime, value )

		return ret

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		bufTime = self.__helper.get_buf_time()
		sargs = SamplerArgs( startTime, endTime, self.pace,
				bufTime, self.__helper.sampleThres, SingleAnalyser.__flush_callback, self )
		self.sampler = MutableSampler( sargs, SingleAnalyser.__init_value, SingleAnalyser.__update_value )

	def __init_value( self, value ):
		return self.__helper.init_value( value )
	
	def __update_value( self, oldValue, sampleValue ):
		return self.__helper.update_value( oldValue, sampleValue )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		pace = sampler.pace
		if sampler.pace > 0:
			toAdd = sampler.pace / 2
		if not time_offset:
			toAdd = 0
		bufio = StringIO()
		logging.info( 'flush data,'+str(self) )

		split = self.__helper.get_split()
		minTime = self.align_time_to_pace( sampler.minTime )
		maxTime = sampler.maxTime
		if pace < 0:
			minTime = maxTime = -1
		
		self.output_head()
		lastTime = -1
		for item in blist:
			if curTime < minTime:
				curTime += pace
				continue
			if maxTime > 0 and curTime > maxTime:
				break

			if not self.__helper.exclude_value( item ):
				vstr = self.__helper.str_value( item )
				if vstr is not None:
					if split is not None and not self.__helper.useInterStr:
						if self.fileStartTime < 0:
							self.fileStartTime = curTime
						sampleTime = curTime + toAdd
						tstr = str_seconds( sampleTime )
						bufio.write( tstr )
						bufio.write( split )
						logging.debug( tstr+vstr+'toAdd:'+str(toAdd)+'pace:'+str(pace) )
					bufio.write( vstr )
					bufio.write( '\n' )
					if bufio.len > 10485760:	#10Mbytes
						logs = bufio.getvalue()
						self.fout.write( logs )
						bufio.close()
						bufio = StringIO()
					lastTime = curTime
			curTime += pace

		if split is not None and not self.__helper.useInterStr:
			if lastTime > 0:
				self.fileEndTime = lastTime

		logs = bufio.getvalue()
		self.fout.write( logs )
		bufio.close()

	def prepare_output( self ):
		self.__helper.prepare_dump()

	def decode_output_head( self, hstr ):
		split = self.__helper.get_split()
		offset = 0
		if not self.__helper.useInterStr:
			#must check if there is head
			prefix = 'time' + split
			if not hstr.startswith(prefix):
				return True
			#skip the 'time' part
			offset = hstr.find( split ) + 1
		else:
			#the internal helper may use the parser to decoding
			self.__helper.parser = self.parser

		self.__helper.head_str( hstr, offset, split )

		return True

	def encode_output_head( self ):
 		vh = self.__helper.str_head()
		if vh is None:
			return None

		split = self.__helper.get_split()
		head = 'time' + split + vh
		return head

	def decode_output_value( self, vstr ):
		split = self.__helper.get_split()
		offset = 0
		if not self.__helper.useInterStr:
			offset = vstr.find( split )
			tstr = vstr[ 0:offset ]
			vtime = seconds_str( tstr )
			offset += 1
			(valList, offset) = self.__helper.value_str( vstr, offset, split )
			return (vtime, valList)

		self.__helper.parser = self.parser
		(vtime, valList, offset) = self.__helper.value_str( vstr, offset, split )
		return (vtime, valList)

	def on_close( self ):
		logging.info( 'close'+str(self.__class__)+str(self) )
		if self.sampler is not None:
			self.sampler.flush()
			self.__helper.on_close()
			self.sampler = None


class ActiveSessionsAnalyser( Analyser ):
	def __init__( self, config ):
		super( ActiveSessionsAnalyser, self ).__init__( config )
		self.totalSent = 0
		self.sampler = None
		self.hasWritten = False
		self.zeroValueCount = 0
		self.zeroStartTime = 0
		self.hasNoneZero = False

	def anly_zero_pace( self, logInfo ):
		return True

	def anly_negative_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		if self.sampler.add_sample( logInfo.recvdTime, 1 ) != 0:
			return False
		return True

	def anly_pace( self, logInfo ):
		if self.sampler is None:
			self.__create_sampler( logInfo )
		servTime = logInfo.servTime / 1000000.0
		if servTime == 0:
			servTime = 1
		num = servTime / self.sampler.pace
		diff = num - int(num)	#in case it's a integer
		if diff > 0:
			num = int( num+1 )
		else:
			num = int( num )
		value = 1
		ret = self.sampler.add_samples( logInfo.recvdTime, value, num )
		if ret < 0:
			if self.hasWritten:
				logging.debug( 'old log'+str(logInfo) )
			return False	#TODO
		elif ret > 0:		#need to flash the buffer to the file
			ctime = logInfo.recvdTime
			while ret > 0:
				ctime += ret * self.sampler.pace
				num -= ret
				ret = self.sampler.add_samples( ctime, value, num )
		return True

	def anly_outut_value( self, vtime, value ):
		if self.sampler is None:
			startTime = self.get_sample_start_time( None )
			endTime = self.get_sample_end_time( None )
			if startTime <= 0:
				startTime = int(vtime)
			sargs = SamplerArgs( startTime, endTime, self.pace,
				self.bufTime, NUM_THRES, ActiveSessionsAnalyser.__flush_callback, self )
			self.sampler = Sampler( sargs )
		
		ret = self.sampler.add_sample( vtime, value )
		
		return ret

	def __create_sampler( self, logInfo ):
		startTime = self.get_sample_start_time( logInfo )
		endTime = self.get_sample_end_time( logInfo )
		sargs = SamplerArgs( startTime, endTime, self.pace,
				BUF_TIME, NUM_THRES, ActiveSessionsAnalyser.__flush_callback, self )
		self.sampler = Sampler( sargs )

	def __flush_callback( self, sampler, blist ):
		curTime = sampler.startTime
		toAdd = 0
		pace = sampler.pace
		if pace > 0:
			toAdd = pace / 2
		if not time_offset:
			toAdd = 0
		bufio = StringIO()
		logging.info( 'flush data,'+str(self) )

		minTime = self.align_time_to_pace( sampler.minTime )
		maxTime = sampler.maxTime
		if pace < 0:
			minTime = maxTime = -1

		self.output_head()
		for value in blist:
			if curTime < minTime:
				curTime += pace
				continue
			if maxTime > 0 and curTime > maxTime:
				break

			self.__write_line( bufio, value, curTime, toAdd, pace )
			curTime += pace
		ostr = bufio.getvalue()
		self.fout.write( ostr )
		self.hasWritten = True

	def __write_line( self, bufio, value, curTime, toAdd, pace ):
		dtime = to_datetime( curTime + toAdd )
		tstr = str_time( dtime )
		bufio.write( tstr )
		bufio.write( ';' )
		bufio.write( str(value) )
		bufio.write( '\n' )

	def decode_output_value( self, vstr ):
		segs = vstr.split( ';' )
		seconds = seconds_str( segs[0] )
		pace = self.pace
		if pace < 0:
			pace = 1
		value = int( segs[1] )

		return (seconds, value)

	def on_close( self ):
		logging.info( 'close'+str(self.__class__)+str(self) )
		if self.sampler is not None:
			self.sampler.flush()
			self.sampler = None

