from base import *
from datetime import timedelta
import os
from cStringIO import StringIO

BUF_TIME = 36000		#in seconds
NUM_THRES = 36000

class Sampler( object ):
	def __init__( self, startTime, pace, bufTime, numThres ):
		if pace == 0:
			raise Exception( 'sampler not support zero pace' )
		print 'create sample bufTime', bufTime, 'numThres', numThres, 'pace', pace
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
		self.curTime = startTime
		self.pace = pace
		self.paceTime = self.curTime + self.pace
		self.servTime = 1
		self.__lastTime = startTime
		self.sampleTime = startTime

	#@return:
	#	-1: failed
	#	 0: success but not finish one pace
	#	 1: success and finish one pace
	def do_pace( self, logInfo ):
		if self.pace == 0:
			self.curTime = logInfo.rtime
			self.servTime = logInfo.stime / 1000000.0		#to second
			self.sampleTime = self.curTime
			return 1
		elif self.pace > 0:
			lastTime = self.__lastTime
			self.__lastTime = logInfo.rtime
			curTime = logInfo.rtime
			if self.paceTime < curTime:
				#self.servTime = delta_time( self.curTime, lastTime )
				self.servTime = self.pace
				self.sampleTime = self.curTime + self.servTime / 2
				self.curTime = logInfo.rtime
				self.paceTime = self.curTime + self.pace
				if self.servTime == 0:
					print 'zero serv time cur pace>>', self.curTime, self.servTime, self.sampleTime, self.__lastTime
					self.servTime = self.pace
				return 1
			return 0
		return -1

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
		res = super(BandwidthAnalyser, self).do_pace( logInfo )
		self.totalSent += logInfo.allSent
		if res == 1:
			#print 'total sent:', self.totalSent, 'serv time:', self.servTime
			band = self.totalSent * 8.0 / self.servTime / 1024 / 1024
			band = round( band, 3 )
			dtime = to_datetime( self.sampleTime )
			log = str_time( dtime ) + '\t' + str( band ) + '\t' + str(self.servTime) + '\t' + str(self.totalSent) + '\n'
			self.fout.write( log )
			self.totalSent = 0
		elif res < 0:
			self.totalSent = 0
			return False
		return True

	def __anly_pace( self, logInfo ):
		if self.sampler is None:
			self.sampler = Sampler( self.curTime - BUF_TIME, self.pace, BUF_TIME, NUM_THRES )
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


class AnalyserFactory:
	def __init__( self ):
		pass

	def create_from_args( self, args, startTime, endTime ):
		analysers = list()
		outdir = os.path.join( args.path, RES_DIR )
		mkdir( outdir )
		num = 0
		if args.analyseType == 0:		#bandwidth 
			path = os.path.join( outdir, 'bandwidth_' + str(args.pace) + '_' + str(num) )
			anly = BandwidthAnalyser( path, startTime, endTime, args.pace )
			analysers.append( anly )
		return analysers;









