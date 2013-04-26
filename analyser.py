from base import *
from datetime import timedelta
import os

class Analyser:
	def __init__( self, startTime, endTime, pace ):
		self.curTime = startTime
		self.pace = pace
		self.servTime = 1
		self.__lastTime = startTime
		self.sampleTime = startTime

	#@return:
	#	-1: failed
	#	 0: success but not finish one pace
	#	 1: success and finish one pace
	def __do_pace( self, logInfo ):
		if self.pace == 0:
			self.curTime = logInfo.rtime
			self.servTime = logInfo.stime
			self.sampleTime = self.servTime
			return 1
		elif self.pace > 0:
			lastTime = self.__lastTime
			self.__lastTime = logInfo.rtime
			if self.curTime+self.pace < logInfo.rtime:
				self.servTime = delta_time( self.curTime, lastTime )
				self.sampleTime = self.curTime + timedelta( 0, self.servTime / 2 )
				self.curTime = logInfo.rtime
				return 1
			return 0
		return -1

	def analyse_log( self, logInfo ):
		raise 'derived must implement analyse_log virtual function'

	def close( self ):
		raise 'derived must implement close virtual function'

class BandwidthAnalyser( Analyser ):
	def __init__( self, fout, startTime, endTime, pace ):
		super( BandwidthAnalyser, self ).__init__( startTime, endTime, pace )
		self.fout = fout
		self.totalSent = 0

	def analyse_log( self, logInfo ):
		res = super(BandwidthAnalyser, self).__do_pace( logInfo )
		self.totalSent += logInfo.allSent
		if res == 1:
			band = self.totalSent * 1000000.0 / self.servTime
			log = str_time( self.sampleTime ) + '\t' + str( band ) + '\n'
			self.fout.write( log )
			self.totalSent = 0
		elif res < 0:
			self.totalSent = 0
			return False
		return True

	def close( self ):
		self.fout.close()


class AnalyserFactory:
	def __init__( self ):
		pass

	def create_from_args( args, startTime, endTime ):
		analysers = list()
		outdir = os.path.join( args.path, 'output' )
		mkdir( outdir )
		num = 0
		if args.analyseType == 0:		#bandwidth 
			path = os.path.join( outdir, 'bandwidth_' + str(num) )
			anly = BandwidthAnalyser( fout, startTime, endTime, args.pace )
			analysers.append( anly )
		return analysers;









