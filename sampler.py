#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

import logging
import sys

from base import *

class SamplerArgs( object ):
	def __init__( self, startTime, endTime, pace, bufTime, numThres, flush_cb, obj ):
		self.startTime = startTime
		self.endTime = endTime
		self.pace = pace
		self.bufTime = bufTime
		self.numThres = numThres
		self.flush_cb = flush_cb
		self.cbobj = obj

	def __str__( self ):
		return str( self.__dict__ )

class Sampler( object ):
	def __init__( self, args ):
		if args.pace == 0:
			raise Exception( 'sampler not support zero pace' )
		logging.info( 'create sampler with args:'+str(args) )
		self.slist1 = None
		self.slist2 = None
		self.reset( args )

		self.flush_cb = args.flush_cb
		self.cbobj = args.cbobj
		num = args.bufTime / args.pace
		if num > args.numThres:
			num = args.numThres
		elif num < 1:
			num = 1
		self.__total = num * 2
		self.__buf1 = [0] * num
		self.__buf2 = [0] * num
		self.slist1 = self.__buf1
		self.slist2 = self.__buf2

	def reset( self, args ):
		self.args = args
		if args.startTime < 0:
			args.startTime = 0
		self.startTime = int(args.startTime)
		logging.info( 'reset sample, start time:'+str_seconds(self.startTime) )
		self.orgStartTime = int(args.startTime)
		self.pace = args.pace
		self.endTime = int(args.endTime)
		if self.endTime > 0 and self.endTime < self.startTime:
			raise Exception( 'endTime < startTime error' )
		self.maxTime = -1
		self.minTime = -1
		self.totalCount1 = 0	#for the first list
		self.totalCount2 = 0	#for the second list
		self.tailIdx = -1
		if self.endTime > 0 and self.pace > 0:
			self.tailIdx = int( (self.endTime- self.startTime) / self.pace ) + 1

		#check if needs to init the list
		if self.slist1 is not None:
			self.__init_list( self.slist1 )
		if self.slist2 is not None:
			self.__init_list( self.slist2 )


	def __stat_cur_time( self, stime, etime ):
		if etime > self.maxTime:
			self.maxTime = etime
		if self.minTime <= 0 or stime < self.minTime:
			self.minTime = stime

	#@return:
	#	-1: failed
	#	 0: success
	#	 1: failed but full
	def add_sample( self, time, value ):
		time = int(time)
		if time < self.orgStartTime:
			return 0			#should ignore all samples earlier than origion start time

		if self.endTime > 0 and time > self.endTime:
			return -1
		idx = 0
		if self.pace > 0:
			idx = int( (time - self.startTime) / self.pace )
		if idx < 0:
			return -1
		ret = self.__add_sample( idx, value )
		if ret == 1:
			self.__flush_buffer()
			#try again
			ret = self.__add_sample( idx, value )
		if ret == 0:
			self.__stat_cur_time( time, time )
		return ret

	def add_samples( self, startTime, value, num ):
		startTime = int(startTime)
		etime = startTime + (num-1)*self.pace
		if etime < self.orgStartTime:
			return 0			#should ignore all samples earlier than origion start time

		idx = 0
		if self.pace > 0:
			idx = int( (startTime - self.startTime) / self.pace )
		tail = num + idx
		if self.tailIdx > 0 and tail > self.tailIdx:
			return -1
		if idx < 0:
			if tail < 0:
				return -1
			idx = 0
		count = 0
		while idx < tail:
			if self.__add_sample( idx, value ) == 1:
				self.__flush_buffer()
				#try again
				if self.__add_sample( idx, value ) != 0:
					return count
			idx += 1
			count += 1

		self.__stat_cur_time( startTime, etime )
		return 0

	def flush( self ):
		if self.is_empty():
			return

		self.__flush_buffer()
		self.__flush_buffer()

	def __add_sample( self, idx, value ):
		logging.debug( 'add sample,idx:'+str(idx)+',tailIdx:'+str(self.tailIdx)+',value:'+str(value) )
		if idx >= self.__total:
			return 1

		size = self.__total / 2
		if idx >= size:
			idx -= size
			self.slist2[idx] = self.slist2[idx] + value
			self.totalCount2 += 1
		else:
			self.slist1[idx] = self.slist1[idx] + value
			self.totalCount1 += 1
		return 0

	def size( self ):
		return self.totalCount1 + self.totalCount2

	def is_empty( self ):
		return self.size() == 0

	def __flush_buffer( self ):
		logging.info( 'minTime:'+str_seconds(self.minTime)+',maxTime:'+str_seconds(self.maxTime)+ \
				',totalCount1:'+str(self.totalCount1)+',totalCount2:'+str(self.totalCount2)+\
				',list len:'+str(len(self.slist1)) )
		curList = self.slist1
		#must call before change the status of the sampler
		if self.totalCount1 > 0:
			self.flush_cb( self.cbobj, self, curList )
		else:
			logging.info( 'the curList is empty, no need to flush' )
		if self.pace > 0:
			self.startTime += self.pace * self.__total / 2
		self.slist1 = self.slist2
		self.slist2 = curList
		self.totalCount1 = self.totalCount2
		self.totalCount2 = 0
		self.__clear_buffer( curList )

	def __clear_buffer( self, blist ):
		self.__init_list( blist )

	def __init_list( self, blist ):
		idx = 0
		size = len(blist)
		while idx < size:
			blist[idx] = 0
			idx += 1


class MutableSampler( BaseObject ):
	def __init__( self, args, init_value, update_value ):
		if args.pace == 0:
			raise Exception( 'sampler not support zero pace' )
		logging.info( 'create sampler, with args:'+str(args) )
		self.slist1 = None
		self.slist2 = None
		self.reset( args )

		self.flush_cb = args.flush_cb
		self.cbobj = args.cbobj
		self.init_value = init_value
		self.update_value = update_value
		num = args.bufTime / args.pace
		if num > args.numThres:
			num = args.numThres
		elif args.pace < 0:
			num = 1
		else:
			num = 1000
		self.__total = num * 2
		self.__buf1 = [0] * num
		self.__buf2 = [0] * num
		self.slist1 = self.__buf1
		self.slist2 = self.__buf2
		self.__init_list( self.slist1, True )
		self.__init_list( self.slist2, True )


	def reset( self, args ):
		self.args = args
		if args.startTime < 0:
			args.startTime = 0
		self.startTime = int(args.startTime)
		self.orgStartTime = int(args.startTime)
		self.pace = args.pace
		self.endTime = int(args.endTime)
		if self.endTime > 0 and self.endTime < self.startTime:
			raise Exception( 'endTime < startTime error' )
		self.tailIdx = -1
		if self.endTime > 0 and self.pace > 0:
			self.tailIdx = int( (self.endTime- self.startTime) / self.pace ) + 1
		self.maxTime = -1
		self.minTime = -1
		self.totalCount1 = 0	#for the first list
		self.totalCount2 = 0	#for the second list

		#check if needs to init the list
		if self.slist1 is not None:
			self.__init_list( self.slist1 )
		if self.slist2 is not None:
			self.__init_list( self.slist2 )

	def __stat_cur_time( self, stime, etime ):
		if etime > self.maxTime:
			self.maxTime = etime
		if self.minTime <= 0 or stime < self.minTime:
			self.minTime = stime

	#@return:
	#	-1: failed
	#	 0: success
	#	 1: failed but full
	def add_sample( self, time, value ):
		time = int(time)
		if time < self.orgStartTime:
			return 0			#should ignore all samples earlier than origion start time

		if self.endTime > 0 and time > self.endTime:
			return -1
		idx = 0
		if self.pace > 0:
			idx = int( (time - self.startTime) / self.pace )
		if idx < 0:
			logging.debug( 'old time:'+str_seconds(time)+',startTime:'+str_seconds(self.startTime) )
			return -1
		#if idx is out of the buffer, reset the buffer to the current sample
		if idx > self.__total*2:
			logging.info( 'out of buffer, idx:'+str(idx)+',tail:'+str(self.__total) )
			self.flush()
			self.startTime = time
			idx = 0
		ret = self.__add_sample( idx, value )
		if ret == 1:
			self.__flush_buffer()
			#try again
			ret = self.add_sample( time, value )
		if ret == 1:
			logging.error( 'failed to add sample, idx:'+str(idx)+',total:'+str(self.__total) )
		elif ret == 0:
			self.__stat_cur_time( time, time )
		return ret

	def add_samples( self, startTime, value, num ):
		startTime = int(startTime)
		etime = startTime + (num-1)*self.pace
		if etime < self.orgStartTime:
			return 0			#should ignore all samples earlier than origion start time

		idx = 0
		if self.pace > 0:
			idx = int( (startTime - self.startTime) / self.pace )
		tail = num + idx
		if self.tailIdx > 0 and tail > self.tailIdx:
			return -1
		if idx < 0:
			if tail < 0:
				return -1
			idx = 0
		count = 0
		while idx < tail:
			if self.__add_sample( idx, value ) == 1:
				self.__flush_buffer()
				#try again
				if self.__add_sample( idx, value ) != 0:
					return count
			idx += 1
			count += 1
		self.__stat_cur_time( startTime, etime )
		return 0

	def flush( self ):
		if self.is_empty():
			return 

		self.__flush_buffer()
		self.__flush_buffer()

	def __add_sample( self, idx, value ):
		logging.debug( 'add sample,idx:'+str(idx)+',tailIdx:'+str(self.tailIdx)+',value:'+str(value) )
		if idx >= self.__total:
			return 1
		size = self.__total / 2
		if idx >= size:
			idx -= size
			oldValue = self.slist2[idx]
			newValue = self.update_value( self.cbobj, oldValue, value )
			self.slist2[idx] = newValue
			self.totalCount2 += 1
		else:
			oldValue = self.slist1[idx]
			newValue = self.update_value( self.cbobj, oldValue, value )
			self.slist1[idx] = newValue
			self.totalCount1 += 1
		return 0

	def size( self ):
		return self.totalCount1 + self.totalCount2

	def is_empty( self ):
		return self.size() == 0

	def __flush_buffer( self ):
		logging.info( 'minTime:'+str_seconds(self.minTime)+',maxTime:'+str_seconds(self.maxTime)+ \
				',totalCount1:'+str(self.totalCount1)+',totalCount2:'+str(self.totalCount2)+\
				',list len:'+str(len(self.slist1)) )
		curList = self.slist1
		#must call before change the status of the sampler
		if self.totalCount1 > 0:
			self.flush_cb( self.cbobj, self, curList )
		else:
			logging.info( 'the curList is empty, no need to flush' )
		if self.pace > 0:
			self.startTime += self.pace * self.__total / 2
		self.slist1 = self.slist2
		self.slist2 = curList
		self.totalCount1 = self.totalCount2
		self.totalCount2 = 0
		self.__clear_buffer( curList )

	def __clear_buffer( self, blist ):
		self.__init_list( blist )

	def __init_list( self, blist, create = False ):
		idx = 0
		size = len(blist)
		while idx < size:
			value = blist[idx]
			if create:
				value = self.init_value( self.cbobj, None )
			else:
				value = self.init_value( self.cbobj, value )
			blist[idx] = value
			idx += 1

	def __str__( self ):
		ss = 'startTime:' + str(self.startTime) + '->'+str_seconds(self.startTime)+'\t'
		ss += 'endTime:' + str(self.endTime) + '->'+str_seconds(self.endTime)+'\t'
		ss += 'pace:' + str(self.pace)
		return ss


