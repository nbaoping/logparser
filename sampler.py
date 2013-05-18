#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

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
		print 'create sampler', args
		if args.startTime < 0:
			args.startTime = 0
		self.startTime = args.startTime
		self.pace = args.pace
		self.endTime = args.endTime
		if self.endTime > 0 and self.endTime < self.startTime:
			raise Exception( 'endTime < startTime error' )
		self.tailIdx = -1
		if self.endTime > 0 and self.pace > 0:
			self.tailIdx = int( (self.endTime- self.startTime) / self.pace ) + 1

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

	#@return:
	#	-1: failed
	#	 0: success
	#	 1: failed but full
	def add_sample( self, time, value ):
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
		return ret

	def add_samples( self, startTime, value, num ):
		#print 'add samples', startTime, value, num
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
		return 0

	def flush( self ):
		self.__flush_buffer()
		self.__flush_buffer()

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

	def __flush_buffer( self ):
		curList = self.slist1
		#must call before change the status of the sampler
		self.flush_cb( self.cbobj, self, curList )
		if self.pace > 0:
			self.startTime += self.pace * self.__total / 2
		self.slist1 = self.slist2
		self.slist2 = curList
		self.__clear_buffer( curList )

	def __clear_buffer( self, blist ):
		self.__init_list( blist )

	def __init_list( self, blist ):
		idx = 0
		size = len(blist)
		while idx < size:
			blist[idx] = 0
			idx += 1


class MutableSampler( object ):
	def __init__( self, args, init_value, update_value ):
		if args.pace == 0:
			raise Exception( 'sampler not support zero pace' )
		print 'create sampler', args
		if args.startTime < 0:
			args.startTime = 0
		self.startTime = args.startTime
		self.pace = args.pace
		self.endTime = args.endTime
		if self.endTime > 0 and self.endTime < self.startTime:
			raise Exception( 'endTime < startTime error' )
		self.tailIdx = -1
		if self.endTime > 0 and self.pace > 0:
			self.tailIdx = int( (self.endTime- self.startTime) / self.pace ) + 1

		self.flush_cb = args.flush_cb
		self.cbobj = args.cbobj
		self.init_value = init_value
		self.update_value = update_value
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
		self.__init_list( self.slist1, True )
		self.__init_list( self.slist2, True )

	#@return:
	#	-1: failed
	#	 0: success
	#	 1: failed but full
	def add_sample( self, time, value ):
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
		return ret

	def add_samples( self, startTime, value, num ):
		#print 'add samples', startTime, value, num
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
		return 0

	def flush( self ):
		self.__flush_buffer()
		self.__flush_buffer()

	def __add_sample( self, idx, value ):
		#print 'add sample', idx, value
		if idx >= self.__total:
			return 1
		size = self.__total / 2
		if idx >= size:
			idx -= size
			oldValue = self.slist2[idx]
			newValue = self.update_value( self.cbobj, oldValue, value )
			self.slist2[idx] = newValue
		else:
			oldValue = self.slist1[idx]
			newValue = self.update_value( self.cbobj, oldValue, value )
			self.slist1[idx] = newValue
		return 0

	def __flush_buffer( self ):
		curList = self.slist1
		#must call before change the status of the sampler
		self.flush_cb( self.cbobj, self, curList )
		if self.pace > 0:
			self.startTime += self.pace * self.__total / 2
		self.slist1 = self.slist2
		self.slist2 = curList
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
