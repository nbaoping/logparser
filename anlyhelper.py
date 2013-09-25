from base import *

BUF_TIME = 36000		#in seconds
NUM_THRES = 36000

class AnalyserHelper( BaseObject ):
	def __init__( self, isSingleType = True ):
		self.isSingleType = isSingleType
		self.sampleThres = NUM_THRES
		self.useInterStr = False
		self.sorted = False
		self.insertValue = None

	def get_buf_time( self ):
		if self.sorted:
			return 600
		return BUF_TIME

	def get_sample_time( self, logInfo ):
		return logInfo.recvdTime

	#return the statistics value
	def get_value( self, logInfo ):
		raise_virtual( func_name() )

	def init_value( self, value ):
		raise_virtual( func_name() )
	
	def update_value( self, oldValue, sampleValue ):
		raise_virtual( func_name() )

	def exclude_value( self, value ):
		raise_virtual( func_name() )

	def head_str( self ):
		return None

	def str_value( self, value ):
		raise_virtual( func_name() )

	def get_split( self ):
		return ';'

	def on_close( self ):
		pass

	def get_multiple_value( self, helperList, logInfo ):
		(svalList, mvalList) = self.__get_value_list( helperList, logInfo )
		sampleValList = list()
		sampleValList.append( (svalList, 1) )
		size = len(svalList)

		for (idx, item) in mvalList:
			(avalue, num) = item
			nlist = [None] * size
			nlist[idx] = avalue
			sampleValList.append( (nlist, num) )

		return sampleValList


	def __get_value_list( self, helperList, logInfo ):
		svalList = list()
		mvalList = list()
		idx = 0
		for helper in helperList:
			val = helper.get_value( logInfo )
			if helper.isSingleType:
				svalList.append( val )
			else:
				#is multipe value type
				hasSingle = False
				tupleList = val 
				for (avalue, num) in tupleList:
					if num < 2:
						if not hasSingle:
							hasSingle = True
							svalList.append( avalue )
						else:
							#normally only one sigle avalue can be received
							print 'error, only one sigle value allowed, avalue:', avalue
					else:
						mvalList.append( (idx, (avalue, num)) )

				if not hasSingle:
					svalList.append( None )

			idx += 1

		return ( svalList, mvalList )



class RawOutputHelper( AnalyserHelper ):
	def __init__( self, ocfgList ):
		super(RawOutputHelper, self).__init__()
		self.useInterStr = True
		self.fmtNameList = list()
		for ocfg in ocfgList:
			self.fmtNameList.append( ocfg.fmtName )

	def get_value( self, logInfo ):
		vlist = list()
		for fmtName in self.fmtNameList:
			value = logInfo.get_member( fmtName )
			vlist.append( value )
		return (logInfo.recvdTime, vlist)

	def init_value( self, value ):
		return list()
	
	def update_value( self, oldValue, sampleValue ):
		oldValue.append( sampleValue )
		return oldValue

	def head_str( self ):
		split = self.get_split()
		num = 0
		hstr = ''
		for fmtName in self.fmtNameList:
			num += 1
			if num > 1:
				hstr += split
			hstr += fmtName
		return hstr

	def exclude_value( self, value ):
		return len(value) == 0

	def str_value( self, value ):
		if value is None:
			return ''

		split = self.get_split()
		vlist = value
		bufio = StringIO()
		if not self.sorted:
			vlist = sorted( value, key=itemgetter(0) )

		num = 0
		for item in vlist:
			num += 1
			sampleTime = item[0]
			tstr = str_seconds( sampleTime )
			mstr = str( int((sampleTime*1000))%1000 )
			if num > 1:
				bufio.write( '\n' )
			bufio.write( tstr )
			bufio.write( '.' )
			bufio.write( mstr )
			bufio.write( split )
			self.__write_fmt_list( bufio, item[1], split )
		ss = bufio.getvalue()
		bufio.close()
		return ss

	def __write_fmt_list( self, bufio, vlist, split ):
		num = 0
		for val in vlist:
			num += 1
			if num > 1:
				bufio.write( split )
			bufio.write( str(val) )



class OutTimeAverageHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutTimeAverageHelper, self).__init__( False )
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.pace = ocfg.pace
		self.unitrate = ocfg.unitrate
		self.insertValue = ocfg.insertValue
	
	def init_value( self, value ):
		return 0
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		servTime = logInfo.servTime / 1000000.0
		if servTime == 0:
			servTime = 1
		num = servTime / self.pace
		if num <= 1:
			avalue = value
		else:
			avalue = value / float(num)
		#in case, it's a integer
		num = int( num+0.99999 )
		return [(avalue, num)]

	def update_value( self, oldValue, sampleValue ):
		value = oldValue + sampleValue

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		value = round( value * self.unitrate, 3 )
		return str(value)


class OutTimeActiveHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutTimeActiveHelper, self).__init__( False )
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.pace = ocfg.pace
		self.unitrate = ocfg.unitrate
		self.insertValue = ocfg.insertValue
	
	def init_value( self, value ):
		return 0
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		servTime = value * self.unitrate
		if servTime == 0:
			servTime = 1
		num = servTime / self.pace
		#in case, it's a integer
		num = int( num+0.99999 )
		return [(1, num)]

	def update_value( self, oldValue, sampleValue ):
		value = oldValue + sampleValue

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		return str(value)


class OutCountHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutCountHelper, self).__init__()
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		return 1

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		value = oldValue + 1

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		return self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		return str(value)


class OutSumHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutSumHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return value

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		value = oldValue + sampleValue

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		return str(value)


class OutMaxHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutMaxHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return value

	def init_value( self, value ):
		return 'max'
	
	def update_value( self, oldValue, sampleValue ):
		value = oldValue
		if oldValue == 'max':
			value = sampleValue
		elif sampleValue > oldValue:
			value = sampleValue

		return value

	def exclude_value( self, value ):
		if self.insertValue is None:
			#need to exclude the one that is not sampled
			if value == 'max':
				return True

		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		if value == 'max':
			if self.insertValue is not None:
				return self.insertValue
			return '0'
		return str(value)


class OutMinHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutMinHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return value

	def init_value( self, value ):
		return 'min'
	
	def update_value( self, oldValue, sampleValue ):
		value = oldValue
		if oldValue == 'min':
			value = sampleValue
		elif sampleValue < oldValue:
			value = sampleValue

		return value

	def exclude_value( self, value ):
		if self.insertValue is None:
			#need to exclude the one that is not sampled
			if value == 'min':
				return True

		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		if value == 'min':
			if self.insertValue is not None:
				return self.insertValue
			return '0'
		return str(value)


class OutAverageHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutAverageHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return value

	def init_value( self, value ):
		return (0, 0)
	
	def update_value( self, oldValue, sampleValue ):
		total = oldValue[0] + sampleValue
		count = oldValue[1] + 1
		value = (total, count)

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		count = value[1]
		total = value[0]
		if count == 0:
			return '0'
		avg = round(total * 1.0 / count, 3)
		return str(avg)


class OutAmountHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutAmountHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return value

	def init_value( self, value ):
		return dict()
	
	def update_value( self, oldValue, sampleValue ):
		value = oldValue
		if sampleValue in oldValue:
			oldValue[sampleValue] += 1
		else:
			oldValue[sampleValue] = 1

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		return str(len(value))


def create_base_helper( ocfg ):
	exptype = ocfg.exptype
	helper = None
	if exptype == 'sum':
		helper = OutSumHelper( ocfg )
	elif exptype == 'max':
		helper = OutMaxHelper( ocfg )
	elif exptype == 'min':
		helper = OutMinHelper( ocfg )
	elif exptype == 'average':
		helper = OutAverageHelper( ocfg )
	elif exptype == 'timeAverage':
		helper = OutTimeAverageHelper( ocfg )
	elif exptype == 'timeActive':
		helper = OutTimeActiveHelper( ocfg )
	elif exptype == 'count':
		helper = OutCountHelper( ocfg )
	elif exptype == 'amount':
		helper = OutAmountHelper( ocfg )

	return helper


class OutMapHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutMapHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
		self.keyMap = dict()
		self.split = ocfg.split
		self.helperList = None
		print '==================================\n'
		if ocfg.outList is not None:
			self.helperList = list()
			print ocfg.outList
			for subCfg in ocfg.outList:
				subCfg.pace = ocfg.pace
				helper = self.__create_output_helper( subCfg )
				self.helperList.append( helper )
				if not helper.isSingleType:
					self.isSingleType = False
	
	def __create_output_helper( self, ocfg ):
		exptype = ocfg.exptype
		print func_name(), ocfg
		helper = create_base_helper( ocfg )
		print helper

		return helper

	def init_value( self, value ):
		value = dict()
		return value
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		self.curKey = value
		
		if self.helperList is not None:
			if not self.isSingleType:
				valList = self.get_multiple_value( self.helperList, logInfo )
			else:
				valList = list()
				for helper in self.helperList:
					val = helper.get_value( logInfo )
					valList.append( val )

			value = valList

		return value

	def update_value( self, oldValue, sampleValue ):
		key = self.curKey
		value = oldValue
		if self.helperList is None:
			count = 1
			if key in oldValue:
				count += oldValue[key]
			else:
				self.keyMap[key] = 1
			oldValue[key] = count
		else:
			newValList = sampleValue
			if key not in oldValue:
				valList = list()
				idx = 0
				for helper in self.helperList:
					val = helper.init_value( newValList[idx] )
					valList.append( val )
					idx += 1
				oldValue[key] = valList
				self.keyMap[key] = 1

			oldValList = oldValue[key]
			idx = 0
			for helper in self.helperList:
				newValue = newValList[idx]
				if newValue is not None:
					val = helper.update_value( oldValList[idx], newValue )
					oldValList[idx] = val
				idx += 1

		return value

	def exclude_value( self, value ):
		return False

	def head_str( self ):
		if self.split:
			split = self.get_split()
			hstr = ''
			for key in self.keyMap.keys():
				if hstr != '':
					hstr += split
				if self.helperList is None:
					subHead = str(key)
				else:
					subHead = self.__get_outlist_head( key, split )

				hstr += subHead

			return hstr

		return self.fmtName + '_' + self.exptype

	def __get_outlist_head( self, key, split ):
		subHead = ''
		kstr = str(key)
		for helper in self.helperList:
			th = kstr + '_' + helper.head_str()
			if subHead != '':
				subHead += split
			subHead += th 

		return subHead

	def __get_outlist_value( self, valList, split ):
		vstr = ''
		idx = 0
		for helper in self.helperList:
			if idx > 0:
				vstr += split

			value = None
			if valList is not None:
				value = valList[idx]
			vstr += helper.str_value( value )
			idx += 1

		return vstr

	def str_value( self, value ):
		if not self.split:
			return str(value)
		else:
			idx = 0
			ss = ''
			split = self.get_split()
			keyList = self.keyMap.keys()
			for key in keyList:
				if idx > 0:
					ss += split
				if self.helperList is None:
					count = 0
					if key in value:
						count = value[key]
					ss += str(count)
				else:
					valList = None
					if key in value:
						valList = value[key]
					ss += self.__get_outlist_value( valList, split )
				idx += 1
			return ss


class OutputHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutputHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
		if self.exptype == 'map':
			self.keyMap = dict()
			self.split = ocfg.split
		elif self.exptype == 'raw':
			raise Exception( 'not support raw type in OutputsHelper' )
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return value

	def init_value( self, value ):
		exptype = self.exptype
		if exptype == 'sum':
			return 0
		elif exptype == 'max':
			return 'max'
		elif exptype == 'min':
			return 'min'
		elif exptype == 'average':
			return (0, 0)
		elif exptype == 'map':
			return dict()
		return list()
	
	def update_value( self, oldValue, sampleValue ):
		exptype = self.exptype
		value = oldValue
		if exptype == 'sum':
			value = oldValue + sampleValue
		elif exptype == 'max':
			if oldValue == 'max':
				value = sampleValue
			elif sampleValue > oldValue:
				value = sampleValue
		elif exptype == 'min':
			if oldValue == 'min':
				value = sampleValue
			elif sampleValue < oldValue:
				value = sampleValue
		elif exptype == 'average':
			total = oldValue[0] + sampleValue
			count = oldValue[1] + 1
			value = (total, count)
		elif exptype == 'map':
			count = 1
			if sampleValue in oldValue:
				count += oldValue[sampleValue]
			else:
				self.keyMap[sampleValue] = 1
			oldValue[sampleValue] = count
			value = oldValue
		else:
			oldValue.append( sampleValue )

		return value

	def exclude_value( self, value ):
		if self.insertValue is None:
			#need to exclude the one that is not sampled
			exptype = self.exptype
			if exptype == 'max':
				if value == 'max':
					return True
			elif exptype == 'min':
				if value == 'min':
					return True

		return False

	def head_str( self ):
		if self.exptype == 'map' and self.split:
			split = self.get_split()
			hstr = ''
			idx = 0
			keyList = self.keyMap.keys()
			for key in keyList:
				if idx > 0:
					hstr += split
				hstr += str(key)
				idx += 1
			return hstr
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		exptype = self.exptype
		if exptype == 'sum':
			return str(value)
		elif exptype == 'max':
			if value == 'max':
				if self.insertValue is not None:
					return self.insertValue
				return '-1'
			return str(value)
		elif exptype == 'min':
			if value == 'min':
				if self.insertValue is not None:
					return self.insertValue
				return '-1'
			return str(value)
		elif exptype == 'average':
			count = value[1]
			total = value[0]
			if count == 0:
				return '0'
			avg = round(total * 1.0 / count, 3)
			return str(avg)
		elif exptype == 'map':
			if not self.split:
				return str(value)
			else:
				idx = 0
				ss = ''
				split = self.get_split()
				keyList = self.keyMap.keys()
				for key in keyList:
					if idx > 0:
						ss += split
					count = 0
					if key in value:
						count = value[key]
					ss += str(count)
					idx += 1
				return ss
		else:
			return None

		return str( value )


class OutputsHelper( AnalyserHelper ):
	def __init__( self, outList, pace ):
		super(OutputsHelper, self).__init__()
		helperList = list()
		for ocfg in outList:
			ocfg.pace = pace
			helper = self.__create_output_helper( ocfg )
			helperList.append( helper )
			if not helper.isSingleType:
				self.isSingleType = False
		self.size = len(helperList)
		self.helperList = helperList

	def __create_output_helper( self, ocfg ):
		exptype = ocfg.exptype
		helper = None
		print 'exptype-->', exptype
		if exptype == 'map':
			helper = OutMapHelper( ocfg )

		if helper is None:
			helper = create_base_helper( ocfg )

		return helper

	def init_value( self, value ):
		vlist = list()
		for helper in self.helperList:
			val = helper.init_value( value )
			vlist.append( val )
		return vlist
	
	#return the statistics value
	def get_value( self, logInfo ):
		if not self.isSingleType:
			vlist = self.get_multiple_value( self.helperList, logInfo )
		else:
			vlist = list()
			for helper in self.helperList:
				val = helper.get_value( logInfo )
				vlist.append( val )
		return vlist

	def update_value( self, oldValue, sampleValue ):
		idx = 0
		for helper in self.helperList:
			newValue = sampleValue[idx]
			if newValue is not None:
				val = helper.update_value( oldValue[idx], newValue )
				oldValue[idx] = val
			idx += 1
		return oldValue

	def exclude_value( self, value ):
		idx = 0
		for helper in self.helperList:
			if not helper.exclude_value(value[idx]):
				return False
			idx += 1
		return True

	def head_str( self ):
		hstr = ''
		has = False
		idx = 0
		split = self.get_split()
		for helper in self.helperList:
			tstr = helper.head_str()
			if tstr is None:
				if helper.exptype != 'raw':
					tstr = helper.fmtName + '_' + helper.exptype
				else:
					tstr = helper.fmtName
			else:
				has = True
			
			if idx > 0:
				hstr += split
			hstr += tstr
			idx += 1

		if has:
			return hstr
		return None


	def str_value( self, value ):
		idx = 0
		vstr = ''
		split = self.get_split()
		for helper in self.helperList:
			if idx > 0:
				vstr += split
			vstr += helper.str_value( value[idx] )
			idx += 1

		return vstr
