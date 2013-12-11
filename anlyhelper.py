from operator import itemgetter
from StringIO import StringIO
import traceback

from base import *

BUF_TIME = 36000		#in seconds
NUM_THRES = 36000

def time_average( value, logTime, servTime, startTime, pace ):
	endTime = logTime + servTime
	midSeg = endSeg = None
	midNum = 0
	metime = None
	mstime = int((logTime-startTime)/pace)*pace + pace + startTime
	if mstime < endTime:
		headSeg = mstime - logTime
		midNum = int((endTime-mstime)/pace)
		metime = midNum*pace + mstime
		midSeg = metime - mstime
		endSeg = endTime - metime
	else:
		headSeg = servTime
	
	hval = float(headSeg)/servTime * value
	mval = endVal = None
	if midNum > 0:
		mval = float(midSeg)/servTime * value / midNum
	else:
		mstime = None
	if endSeg is not None:
		endVal = float(endSeg)/servTime * value

	return (hval, (mstime, mval, midNum), (metime, endVal))

class AnalyserHelper( BaseObject ):
	def __init__( self, isSingleType = True ):
		self.isSingleType = isSingleType
		self.sampleThres = NUM_THRES
		self.useInterStr = False
		self.sorted = False
		self.insertValue = None
		self.oneProcessMode = 1

	def set_one_process_mode( self, mode ):
		self.oneProcessMode = mode

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

	def prepare_dump( self ):
		pass

	#get head string
	def str_head( self ):
		return None

	#parse head info from string
	def head_str( self, hstr, offset, psplit ):
		nidx = hstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(hstr)
		return nidx
	
	#get value string
	def str_value( self, value ):
		raise_virtual( func_name() )

	#parse value from String
	#the parsing must start from the offset
	#psplit stands for the split of the parent helper
	def value_str( self, vstr, offset, psplit ):
		print func_name(), '>>', 'not implement', self
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

	def read_outlist_value( self, helperList, vstr, offset, split ):
		outValList = list()
		cidx = offset
		maxLen = 0
		for helper in helperList:
			#print func_name(), '>>', vstr, 'cur:', vstr[cidx], 'idx:', cidx
			(valList, cidx) = helper.value_str( vstr, cidx, split )
			#print func_name(), '>>', valList, cidx
			vlen = len(valList)
			if vlen > maxLen:
				maxLen = vlen
			outValList.append( valList )
			cidx += 1	#skip the split char
		offset = cidx - 1 #back to the right tail idx

		#convert to the new list
		newValList = list()
		idx = 0
		#print func_name(), '>>maxLen:', maxLen, outValList
		while idx < maxLen:
			valList = list()
			for vlist in outValList:
				val = None
				if idx < len(vlist):
					val = vlist[idx]
				valList.append( val )

			newValList.append( valList )
			idx += 1

		#print func_name(), '>>maxLen:', maxLen, newValList
		return (newValList, offset)


class DesHelper( AnalyserHelper ):
	def __init__( self ):
		super(DesHelper, self).__init__()
	
	#return the statistics value
	def get_value( self, logInfo ):
		if not logInfo.exist( 'description' ):
			logInfo.description = 'null'
		value = dict()
		value[logInfo.description] = 1
		return value

	def init_value( self, value ):
		return dict()
	
	def update_value( self, oldValue, sampleValue ):
		for des in sampleValue.keys():
			count = sampleValue[ des ]
			if des in oldValue:
				count += oldValue[ des ]
			oldValue[des] = count
		return oldValue

	def exclude_value( self, value ):
		return len(value) == 0

	def str_head( self ):
		return 'description'

	def str_value( self, value ):
		return str( value )

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		value = vstr[offset:nidx]

		#decode the map value
		vmap = dict()
		value = value[1:len(value)-1]
		segs = value.split( ',' )
		for seg in segs:
			isegs = seg.split( ':' )
			des = isegs[0].strip()
			if des[0].startswith( "'" ):
				des = des[1:len(des)-1]
			count = int(isegs[1].strip())
			vmap[des] = count

		return ( [vmap], nidx )


class ConsumedHelper( AnalyserHelper ):
	def __init__( self ):
		super(ConsumedHelper, self).__init__()
	
	#return the statistics value
	def get_value( self, logInfo ):
		if not logInfo.exist( 'servTime' ):
			logInfo.servTime = 0
		return (logInfo.servTime / 1000000.0, 1)

	def init_value( self, value ):
		return (0, 0)
	
	def update_value( self, oldValue, sampleValue ):
		consumed = oldValue[0] + sampleValue[0]
		total = oldValue[1] + sampleValue[1]
		return (consumed, total)

	def exclude_value( self, value ):
		return value[0] < 0

	def str_head( self ):
		return 'consumed'

	def str_value( self, value ):
		consumed = value[0]
		total = value[1]
		if total <= 0:
			return '0'
		average = consumed / total
		average = round( average, 3 )
		return str( average )

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		consumed = float( vstr[offset:nidx] )
		value = (consumed, 1)

		return ( [value], nidx )


class AssembleHelper( AnalyserHelper ):
	def __init__( self ):
		super(AssembleHelper, self).__init__()
		self.useInterStr = True
		self.sampleThres = 1000000
	
	#return the statistics value
	def get_value( self, logInfo ):
		return (logInfo.recvdTime, logInfo.originLine)

	def init_value( self, value ):
		return list()
	
	def update_value( self, oldValue, sampleValue ):
		oldValue.append( sampleValue )
		return oldValue

	def exclude_value( self, value ):
		return len(value) == 0

	def str_value( self, value ):
		itemList = value
		if not self.sorted:
			itemList = sorted( value, key=itemgetter(0) )
		bufio = StringIO()
		first = True
		for item in itemList:
			line = item[1].rstrip()
			if first:
				bufio.write( line )
				first = False
			else:
				bufio.write( '\n' )
				bufio.write( line )
		logs = bufio.getvalue()
		bufio.close()
		return logs

	def value_str( self, vstr, offset, psplit ):
		logInfo = None
		try:
			logInfo = self.parser.parse_line( vstr )
		except:
			print traceback.print_exc()

		if logInfo is not None:
			sampleValue = self.get_value( logInfo )
			return ( logInfo.recvdTime, [sampleValue], len(vstr) )
		return ( 0, [], len(vstr) )


class CounterHelper( AnalyserHelper ):
	def __init__( self ):
		super(CounterHelper, self).__init__()
	
	#return the statistics value
	def get_value( self, logInfo ):
		return 1

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		return oldValue + sampleValue

	def exclude_value( self, value ):
		return value < 0

	def str_head( self ):
		return 'counter'

	def str_value( self, value ):
		return str( value )

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		value = int( vstr[offset:nidx] )

		return ( [value], nidx )


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

	def str_head( self ):
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

	def value_str( self, vstr, offset, psplit ):
		split = self.get_split()

		#decode the time string
		nidx = vstr.find( '.', offset )
		tstr = vstr[ offset:nidx ]
		seconds = seconds_str( tstr )
		offset = nidx + 1
		nidx = vstr.find( split )
		mstr = vstr[ offset:nidx ]
		msec = int(mstr) / 1000.0
		sampleTime = seconds + msec
		offset = nidx + 1

		(valList, offset) = self.__read_fmt_list( vstr, offset, split )
		item = (sampleTime, valList)
		return ( sampleValue, [item], offset)

	def __write_fmt_list( self, bufio, vlist, split ):
		num = 0
		for val in vlist:
			num += 1
			if num > 1:
				bufio.write( split )
			bufio.write( str(val) )

	def __read_fmt_list( self, vstr, offset, split ):
		vlist = list()
		cidx = offset
		for fmtName in self.fmtNameList:
			nidx = vstr.find( split, cidx )
			if nidx < 0:
				nidx = len(vstr)
			val = vstr[ cidx:nidx ]
			vlist.append( val )
			cidx = nidx + 1		#skip the split char

		return (vlist, cidx)


class OutTimeAverageHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutTimeAverageHelper, self).__init__( False )
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.pace = ocfg.pace
		self.unitRate = ocfg.unitRate
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

	def str_head( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		value = round( value * self.unitRate / self.pace, 3 )
		return str(value)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		tstr = vstr[offset:nidx]
		#print vstr, offset, tstr
		value = float( tstr ) * self.pace
		value /= self.unitRate

		return ( [value], nidx )


class OutTimeActiveHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutTimeActiveHelper, self).__init__( False )
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.pace = ocfg.pace
		self.unitRate = ocfg.unitRate
		self.insertValue = ocfg.insertValue
	
	def init_value( self, value ):
		return 0
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		servTime = value * self.unitRate
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

	def str_head( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		if self.calcTiming and self.pace > 0:
			value /= self.pace
			value = round( value, 3 )
		return str(value)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		if self.calcTiming and self.pace > 0:
			value = float( vstr[offset:nidx] )
			value *= self.pace
		else:
			value = int( vstr[offset:nidx] )

		return ( [value], nidx )


class OutCountHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutCountHelper, self).__init__()
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
		self.pace = ocfg.pace
	
	def get_value( self, logInfo ):
		return 1

	def init_value( self, value ):
		return 0
	
	def update_value( self, oldValue, sampleValue ):
		value = oldValue + sampleValue

		return value

	def exclude_value( self, value ):
		return False

	def str_head( self ):
		return self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'
		
		if self.calcTiming and self.pace > 0:
			value /= self.pace
			value = round( value, 3 )
		return str(value)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		if self.calcTiming and self.pace > 0:
			value = float( vstr[offset:nidx] )
			value *= self.pace
		else:
			value = int( vstr[offset:nidx] )

		return ( [value], nidx )


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

	def str_head( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		if self.calcTiming and self.pace > 0:
			value /= self.pace
			value = round( value, 3 )
		return str(value)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		value = float( vstr[offset:nidx] )
		if self.calcTiming and self.pace > 0:
			value *= self.pace

		return ( [value], nidx )


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

	def str_head( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		if value == 'max':
			if self.insertValue is not None:
				return self.insertValue
			return '0'
		return str(value)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		value = float( vstr[offset:nidx] )

		return ( [value], nidx )


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

	def str_head( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		if value == 'min':
			if self.insertValue is not None:
				return self.insertValue
			return '0'
		return str(value)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		value = float( vstr[offset:nidx] )

		return ( [value], nidx )


class OutAverageHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutAverageHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.insertValue = ocfg.insertValue
	
	def get_value( self, logInfo ):
		value = logInfo.get_member( self.fmtName )
		return (value, 1)

	def init_value( self, value ):
		return (0, 0)
	
	def update_value( self, oldValue, sampleValue ):
		total = oldValue[0] + sampleValue[0]
		count = oldValue[1] + sampleValue[1]
		value = (total, count)

		return value

	def exclude_value( self, value ):
		return False

	def str_head( self ):
		return self.fmtName + '_' + self.exptype + ';count'

	#parse head info from string
	def head_str( self, hstr, offset, psplit ):
		nidx = hstr.find( ';', offset )
		if nidx < 0:
			nidx = len(hstr)
		cidx = nidx + 1
		nidx = hstr.find( psplit, cidx )
		if nidx < 0:
			nidx = len(hstr)
		return nidx
	
	def str_value( self, value ):
		if value is None:
			return '0;0'

		count = value[1]
		total = value[0]
		if count == 0:
			return '0;0'
		avg = round(total * 1.0 / count, 3)
		return str(avg) + ';' + str(count)

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( ';', offset )
		if nidx < 0:
			nidx = len(vstr)
		value = float( vstr[offset:nidx] )
		
		curIdx = nidx + 1
		nidx = vstr.find( psplit, curIdx )
		if nidx < 0:
			nidx = len(vstr)
		count = int( vstr[curIdx:nidx] )

		value *= count
		value = (value, count)

		return ( [value], nidx )


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

	def str_head( self ):
		return self.fmtName + '_' + self.exptype

	def str_value( self, value ):
		if value is None:
			return '0'

		return str(len(value))

	def value_str( self, vstr, offset, psplit ):
		nidx = vstr.find( psplit, offset )
		if nidx < 0:
			nidx = len(vstr)
		value = int( vstr[offset:nidx] )

		return ( [value], nidx )


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
	
	if helper is not None:
		helper.calcTiming = ocfg.calcTiming
	return helper


class OutMapHelper( AnalyserHelper ):
	def __init__( self, ocfg ):
		super(OutMapHelper, self).__init__()
		self.fmtName = ocfg.fmtName
		self.exptype = ocfg.exptype
		self.sortOut = ocfg.sort
		self.pace = ocfg.pace
		self.insertValue = ocfg.insertValue
		self.keyMap = dict()
		self.keyList = None		#used to string the value
		self.curKeyList = None	#used to decode the value
		self.split = True
		self.endChar = ' '
		self.helperList = None
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
		helper = create_base_helper( ocfg )
		print func_name(), ocfg, helper

		return helper

	def set_one_process_mode( self, mode ):
		self.oneProcessMode = mode
		if self.helperList is None:
			return

		for helper in self.helperList:
			helper.set_one_process_mode( mode )

	def init_value( self, value ):
		value = dict()
		return value
	
	#for map, the return value must contain the key value and childs' values
	def get_value( self, logInfo ):
		#return the key
		key = logInfo.get_member( self.fmtName )
		
		if self.helperList is not None:
			if not self.isSingleType:
				valList = self.get_multiple_value( self.helperList, logInfo )
				newList = list()
				#add key value to child value list
				for (cvalList, num) in valList:
					item = (key, cvalList)
					newList.append( (item, num) )
				value = newList
			else:
				valList = list()
				for helper in self.helperList:
					val = helper.get_value( logInfo )
					valList.append( val )
				
				value = (key, valList)
		else:
			value = (key, 1)

		return value

	def update_value( self, oldValue, sampleValue ):
		value = oldValue
		if self.helperList is None:
			(key, count) = sampleValue
			if key in oldValue:
				count += oldValue[key]
			else:
				self.keyMap[key] = 1
			oldValue[key] = count
		else:
			(key, newValList) = sampleValue
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

	def prepare_dump( self ):
		if self.helperList is not None:
			for helper in self.helperList:
				helper.prepare_dump()
		
		if self.keyList is None:
			self.keyList = self.keyMap.keys()
		self.keyList.sort()

	def str_head( self ):
		return self.__str_head()

	def __str_head( self ):
		split = self.get_split()
		hstr = ''
		for key in self.keyList:
			if hstr != '':
				hstr += split
			if self.helperList is None:
				subHead = str(key)
			else:
				subHead = self.__get_outlist_head( key, split )

			hstr += subHead

		return hstr + split


	def head_str( self, hstr, offset, psplit ):
		split = self.get_split()
		curoff = offset
		size = len(hstr)
		if self.keyList is None:
			self.keyList = list()
		self.curkeyList = list()
		addNewKey = False
		while curoff < size:
			if hstr[curoff] == psplit:
				break

			if self.helperList is None:
				nidx = hstr.find( split, curoff )
				key = hstr[curoff:nidx]
				curoff = nidx
			else:
				(key, curoff) = self.__read_outlist_head( hstr, curoff, split )
			self.curkeyList.append( key )
			if key not in self.keyList:
				addNewKey = True
				self.keyList.append( key )
			curoff += 1
		
		if addNewKey:
			self.keyList.sort()

		return curoff

	def __get_outlist_head( self, key, split ):
		subHead = ''
		kstr = str(key)
		for helper in self.helperList:
			th = kstr + '_' + helper.str_head()
			if subHead != '':
				subHead += split
			subHead += th 

		return subHead

	def __read_outlist_head( self, hstr, offset, split ):
		cidx = offset
		for helper in self.helperList:
			nidx = hstr.find( '_', cidx )
			key = hstr[cidx:nidx]
			cidx = nidx + 1 #skip the '_'
			nidx = helper.head_str( hstr, cidx, split )
			cidx = nidx + 1 #skip the split

		return (key, nidx)


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

	def __need_sort( self ):
		if self.oneProcessMode and self.pace < 0 and self.sortOut and self.helperList is None:
			return True
		return False

	def str_value( self, value ):
		if self.__need_sort():
			return self.__sort_str_value( value )
		else:
			return self.__str_value( value )

	def __str_value( self, value ):
		idx = 0
		ss = ''
		split = self.get_split()

		for key in self.keyList:
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
		return ss + split

	def __sort_str_value( self, value ):
		split = self.get_split()
		itemList = list()
		for key in self.keyList:
			if key in value:
				count = value[key]
			else:
				count = 0
			itemList.append( (count, key) )
		
		sortList = sorted( itemList, key=itemgetter(0), reverse=True )
		vstr = '\nchannel' + split + 'total count' + '\n'
		for (count, key) in sortList:
			vstr += key + split + str(count) + '\n'

		return vstr

	def value_str( self, vstr, offset, psplit ):
		split = self.get_split()
		cidx = offset
		outValList = list()
		for key in self.curkeyList:
			if self.helperList is None:
				nidx = vstr.find( split, cidx )
				count = int( vstr[cidx:nidx] )
				cidx = nidx + 1
				outValList.append( (key, count) )
			else:
				(olist, cidx) = self.read_outlist_value( self.helperList, vstr, cidx, split )
				for vlist in olist:
					outValList.append( (key, vlist) )
				cidx += 1

		return (outValList, cidx)


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

	def str_head( self ):
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

	def set_one_process_mode( self, mode ):
		self.oneProcessMode = mode
		for helper in self.helperList:
			helper.set_one_process_mode( mode )

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

	def prepare_dump( self ):
		for helper in self.helperList:
			helper.prepare_dump()

	def str_head( self ):
		hstr = ''
		has = False
		idx = 0
		split = self.get_split()
		for helper in self.helperList:
			tstr = helper.str_head()
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

	def head_str( self, hstr, offset, psplit ):
		split = self.get_split()
		cidx = offset
		for helper in self.helperList:
			cidx = helper.head_str( hstr, cidx, split )
			cidx += 1

		return cidx - 1

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

	def value_str( self, vstr, offset, psplit ):
		split = self.get_split()
		cidx = offset
		(outValList, cidx) = self.read_outlist_value( self.helperList, vstr, cidx, split )

		return (outValList, cidx)


