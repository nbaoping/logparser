import random

from base import *

try:
	import hashlib
except:
	logging.error( 'no hashlib module' )

def get_md5( istr, argList ):
	m = hashlib.md5()
	m.update( istr )
	return m.hexdigest()

def rand_int( istr, argList ):
	if argList is None or len(argList) < 2:
		return -1

	beg = argList[0]
	end = argList[1]
	return random.randint( beg, end )

def fmt_arg_int( argItemList ):
	if argItemList is None or len(argItemList) == 0:
		return None

	argList = list()
	i = 0
	(val, isBasic) = argItemList[i]
	#is not BasicValue
	if not isBasic:
		val = int( val )
	argList.append( (val, isBasic) )
	i += 1
	if len(argItemList) > 1:
		(val, isBasic) = argItemList[i]
		if not isBasic:
			val = int( val )
		argList.append( (val,isBasic) )
	
	return argList

def sub_str( istr, argList ):
	if argList is None or len(argList) == 0:
		return istr

	sidx = argList[0]
	eidx = len(istr)
	if len(argList) > 1:
		eidx = argList[1]

	return istr[sidx:eidx]

def get_field_func( funcName ):
	fieldFunc = None
	if funcName == 'md5':
		if is_new_version():
			fieldFunc = get_md5
	elif funcName == 'substr':
		fieldFunc = sub_str
	elif funcName == 'rand':
		fieldFunc = rand_int

	if fieldFunc is None:
		logging.error( 'the function:'+funcName+' is not supported' )
	
	return fieldFunc

def fmt_func_arg_list( funcName, argItemList ):
	argList = argItemList
	if funcName == 'substr':
		argList = fmt_arg_int( argItemList )
	elif funcName == 'rand':
		argList = fmt_arg_int( argItemList )
	
	return argList

