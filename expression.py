#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from base import *

class Expression( BaseObject ):
	def __init__( self ):
		pass

	def is_true( self, input ):
		raise_virtual( func_name() )

class AndExp( Expression ):
	def __init__( self, expList ):
		self.expList = expList

	def is_true( self, input ):
		if self.expList is None or len(self.expList) == 0:
			return True
		for exp in self.expList:
			if exp is not None and not exp.is_true(input):
				return False
		return True

class OrExp( Expression ):
	def __init__( self, expList ):
		self.expList = expList

	def is_true( self, input ):
		if self.expList is None or len(self.expList) == 0:
			return True
		for exp in self.expList:
			if exp is not None and exp.is_true(input):
				return True
		return False


class NotExp( Expression ):
	def __init__( self, exp ):
		self.exp = exp

	def is_true( self, input ):
		v = False
		if self.exp is not None:
			v = self.exp.is_true(input)

def parse_exp_from_xml( node, parse_func, arg ):
	expList = list()
	if node is None:
		return expList
	for cnode in node.childNodes:
		print cnode, cnode.nodeName
		name = cnode.nodeName
		if name == 'andExp':
			childList = parse_exp_from_xml( cnode, parse_func, arg )
			andExp = AndExp( childList )
			expList.append( andExp )
		elif name == 'orExp':
			childList = parse_exp_from_xml( cnode, parse_func, arg )
			orExp = OrExp( childList )
			expList.append( orExp )
		elif name == 'notExp':
			childList = parse_exp_from_xml( cnode, parse_func, arg )
			if childList is not None and len(childList) > 0:
				notExp = NotExp( childList[0] )
				expList.append( notExp )
		else:
			clist = parse_func( arg, cnode )
			if clist is not None:
				expList.extend( clist )
	return expList



