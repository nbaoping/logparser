#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/08/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from base import *
from logparser import *


class TranslogCtx():
	def register( self ):
		raise_virtual( func_name() )


class SRXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%sr_ip', 'sr_ip', SRXactCtx.__parse_string, self )
		register_token( '%sr_ua', 'sr_uagent', SRXactCtx.__parse_string, self )
		register_token( '%sr_dt', 'sr_date', SRXactCtx.__parse_date, self )
		register_token( '%sr_tm', 'sr_time', SRXactCtx.__parse_time, self )
		register_token( '%sr_ur', 'sr_url', SRXactCtx.__parse_string, self )
		register_token( '%sr_pt', 'sr_protocol', SRXactCtx.__parse_string, self )
		register_token( '%sr_se', 'sr_server', SRXactCtx.__parse_string, self )
		register_token( '%sr_rp', 'sr_rpath', SRXactCtx.__parse_string, self )
		register_token( '%sr_st', 'sr_status', SRXactCtx.__parse_string, self )
		register_token( '%sr_rm', 'sr_method', SRXactCtx.__parse_string, self )

	def __parse_string( self, field, logInfo, fmt ):
		return field

	def __parse_date( self, field, logInfo, fmt ):
		logInfo.recvdTime = field
		return 0

	def __parse_time( self, field, logInfo, fmt ):
		logInfo.recvdTime += ' ' + field
		dtime = datetime.strptime( logInfo.recvdTime, self.timeFmt )
		secs = total_seconds( dtime )
		logInfo.recvdTime = secs
		return 0



__srxactCtx = SRXactCtx()

def register_translog():
	__srxactCtx.register()
