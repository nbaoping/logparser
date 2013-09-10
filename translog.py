#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/08/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

from base import *
from logparser import *


__RECVD_TIME_FMT = '[%d/%b/%Y:%H:%M:%S'
def parse_recvd_time( field, logInfo, fmt ):
	segs = field.split( '.' )
	dtime = strptime( segs[0], __RECVD_TIME_FMT )
	dtime = total_seconds( dtime )
	mstr = segs[1]
	nsegs = mstr.split( '+' )
	if len(nsegs) < 2:
		nsegs = mstr.split( '-' )
	if len(nsegs) >= 2:
		msec = float( nsegs[0] )
		dtime += (msec/1000.0)
	return dtime

class TranslogCtx( object ):
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
		dtime = strptime( logInfo.recvdTime, self.timeFmt )
		secs = total_seconds( dtime )
		logInfo.recvdTime = secs
		return 0


class SMXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%sm_dat', 'sm_date', SMXactCtx.__parse_date, self )
		register_token( '%sm_tim', 'sm_time', SMXactCtx.__parse_time, self )
		register_token( '%sm_cpu', 'sm_cpu', SMXactCtx.__parse_int, self )
		register_token( '%sm_mem', 'sm_mem', SMXactCtx.__parse_int, self )
		register_token( '%sm_kme', 'sm_kmem', SMXactCtx.__parse_int, self )
		register_token( '%sm_dsk', 'sm_disk', SMXactCtx.__parse_int, self )
		register_token( '%sm_fdk', 'sm_fail_disk', SMXactCtx.__parse_string, self )
		register_token( '%sm_pdk', 'sm_perdisk', SMXactCtx.__parse_disk_load, self )
		register_token( '%sm_bnd', 'sm_bandwidth', SMXactCtx.__parse_interface_usage, self )
		register_token( '%sm_fdc', 'sm_fdcount', SMXactCtx.__parse_int, self )
		register_token( '%sm_tsv', 'sm_tcp_server', SMXactCtx.__parse_int, self )
		register_token( '%sm_tcl', 'sm_tcp_client', SMXactCtx.__parse_int, self )
		register_token( '%sm_prs', 'sm_processes', SMXactCtx.__parse_int, self )
		register_token( '%sm_dsc', 'sm_ds_cpu', SMXactCtx.__parse_float, self )
		register_token( '%sm_mth', 'sm_ms_thres', SMXactCtx.__parse_string, self )
		register_token( '%sm_mat', 'sm_ms_augthres', SMXactCtx.__parse_string, self )
		register_token( '%sm_msp', 'sm_ms_stop', SMXactCtx.__parse_string, self )
		register_token( '%sm_mrs', 'sm_ms_rtsp', SMXactCtx.__parse_int, self )
		register_token( '%sm_mrp', 'sm_ms_rtp', SMXactCtx.__parse_int, self )
		register_token( '%sm_fth', 'sm_fms_thres', SMXactCtx.__parse_string, self )
		register_token( '%sm_fat', 'sm_fms_augthres', SMXactCtx.__parse_string, self )
		register_token( '%sm_fsp', 'sm_fms_stop', SMXactCtx.__parse_string, self )
		register_token( '%sm_fcn', 'sm_fms_connections', SMXactCtx.__parse_int, self )
		register_token( '%sm_wth', 'sm_web_thres', SMXactCtx.__parse_string, self )
		register_token( '%sm_wat', 'sm_web_augthres', SMXactCtx.__parse_string, self )
		register_token( '%sm_wsp', 'sm_web_stop', SMXactCtx.__parse_string, self )
		register_token( '%sm_wcp', 'sm_web_cpu', SMXactCtx.__parse_float, self )
		register_token( '%sm_wme', 'sm_web_mem', SMXactCtx.__parse_int, self )
		register_token( '%sm_wrq', 'sm_web_requests', SMXactCtx.__parse_int, self )
		register_token( '%sm_wse', 'sm_web_sessions', SMXactCtx.__parse_int, self )
		register_token( '%sm_wup', 'sm_web_upstreams', SMXactCtx.__parse_int, self )
		register_token( '%sm_tth', 'sm_wmt_thres', SMXactCtx.__parse_string, self )
		register_token( '%sm_tat', 'sm_wmt_augthres', SMXactCtx.__parse_string, self )
		register_token( '%sm_tsp', 'sm_wmt_stop', SMXactCtx.__parse_string, self )
		register_token( '%sm_tmc', 'sm_wmt_mlcpu', SMXactCtx.__parse_float, self )
		register_token( '%sm_tmm', 'sm_wmt_mlmem', SMXactCtx.__parse_int, self )
		register_token( '%sm_tcc', 'sm_wmt_ccpu', SMXactCtx.__parse_float, self )
		register_token( '%sm_tcm', 'sm_wmt_cmem', SMXactCtx.__parse_int, self )
		register_token( '%sm_tun', 'sm_wmt_unicasts', SMXactCtx.__parse_int, self )
		register_token( '%sm_trm', 'sm_wmt_remotes', SMXactCtx.__parse_int, self )
		register_token( '%sm_tlv', 'sm_wmt_live', SMXactCtx.__parse_int, self )
		register_token( '%sm_tvd', 'sm_wmt_vod', SMXactCtx.__parse_int, self )
		register_token( '%sm_tht', 'sm_wmt_http', SMXactCtx.__parse_int, self )
		register_token( '%sm_trt', 'sm_wmt_rtsp', SMXactCtx.__parse_int, self )
		register_token( '%sm_gtp', 'sm_rtspg_tps', SMXactCtx.__parse_int, self )
		register_token( '%sm_ucp', 'sm_uns_cpu', SMXactCtx.__parse_float, self )
		register_token( '%sm_ume', 'sm_uns_mem', SMXactCtx.__parse_int, self )

	def __parse_string( self, field, logInfo, fmt ):
		return field

	def __parse_int( self, field, logInfo, fmt ):
		if field == '-':
			return 0
		return int(field)

	def __parse_float( self, field, logInfo, fmt ):
		if field == '-':
			return 0.0
		return float(field)

	def __parse_date( self, field, logInfo, fmt ):
		logInfo.recvdTime = field
		return 0

	def __parse_time( self, field, logInfo, fmt ):
		logInfo.recvdTime += ' ' + field
		dtime = strptime( logInfo.recvdTime, self.timeFmt )
		secs = total_seconds( dtime )
		logInfo.recvdTime = secs
		return 0

	def __parse_disk_load( self, field, logInfo, fmt ):
		segs = field.split( '|' )
		total = 0
		loadList = list()
		for item in segs:
			isegs = item.split( '^' )
			load = float( isegs[1] )
			loadList.append( load )
			total += load
		loadList.append( total )
		dstr = ''
		idx = 0
		for load in loadList:
			if idx > 0:
				dstr += '|'
			dstr += str(load)
			idx += 1
		return dstr

	def __parse_interface_usage( self, field, logInfo, fmt ):
		segs = field.split( '|' )
		usageList = list()
		for item in segs:
			isegs = item.split( '^' )
			iusage = int( isegs[1] )
			ousage = int( isegs[2] )
			usageList.append( (iusage, ousage) )

		istr = ''
		ostr = ''
		idx = 0
		for item in usageList:
			if idx > 0:
				istr += '|'
				ostr += '|'
			istr += str(item[0])
			ostr += str(item[1])
			idx += 1
		return istr + ';' + ostr


class WEXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%C', 'lookupTime', WEXactCtx.__parse_lookup_time, self )
		register_token( '%C_alt', 'auth_time', WEXactCtx.__parse_string, self )
		register_token( '%C_clt', 'cal_time', WEXactCtx.__parse_string, self )
		register_token( '%C_crt', 'cr_time', WEXactCtx.__parse_string, self )
		register_token( '%C_odt', 'os_time', WEXactCtx.__parse_string, self )

	def __parse_lookup_time( self, field, logInfo, fmt ):
		segs = field.split( '|' )
		#convert to milliseconds
		logInfo.auth_time = float(segs[0]) / 1000
		logInfo.cal_time = float(segs[1]) / 1000
		logInfo.cr_time = float(segs[2]) / 1000
		logInfo.os_time = float(segs[3]) / 1000
		return field

	def __parse_string( self, field, logInfo, fmt ):
		return field


class WEAbrCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%abr_md', 'abr_mode', WEAbrCtx.__parse_string, self )
		register_token( '%abr_ua', 'abr_agent', WEAbrCtx.__parse_string, self )
		register_token( '%abr_gt', 'abr_gen_time', WEAbrCtx.__parse_string, self )
		register_token( '%abr_ad', 'abr_aid', WEAbrCtx.__parse_string, self )

	def __parse_string( self, field, logInfo, fmt ):
		return field


class WEIngestLogCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S'
		self.connTimeFmt = '%a_%b_%d_%H_%M_%S_%Y'

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%wi_tim', 'wi_time', WEIngestLogCtx.__parse_time, self )
		register_token( '%wi_url', 'wi_url', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_fal', 'wi_fail_list', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_sip', 'wi_sip', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_red', 'wi_read', WEIngestLogCtx.__parse_int, self )
		register_token( '%wi_trd', 'wi_to_read', WEIngestLogCtx.__parse_int, self )
		register_token( '%wi_siz', 'wi_size', WEIngestLogCtx.__parse_int, self )
		register_token( '%wi_dpr', 'wi_percentage', WEIngestLogCtx.__parse_int, self )
		register_token( '%wi_dtm', 'wi_dtime', WEIngestLogCtx.__parse_int, self )
		register_token( '%wi_cbk', 'wi_callback', WEIngestLogCtx.__parse_int, self )
		register_token( '%wi_sta', 'wi_status', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_mim', 'wi_mime', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_rvl', 'wi_reval', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_dmn', 'wi_domain', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_cif', 'wi_con_info', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_ist', 'wi_in_status', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_rur', 'wi_re_url', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_ofl', 'wi_os_fail', WEIngestLogCtx.__parse_string, self )
		#five more fmtName derived from token %wi_cif
		register_token( '%wi_lpt', 'wi_local_port', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_pto', 'wi_protocol', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_ctm', 'wi_conn_time', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_rty', 'wi_retry', WEIngestLogCtx.__parse_string, self )
		register_token( '%wi_rus', 'wi_reuse', WEIngestLogCtx.__parse_string, self )

	def __parse_string( self, field, logInfo, fmt ):
		return field

	def __parse_time( self, field, logInfo, fmt ):
		time = parse_recvd_time( field, logInfo, fmt )
		logInfo.recvdTime = time
		return time

	def __parse_int( self, field, logInfo, fmt ):
		if field == '-':
			return 0
		return int(field)

	#five more fmtName will be parsed
	def __parse_conn_info( self, field, logInfo, fmt ):
		segs = field.split( '|' )
		i = 0
		logInfo.wi_local_porti = int( segs[i] )
		i += 1
		if len(segs) > 4:
			#some version has the protocol field
			logInfo.wi_protocol = segs[i]
			i += 1
		logInfo.wi_conn_time = self.__parse_conn_time( segs[i] )
		i += 1
		logInfo.wi_retry = int( segs[i] )
		i += 1
		logInfo.wi_reuse = int( segs[i] )
		return field

	#Sat_Aug_10_16:19:52_2013
	#%a_%b_%d_%H_%M_%S_%Y
	def __parse_conn_time( self, tstr ):
		dtime = strptime( tstr, self.connTimeFmt )
		dtime = total_seconds( dtime )
		return dtime
		

class CMXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y/%m/%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%cm_dat', 'cm_date', CMXactCtx.__parse_date, self )
		register_token( '%cm_tim', 'cm_time', CMXactCtx.__parse_time, self )
		register_token( '%cm_cty', 'cm_ctype', CMXactCtx.__parse_string, self )
		register_token( '%cm_opt', 'cm_operation', CMXactCtx.__parse_string, self )
		register_token( '%cm_pri', 'cm_priority', CMXactCtx.__parse_float, self )
		register_token( '%cm_cdt', 'cm_cdate', CMXactCtx.__parse_string, self )
		register_token( '%cm_ctm', 'cm_ctime', CMXactCtx.__parse_string, self )
		register_token( '%cm_fsz', 'cm_fsize', CMXactCtx.__parse_int, self )
		register_token( '%cm_hct', 'cm_hit_count', CMXactCtx.__parse_int, self )
		register_token( '%cm_url', 'cm_url', CMXactCtx.__parse_string, self )
		register_token( '%cm_pat', 'cm_path', CMXactCtx.__parse_string, self )
		
	def __parse_string( self, field, logInfo, fmt ):
		return field

	def __parse_date( self, field, logInfo, fmt ):
		logInfo.recvdTime = field
		return field

	def __parse_time( self, field, logInfo, fmt ):
		logInfo.recvdTime += ' ' + field
		segs = logInfo.recvdTime.split( '.' )
		dtime = strptime( segs[0], self.timeFmt )
		secs = total_seconds( dtime )
		if len(segs) > 1:
			ms = int(segs[1]) / 1000.0
			secs += ms
		logInfo.recvdTime = secs
		return field

	def __parse_int( self, field, logInfo, fmt ):
		if field == '-':
			return 0
		return int(field)

	def __parse_float( self, field, logInfo, fmt ):
		if field == '-':
			return 0.0
		return float(field)

	def __parse_create_date( self, field, logInfo, fmt ):
		logInfo.cm_ctime = field
		return field

	def __parse_create_time( self, field, logInfo, fmt ):
		logInfo.cm_ctime += ' ' + field
		dtime = strptime( logInfo.cm_ctime, self.timeFmt )
		secs = total_seconds( dtime )
		return secs


class WMTXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%t_cip', 't_c_ip', WMTXactCtx.__parse_string, self )
		register_token( '%t_dat', 't_date', WMTXactCtx.__parse_date, self )
		register_token( '%t_tim', 't_time', WMTXactCtx.__parse_time, self )
		register_token( '%t_cdn', 't_c_dns', WMTXactCtx.__parse_string, self )
		register_token( '%t_cus', 't_cs_uri_stem', WMTXactCtx.__parse_string, self )
		register_token( '%t_csm', 't_c_starttime', WMTXactCtx.__parse_int, self )
		register_token( '%t_xdr', 't_x_duration', WMTXactCtx.__parse_int, self )
		register_token( '%t_crt', 't_c_rate', WMTXactCtx.__parse_int, self )
		register_token( '%t_cst', 't_c_status', WMTXactCtx.__parse_string, self )
		register_token( '%t_cpd', 't_c_playerid', WMTXactCtx.__parse_string, self )
		register_token( '%t_cpv', 't_c_playerversion', WMTXactCtx.__parse_string, self )
		register_token( '%t_cpl', 't_c_playerlanguage', WMTXactCtx.__parse_string, self )
		register_token( '%t_cua', 't_cs(User_Agent)', WMTXactCtx.__parse_string, self )
		register_token( '%t_crf', 't_cs(Referer)', WMTXactCtx.__parse_string, self )
		register_token( '%t_che', 't_c_hostexe', WMTXactCtx.__parse_string, self )
		register_token( '%t_chx', 't_c_hostexever', WMTXactCtx.__parse_string, self )
		register_token( '%t_cos', 't_c_os', WMTXactCtx.__parse_string, self )
		register_token( '%t_cov', 't_c_osversion', WMTXactCtx.__parse_string, self )
		register_token( '%t_cpu', 't_c_cpu', WMTXactCtx.__parse_string, self )
		register_token( '%t_flt', 't_filelength', WMTXactCtx.__parse_int, self )
		register_token( '%t_fsz', 't_filesize', WMTXactCtx.__parse_int, self )
		register_token( '%t_abw', 't_avgbandwidth', WMTXactCtx.__parse_int, self )
		register_token( '%t_ptl', 't_protocol', WMTXactCtx.__parse_string, self )
		register_token( '%t_tpt', 't_transport', WMTXactCtx.__parse_string, self )
		register_token( '%t_acd', 't_audiocodec', WMTXactCtx.__parse_string, self )
		register_token( '%t_vcd', 't_videocodec', WMTXactCtx.__parse_string, self )
		register_token( '%t_cul', 't_channelURL', WMTXactCtx.__parse_string, self )
		register_token( '%t_sbt', 't_sc_bytes', WMTXactCtx.__parse_int, self )
		register_token( '%t_cbt', 't_c_bytes', WMTXactCtx.__parse_int, self )
		register_token( '%t_sps', 't_s_pkts_sent', WMTXactCtx.__parse_int, self )
		register_token( '%t_cpr', 't_c_pkts_received', WMTXactCtx.__parse_int, self )
		register_token( '%t_cplc', 't_c_pkts_lost_client', WMTXactCtx.__parse_int, self )
		register_token( '%t_cpln', 't_c_pkts_lost_net', WMTXactCtx.__parse_int, self )
		register_token( '%t_cplcn', 't_c_pkts_lost_cont_net', WMTXactCtx.__parse_int, self )
		register_token( '%t_crr', 't_c_resendreqs', WMTXactCtx.__parse_int, self )
		register_token( '%t_cpre', 't_c_pkts_recovered_ECC', WMTXactCtx.__parse_int, self )
		register_token( '%t_cprr', 't_c_pkts_recovered_resent', WMTXactCtx.__parse_int, self )
		register_token( '%t_cbc', 't_c_buffercount', WMTXactCtx.__parse_int, self )
		register_token( '%t_ctbt', 't_c_totalbuffertime', WMTXactCtx.__parse_int, self )
		register_token( '%t_cqa', 't_c_quality', WMTXactCtx.__parse_int, self )
		register_token( '%t_sip', 't_s_ip', WMTXactCtx.__parse_string, self )
		register_token( '%t_sdn', 't_s_dns', WMTXactCtx.__parse_string, self )
		register_token( '%t_stc', 't_s_totalclients', WMTXactCtx.__parse_int, self )
		register_token( '%t_scu', 't_s_cpu_util', WMTXactCtx.__parse_string, self )
		register_token( '%t_cun', 't_cs_user_name', WMTXactCtx.__parse_string, self )
		register_token( '%t_ssd', 't_s_session_id', WMTXactCtx.__parse_string, self )
		register_token( '%t_scp', 't_s_content_path', WMTXactCtx.__parse_string, self )
		register_token( '%t_cur', 't_cs_url', WMTXactCtx.__parse_string, self )
		register_token( '%t_cmn', 't_cs_media_name', WMTXactCtx.__parse_string, self )
		register_token( '%t_cmb', 't_c_max_bandwidth', WMTXactCtx.__parse_int, self )
		register_token( '%t_cmr', 't_cs_media_role', WMTXactCtx.__parse_string, self )
		register_token( '%t_spr', 't_s_proxied', WMTXactCtx.__parse_string, self )
		register_token( '%t_sat', 't_SE_action', WMTXactCtx.__parse_string, self )
		register_token( '%t_sebt', 't_SE_bytes', WMTXactCtx.__parse_int, self )
		register_token( '%t_unm', 't_Username', WMTXactCtx.__parse_string, self )
		register_token( '%t_egt', 't_entry_gen_time', WMTXactCtx.__parse_string, self )
		register_token( '%t_mmt', 't_mime_type', WMTXactCtx.__parse_string, self )
		register_token( '%t_ctp', 't_client_type', WMTXactCtx.__parse_string, self )

		
	def __parse_string( self, field, logInfo, fmt ):
		return field

	def __parse_int( self, field, logInfo, fmt ):
		if field == '-':
			return 0
		return int(field)

	def __parse_date( self, field, logInfo, fmt ):
		logInfo.t_time = field
		return field

	def __parse_time( self, field, logInfo, fmt ):
		logInfo.t_time += ' ' + field
		dtime = strptime( logInfo.t_time, self.timeFmt )
		secs = total_seconds( dtime )
		logInfo.recvdTime = secs
		return secs
	
	#[21/Mar/2012:04:05:45.337+0000]
	def __parse_gen_time( self, field, logInfo, fmt ):
		time = parse_recvd_time( field, logInfo, fmt )
		return time


class MSXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%t_csbt', 't_cs_bytes', MSXactCtx.__parse_string, self )
		register_token( '%t_ury', 't_uri_query', MSXactCtx.__parse_string, self )
		register_token( '%t_cnm', 't_c_username', MSXactCtx.__parse_string, self )
		register_token( '%t_srl', 't_sc_realm', MSXactCtx.__parse_string, self )

	def __parse_string( self, field, logInfo, fmt ):
		return field


class FMSXactCtx( TranslogCtx ):
	def __init__( self ):
		self.timeFmt = '%Y-%m-%d %H:%M:%S' 

	def register( self ):
		self.register_tokens()

	def register_tokens( self ):
		register_token( '%f_xev', 'f_x_event', FMSXactCtx.__parse_string, self )
		register_token( '%f_xctg', 'f_x_category', FMSXactCtx.__parse_string, self )
		register_token( '%f_dat', 'f_date', FMSXactCtx.__parse_date, self )
		register_token( '%f_tim', 'f_time', FMSXactCtx.__parse_time, self )
		register_token( '%f_tz', 'f_tz', FMSXactCtx.__parse_string, self )
		register_token( '%f_xcx', 'f_x_ctx', FMSXactCtx.__parse_string, self )
		register_token( '%f_sip', 'f_s_ip', FMSXactCtx.__parse_string, self )
		register_token( '%f_xpd', 'f_x_pid', FMSXactCtx.__parse_string, self )
		register_token( '%f_xcl', 'f_x_cpu_load', FMSXactCtx.__parse_int, self )
		register_token( '%f_xml', 'f_x_mem_load', FMSXactCtx.__parse_int, self )
		register_token( '%f_xad', 'f_x_adaptor', FMSXactCtx.__parse_string, self )
		register_token( '%f_xvh', 'f_x_vhost', FMSXactCtx.__parse_string, self )
		register_token( '%f_xap', 'f_x_app', FMSXactCtx.__parse_string, self )
		register_token( '%f_xai', 'f_x_appinst', FMSXactCtx.__parse_string, self )
		register_token( '%f_xdr', 'f_x_duration', FMSXactCtx.__parse_string, self )
		register_token( '%f_xst', 'f_x_status', FMSXactCtx.__parse_string, self )
		register_token( '%f_cip', 'f_c_ip', FMSXactCtx.__parse_string, self )
		register_token( '%f_cpr', 'f_c_proto', FMSXactCtx.__parse_string, self )
		register_token( '%f_cpv', 'f_c_proto_ver', FMSXactCtx.__parse_string, self )
		register_token( '%f_sur', 'f_s_uri', FMSXactCtx.__parse_string, self )
		register_token( '%f_csus', 'f_cs_uri_stem', FMSXactCtx.__parse_string, self )
		register_token( '%f_csuq', 'f_cs_uri_query', FMSXactCtx.__parse_string, self )
		register_token( '%f_crf', 'f_c_referrer', FMSXactCtx.__parse_string, self )
		register_token( '%f_cua', 'f_c_user_agent', FMSXactCtx.__parse_string, self )
		register_token( '%f_cci', 'f_c_client_id', FMSXactCtx.__parse_string, self )
		register_token( '%f_csbt', 'f_cs_bytes', FMSXactCtx.__parse_int, self )
		register_token( '%f_scbt', 'f_sc_bytes', FMSXactCtx.__parse_int, self )
		register_token( '%f_cnt', 'f_c_connect_type', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsn', 'f_x_sname', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsq', 'f_x_sname_query', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsrq', 'f_x_suri_query', FMSXactCtx.__parse_string, self )
		register_token( '%f_xss', 'f_x_suri_stem', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsu', 'f_x_suri', FMSXactCtx.__parse_string, self )
		register_token( '%f_xfn', 'f_x_file_name', FMSXactCtx.__parse_string, self )
		register_token( '%f_xfe', 'f_x_file_ext', FMSXactCtx.__parse_string, self )
		register_token( '%f_xfs', 'f_x_file_size', FMSXactCtx.__parse_int, self )
		register_token( '%f_xfl', 'f_x_file_length', FMSXactCtx.__parse_float, self )
		register_token( '%f_xsp', 'f_x_spos', FMSXactCtx.__parse_int, self )
		register_token( '%f_csp', 'f_c_spos', FMSXactCtx.__parse_int, self )
		register_token( '%f_cssb', 'f_cs_stream_bytes', FMSXactCtx.__parse_int, self )
		register_token( '%f_scsb', 'f_sc_stream_bytes', FMSXactCtx.__parse_int, self )
		register_token( '%f_xsrn', 'f_x_service_name', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsqb', 'f_x_sc_qos_bytes', FMSXactCtx.__parse_int, self )
		register_token( '%f_xcm', 'f_x_comment', FMSXactCtx.__parse_string, self )
		register_token( '%f_xed', 'f_x_eid', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsd', 'f_x_sid', FMSXactCtx.__parse_string, self )
		register_token( '%f_xts', 'f_x_trans_sname', FMSXactCtx.__parse_string, self )
		register_token( '%f_xtsq', 'f_x_trans_sname_query', FMSXactCtx.__parse_string, self )
		register_token( '%f_xtfe', 'f_x_trans_file_ext', FMSXactCtx.__parse_string, self )
		register_token( '%f_xtm', 'f_x_trans_mode', FMSXactCtx.__parse_string, self )
		register_token( '%f_xso', 'f_x_soffset', FMSXactCtx.__parse_int, self )
		register_token( '%f_xct', 'f_x_codec_type', FMSXactCtx.__parse_string, self )
		register_token( '%f_xcd', 'f_x_codec', FMSXactCtx.__parse_string, self )
		register_token( '%f_xpl', 'f_x_plugin', FMSXactCtx.__parse_string, self )
		register_token( '%f_xpu', 'f_x_page_url', FMSXactCtx.__parse_string, self )
		register_token( '%f_xsrs', 'f_x_smax_rec_size', FMSXactCtx.__parse_int, self )
		register_token( '%f_xsrd', 'f_x_smax_rec_duration', FMSXactCtx.__parse_int, self )
		register_token( '%f_xff', 'f_x_forwarded_for', FMSXactCtx.__parse_string, self )
		
	def __parse_string( self, field, logInfo, fmt ):
		return field

	def __parse_int( self, field, logInfo, fmt ):
		if field == '-':
			return 0
		return int(field)

	def __parse_float( self, field, logInfo, fmt ):
		if field == '-':
			return 0.0
		return float(field)

	def __parse_date( self, field, logInfo, fmt ):
		logInfo.recvdTime = field
		return field

	def __parse_time( self, field, logInfo, fmt ):
		logInfo.recvdTime += ' ' + field
		dtime = strptime( logInfo.recvdTime, self.timeFmt )
		secs = total_seconds( dtime )
		logInfo.recvdTime = secs
		return 0



__srxactCtx = SRXactCtx()
__smxactCtx = SMXactCtx()
__wexactCtx = WEXactCtx()
__weabrCtx = WEAbrCtx()
__weingestLogCtx = WEIngestLogCtx()
__cmxactCtx = CMXactCtx()
__wmtxactCtx = WMTXactCtx()
__msxactCtx = MSXactCtx()
__fmsxactCtx = FMSXactCtx()

def register_translog():
	__srxactCtx.register()
	__smxactCtx.register()
	__wexactCtx.register()
	__weabrCtx.register()
	__weingestLogCtx.register()
	__cmxactCtx.register()
	__wmtxactCtx.register()
	__msxactCtx.register()
	__fmsxactCtx.register()
