#*********************************************************************************************#
#*********************************************************************************************#
#******************witten by Neil Hao(nbaoping@cisco.com), 2013/05/19*************************#
#*********************************************************************************************#
#*********************************************************************************************#
#*********************************************************************************************#

#the format of web-engine translog that customer are using
__cus_fmt_map = {
		'ft' : '%a %A %b %D %h %H %I %m %O %q %r %>s %t %T %U %V %X',
		'telstra' : '%Z %D %a %R/%>s %O %m %u %M'
}


#the default format that other modules translog
__md_fmt_map = {
		'we_ext':'%Z %D %a %R/%>s %O %m %u %M',
		'we_apache':'%a %U %O %b %I %m %>s %t %D',
		'we_abr':'%a %y %i %u %abr_ad %O %I %S %Z %D %B %E %abr_md %>s %abr_ua %abr_gt %M',
		'we_ingest':'%wi_tim %wi_url %wi_fal %wi_sip %wi_red %wi_trd %wi_siz %wi_dpr %wi_dtm %wi_cbk %wi_sta %wi_mim %wi_rvl %wi_dmn %wi_cif %wi_ist %wi_rur %wi_ofl',
		'sr':'%sr_ip %sr_ua %sr_dt %sr_tm %sr_ur %sr_pt %sr_se %sr_rp %sr_st %sr_rm',
		'sm':'%sm_dat %sm_tim %sm_cpu %sm_mem %sm_kme %sm_dsk %sm_fdk %sm_pdk %sm_bnd %sm_fdc %sm_tsv %sm_tcl %sm_prs %sm_dsc %sm_mth %sm_mat %sm_msp %sm_mrs %sm_mrp %sm_fth %sm_fat %sm_fsp %sm_fcn %sm_wth %sm_wat %sm_wsp %sm_wcp %sm_wme %sm_wrq %sm_wse %sm_wup %sm_tth %sm_tat %sm_tsp %sm_tmc %sm_tmm %sm_tcc %sm_tcm %sm_tun %sm_trm %sm_tlv %sm_tvd %sm_tht %sm_trt %sm_gtp %sm_ucp %sm_ume',
		'cm':'%cm_dat %cm_tim %cm_cty %cm_opt %cm_pri %cm_cdt %cm_ctm %cm_fsz %cm_hct %cm_url %cm_pat',
		'wmt90':'%t_cip %t_dat %t_tim %t_cdn %t_cus %t_csm %t_xdr %t_crt %t_cst %t_cpd %t_cpv %t_cpl %t_cua %t_crf %t_che %t_chx %t_cos %t_cov %t_cpu %t_flt %t_fsz %t_abw %t_ptl %t_tpt %t_acd %t_vcd %t_cul %t_sbt %t_cbt %t_sps %t_cpr %t_cplc %t_cpln %t_cplcn %t_crr %t_cpre %t_cprr %t_cbc %t_ctbt %t_cqa %t_sip %t_sdn %t_stc %t_scu %t_cun %t_ssd %t_scp %t_cur %t_cmn %t_cmb %t_cmr %t_spr %t_sat %t_sebt %t_unm %t_egt %t_mmt %t_ctp',
		'ms':'%t_cip %t_dat %t_tim %t_cdn %t_cus %t_csm %t_xdr %t_crt %t_cst %t_cpd %t_cpv %t_cpl %t_cua %t_cos %t_cov %t_cpu %t_flt %t_fsz %t_abw %t_ptl %t_tpt %t_acd %t_vcd %t_sbt %t_csbt %t_cbt %t_sps %t_cpr %t_cplc %t_cbc %t_ctbt %t_cqa %t_sip %t_sdn %t_stc %t_scu %t_ury %t_cnm %t_srl',
		'fms':'%f_sip %f_xev %f_xctg %f_dat %f_tim %f_tz %f_xcx %f_sip %f_xpd %f_xcl %f_xml %f_xad %f_xvh %f_xap %f_xai %f_xdr %f_xst %f_cip %f_cpr %f_cpv %f_sur %f_csus %f_csuq %f_crf %f_cua %f_cci %f_csbt %f_scbt %f_cnt %f_xsn %f_xsq %f_xsrq %f_xss %f_xsu %f_xfn %f_xfe %f_xfs %f_xfl %f_xsp %f_csp %f_cssb %f_scsb %f_xsrn %f_xsqb %f_xcm %f_xed %f_xsd %f_xts %f_xtsq %f_xtfe %f_xtm %f_xso %f_xct %f_xcd %f_xpl %f_xpu %f_xsrs %f_xsrd %f_xff',
		'fms_old':'%f_sip %f_xev %f_xctg %f_dat %f_tim %f_tz %f_xcx %f_sip %f_xpd %f_xcl %f_xml %f_xad %f_xvh %f_xap %f_xai %f_xdr %f_xst %f_cip %f_cpr %f_sur %f_csus %f_csuq %f_crf %f_cua %f_cci %f_csbt %f_scbt %f_cnt %f_xsn %f_xsq %f_xsrq %f_xss %f_xsu %f_xfn %f_xfe %f_xfs %f_xfl %f_xsp %f_csp %f_cssb %f_scsb %f_xsrn %f_xsqb %f_xcm %f_xed %f_xsd %f_xts %f_xtsq %f_xtfe %f_xtm %f_xso %f_xpl %f_xpu %f_xsrd %f_xff'
		}


#some logs are sorted originally
__logs_sorted_map = {
		'sm':1
		}

def get_log_fmt( customer ):
	customer = customer.lower()
	if customer in __cus_fmt_map:
		return __cus_fmt_map[ customer ]
	return None

def get_module_fmt( mdname ):
	mdname = mdname.lower()
	if mdname in __md_fmt_map:
		return __md_fmt_map[mdname]
	return None

def is_log_sorted( mdname ):
	mdname = mdname.lower()
	if mdname in __logs_sorted_map:
		return True
	return False


