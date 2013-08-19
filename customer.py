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
		'sr':'%sr_ip %sr_ua %sr_dt %sr_tm %sr_ur %sr_pt %sr_se %sr_rp %sr_st %sr_rm'
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
