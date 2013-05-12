

fmt_map = {
		'ft' : '%a %A %b %D %h %H %I %m %O %q %r %>s %t %T %U %V %X' 
}

def get_log_fmt( customer ):
	customer = customer.lower()
	if customer in fmt_map:
		return fmt_map[ customer ]
	return None
