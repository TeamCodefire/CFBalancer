import argparse;
from hashlib import sha256;
from multiprocessing import cpu_count;
from os import getloadavg;

# A global dict to contain the config.
config = {
	'CONFIG_FILE': '/config/load-balancer.conf',
	'CPU_CORES': cpu_count(),
	'SEND_HEARTBEATS': True
}

# The table that stores the server list, and their respective loads.
server_table = dict();

def parse_config(filename):
	"""Load the key / value pairs from the file filename, into the global config dict."""
	config = dict();
	f = open(filename, 'r');

	for line in f.readlines():
		try:
			line = line[0:line.index('#')];
		except:
			pass;

		line = line.strip().split('=');

		if (len(line) < 2):
			continue;

		key = line[0].strip();
		val = line[1].strip();

		if (val.find(',') >= 0):
			val = val.split(',');
			for i in range(len(val)):
				val[i] = val[i].strip();

		config[key] = val;
	f.close();

	return config;

def get_netload():
	"""Returns the current txbytes for the interface, read from /proc"""
	try:
		f = open('/proc/net/dev', 'r');
		lines = f.readlines();
		f.close();
	except:
		return;

	for line in lines:
		if (line[:line.index(':')].strip() == config['NETLOAD_IFACE']):
			return int(line[line.find(':'):].split()[9]);

def main():
	argparser = argparse.ArgumentParser(description = 'CodeFire load balancer daemon.');
	argparser.add_argument('--config');
	args = argparser.parse_args();

	if (args.config):
		config.update(parse_config(args.config));
	else:
		config['CONFIG_FILE'] = str(parse_config('/etc/codefire')['DATASTORE'] + config['CONFIG_FILE']);

	print(config);

	try:
		for ip in config['CODEFIRE_WEB_IPS']:
			server_table[ip] = None;
	except:
		exit(-1);

	# Free up some ram.
	del cpu_count;

if __name__ == '__main__':
	main();
