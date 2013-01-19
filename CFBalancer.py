import argparse;
from hashlib import sha256;
from multiprocessing import cpu_count;
from os import getloadavg;
from socket import socket, AF_INET, SOCK_DGRAM;

from twisted.internet.protocol import DatagramProtocol;
from twisted.internet import reactor, task;

# Globals
netload = None;

# A global dict to contain the config.
config = {
	'CONFIG_FILE': '/configs/loadbalancer/lb.conf',
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

def get_netload(old_netload):
	"""Returns the current txbytes for the interface, read from /proc"""
	try:
		f = open('/proc/net/dev', 'r');
		lines = f.readlines();
		f.close();
	except:
		return;

	for line in lines:
		if (line[:line.find(':')].strip() == config['NETLOAD_INTERFACE']):
			return int(line[line.find(':'):].split()[9]) - old_netload;

class Heartbeat(DatagramProtocol):
	"""Monitors for heartbeats, and updates the server_table with each one."""
	def datagramReceived(self, data, (host, port)):
		if (data[:64] == sha256(data[64:] + config['SHARED_SECRET']).hexdigest()):	# If the security hash matches...
			server_table[host] = [0, data[64:]]		# ...update the server table with the heartbeat data.

def send_heartbeats():
	global netload;

	print(server_table);

	# Build the heartbeat
	netload = get_netload(netload);
	if (config['SEND_HEARTBEATS']):
		heartbeat = str(config['NODE_DL_CNAME'] + ',' + str(getloadavg()[0] / config['CPU_CORES']) + ',' + str(netload));

	for ip in server_table.copy():				# For every server ip we know about...
		# ...send our heartbeat to the host...
		if (heartbeat):
			socket(AF_INET, SOCK_DGRAM).sendto(sha256(heartbeat + config['SHARED_SECRET']).hexdigest() + heartbeat, (ip, int(config['HEARTBEAT_PORT'])));

		# ...and update the server table.
		if (server_table[ip]):				# If the ip is in the server_table still...
			if (server_table[ip][0] > 10):  # If it's been more than 10 seconds since we got a heartbeat...
				del server_table[ip];					# ...delete the host from server_table.
			else:
				server_table[ip][0] += 1;	# Otherwise, increase the count since we last heard from them.

def main():
	argparser = argparse.ArgumentParser(description = 'CodeFire load balancer daemon.');
	argparser.add_argument('--config');
	args = argparser.parse_args();

	if (args.config):
		config.update(parse_config(args.config));
	else:
		config['CONFIG_FILE'] = str(parse_config('/etc/codefire')['DATASTORE'] + config['CONFIG_FILE']);

	try:
		for ip in config['CODEFIRE_WEB_IPS']:
			server_table[ip] = None;
	except:
		exit(-1);

	# Free up some ram.
	global cpu_count;
	del cpu_count;

	# Now start the real work.
	reactor.listenUDP(int(config['HEARTBEAT_PORT']), Heartbeat());
	task.LoopingCall(send_heartbeats).start(int(config['HEARTBEAT_INTERVAL']) / 1000.0);
	reactor.run()

if __name__ == '__main__':
	main();
