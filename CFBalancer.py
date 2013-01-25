import argparse;
import multiprocessing;
from hashlib import sha256;
from os import getloadavg;
from socket import socket, AF_INET, SOCK_DGRAM;

from twisted.internet.protocol import DatagramProtocol, Protocol, Factory;
from twisted.internet import reactor, task;

# Globals
heartbeat_pipe = None;

# A global dict to contain the config.
config = {
	'CONFIG_FILE': '/configs/loadbalancer/lb.conf',
	'CPU_CORES': multiprocessing.cpu_count(),
	'DAEMON': False,
	'IGNORE': False,
	'SEND_HEARTBEATS': True,
	'VERBOSE': True
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

def send_heartbeats(pipe):
	"""Send out heartbeats, to all hosts."""
	from time import sleep;

	netload = 0;
	txbytes = 0;

	while (True):
		sleep(1);
		try:
			f = open('/proc/net/dev', 'r');
			lines = f.readlines();
			f.close();
		except:
			return;

		for line in lines:
			if (line[:line.find(':')].strip() == config['NETLOAD_INTERFACE']):
				netload = int(line[line.find(':'):].split()[9]) - txbytes;
				txbytes += netload;

		if (pipe.poll()):
			data = pipe.recv();

			# Build the heartbeat.
			if (data['payload']):
				heartbeat = data['payload'];
			else:
				heartbeat = str(config['NODE_DL_CNAME'] + ',' + str(getloadavg()[0] / config['CPU_CORES']) + ',' + str(netload));

			# And send it to every host in the list.
			try:
				for host in data['hosts']:
					socket(AF_INET, SOCK_DGRAM).sendto(sha256(heartbeat + config['SHARED_SECRET']).hexdigest() + heartbeat, (host, int(config['HEARTBEAT_PORT'])));
			except:
				pass;

def update_server_table():
	"""Update the server_table, and send out heartbeats."""
	if (config['VERBOSE']):
		print(server_table);

	if (config['SEND_HEARTBEATS']):
		# Send hosts list to the heartbeat subprocess.
		heartbeat_pipe.send({'hosts': server_table.keys(), 'payload': ('IGNORE' if config['IGNORE'] else False)});

	for ip in server_table.keys():		# For every ip in the server table...
		if (server_table[ip]):				# If the ip is still in the server_table...
			if (server_table[ip][0] > 10):  # If it's been more than 10 seconds since we got a heartbeat...
				del server_table[ip];			# ...delete the host from server_table.
			else:							# Otherwise...
				server_table[ip][0] += 1;	# ...increase the count since we last heard from them.

class Heartbeat(DatagramProtocol):
	"""Heartbeat Handler: Monitors for heartbeats, and updates the server_table when one is received."""
	def datagramReceived(self, data, (host, port)):
		if (data[:64] == sha256(data[64:] + config['SHARED_SECRET']).hexdigest()):	# If the security hash matches...
			server_table[host] = [0, data[64:]]		# ...update the server table with the heartbeat data.

class Control(Protocol):
	"""Control socket handler."""
	def dataReceived(self, data):
		loads = str();
		for ip in server_table:
			loads += str(server_table[ip][0]) + "," + server_table[ip][1];
			if (server_table[ip][1][:server_table[ip][1].index(",")] == config['NODE_DL_CNAME']):
				loads += ",*";
			loads += "\r\n";

		self.transport.write(loads);
		self.transport.loseConnection();

class ControlFactory(Factory):
	"""Control socket factory, to spawn Control objects for each request."""
	protocol = Control;

def main():
	"""Initialize everything, and start the event loop."""
	global heartbeat_pipe;

	# Handle the argument parser.
	argparser = argparse.ArgumentParser(description = 'Team CodeFire load reporting daemon.');

	argparser.add_argument('-c', '--config', help = 'the config file to use');
	argparser.add_argument('-v', '--verbose', help = 'enable verbose output', action = 'store_true');

	group = argparser.add_mutually_exclusive_group();
	group.add_argument('-d', '--daemon', help = 'enable daemon mode', action = 'store_true');
	group.add_argument('-i', '--ignore', help = 'set our heartbeat payload to "IGNORE"', action = 'store_true');
	group.add_argument('-l', '--list', help = 'print the server table in a pretty way', action = 'store_true');
	group.add_argument('-p', '--pause', help = 'pause heartbeats', action = 'store_true');
	group.add_argument('-r', '--resume', help = 'resume heartbeats', action = 'store_true');
	group.add_argument('-s', '--show', help = 'same as --list', action = 'store_true');
	group.add_argument('-u', '--unignore', help = 'inverse of --ignore', action = 'store_true');

	args = argparser.parse_args();
	print(args);

	# Load the configs.
	if (args.config):
		config.update(parse_config(args.config));
	else:
		config['CONFIG_FILE'] = str(parse_config('/etc/codefire')['DATASTORE'] + config['CONFIG_FILE']);

	# Add the default hosts.
	try:
		if type(config['CODEFIRE_WEB_IPS']) == list:
			for ip in config['CODEFIRE_WEB_IPS']:
				if (ip):
					server_table[ip] = None;
		elif type(config['CODEFIRE_WEB_IPS']) == str:
			server_table[config['CODEFIRE_WEB_IPS']] = None;
	except:
		exit(-1);

	# Start the heartbeat process.
	rxpipe, heartbeat_pipe = multiprocessing.Pipe(False);
	multiprocessing.Process(target = send_heartbeats, args = [rxpipe]).start();

	# Free up some ram.
	del rxpipe;

	# Register the listeners and start the event loop.
	reactor.listenUDP(int(config['HEARTBEAT_PORT']), Heartbeat());
	reactor.listenTCP(int(config['CONTROL_PORT']), ControlFactory());
	task.LoopingCall(update_server_table).start(int(config['HEARTBEAT_INTERVAL']) / 1000.0);
	reactor.run()

if __name__ == '__main__':
	main();
