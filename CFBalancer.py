import argparse;
import multiprocessing;
from hashlib import sha256;
from os import getloadavg;

from twisted.internet.protocol import DatagramProtocol, Protocol, Factory;
from twisted.internet import reactor, task;

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

def send_heartbeats(pipe, config):
		"""Send out heartbeats, to all hosts."""
		from time import sleep;
		from socket import socket, AF_INET, SOCK_DGRAM;

		netload = 0;
		txbytes = 0;

		while (True):
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

			sleep(int(config['HEARTBEAT_INTERVAL']) / 1000.0);

class Heartbeat(DatagramProtocol):
	"""Heartbeat Handler: Monitors for heartbeats, and updates the server_table when one is received."""
	def __init__(self, cfbalancer):
		self.balancer = cfbalancer;

	def datagramReceived(self, data, (host, port)):
		if (data[:64] == sha256(data[64:] + self.balancer.config['SHARED_SECRET']).hexdigest()):	# If the security hash matches...
			self.balancer.server_table[host] = [0, data[64:]]										# ...update the server table.

class Control(Protocol):
	"""Control socket handler."""
	def __init__(self, cfbalancer):
		self.balancer = cfbalancer;

	def dataReceived(self, data):
		self.transport.write(self.balancer.call_api(data[:(data.find(':') if (data.find(':') > 0) else len(data))].upper()));
		self.transport.loseConnection();

class ControlFactory(Factory):
	"""Control socket factory, to spawn Control objects for each request."""
	def __init__(self, cfbalancer):
		self.balancer = cfbalancer;

	def buildProtocol(self, addr):
		return Control(self.balancer);

class CFBalancer(object):
	def __init__(self, config = None):
		self.heartbeat_pipe = None;
		self.config = dict();			# A dict for the local config
		self.server_table = dict();		# A dict for the server loads

		self.API = dict({				# A dict for the api functions
			'L': self.api_list,
			'S': self.api_list,
			'I': self.api_ignore,
			'U': self.api_unignore,
			'P': self.api_pause,
			'R': self.api_resume
		});

		self.config.update(config);

		# Add the default hosts.
		try:
			if type(self.config['CODEFIRE_WEB_IPS']) == list:
				for ip in self.config['CODEFIRE_WEB_IPS']:
					if (ip):
						self.server_table[ip] = None;
			elif type(self.config['CODEFIRE_WEB_IPS']) == str:
				self.server_table[config['CODEFIRE_WEB_IPS']] = None;
		except:
			exit(-1);

		# Start the heartbeat process.
		rxpipe, self.heartbeat_pipe = multiprocessing.Pipe(False);
		multiprocessing.Process(target = send_heartbeats, args = [rxpipe, self.config]).start();
		del rxpipe;				# Free up some ram.

		# Register the listeners and start the event loop.
		reactor.listenUDP(int(self.config['HEARTBEAT_PORT']), Heartbeat(self));
		reactor.listenTCP(int(self.config['CONTROL_PORT']), ControlFactory(self));
		task.LoopingCall(self.update_server_table).start(int(self.config['HEARTBEAT_INTERVAL']) / 1000.0);
		reactor.run()

	def update_server_table(self):
		"""Update the server_table, and send out heartbeats."""
		if (self.config['VERBOSE']):
			print(self.server_table);

		if (self.config['SEND_HEARTBEATS']):
			# Send hosts list to the heartbeat subprocess
			self.heartbeat_pipe.send({'hosts': self.server_table.keys(), 'payload': ('IGNORE' if self.config['IGNORE'] else False)});

		# Cleanup the server_table
		for ip in self.server_table.keys():				# For every ip in the server table...
			if (self.server_table[ip]):					# If the ip is still in the server_table...
				# If it's not one of the original servers, and it's been more than SERVER_TIMEOUT seconds since we got a heartbeat...
				if (self.server_table[ip][0] > self.config['SERVER_TIMEOUT']):
					del self.server_table[ip];			# ...delete the host from server_table.
				else:									# Otherwise...
					self.server_table[ip][0] += 1;		# ...increase the count since we last heard from them.

	def call_api(self, api_method, *args):
		if (api_method in self.API):
			return str(self.API[api_method](*args));

	def api_list(self):
		loads = str();
		for ip in self.server_table:
			loads += str(self.server_table[ip][0]) + "," + self.server_table[ip][1];
			if (self.server_table[ip][1][:self.server_table[ip][1].index(",")] == self.config['NODE_DL_CNAME']):
				loads += ",*";
			loads += "\r\n";
		return loads;

	def api_ignore(self):
		self.config['IGNORE'] = True;
		return self.config['IGNORE'];

	def api_unignore(self):
		self.config['IGNORE'] = False;
		return self.config['IGNORE'];

	def api_pause(self):
		self.config['SEND_HEARTBEATS'] = False;
		return self.config['SEND_HEARTBEATS'];

	def api_resume(self):
		self.config['SEND_HEARTBEATS'] = True;
		return self.config['SEND_HEARTBEATS'];

class ApiAction(argparse.Action):		# TODO: Load config in here somehow.
	def __call__(self, parser, args, values, option_string=None):
		try:
			from socket import socket, AF_INET, SOCK_STREAM;

			config = dict({
				'CONFIG_FILE': '/configs/loadbalancer/lb.conf',
				'CPU_CORES': multiprocessing.cpu_count(),
				'DAEMON': False,
				'SERVER_TIMEOUT': 10,
				'IGNORE': False,
				'SEND_HEARTBEATS': True,
				'VERBOSE': False
			});

			if (args.config):
				config['CONFIG_FILE'] = args.config;
			else:
				config['CONFIG_FILE'] = str(parse_config('/etc/codefire')['DATASTORE'] + config['CONFIG_FILE']);

			config.update(parse_config(args.config));

			s = socket(AF_INET, SOCK_STREAM);
			s.connect(('localhost', int(config['CONTROL_PORT'])));
			s.sendall(option_string.translate(None, ' -')[0].upper());
			print(s.recv(4096));
		except:
			print("There was an error.");
			exit(-1);

		exit();

def main():
	"""Initialize everything, and start the event loop."""

	# A dict to contain the config
	config = dict({
		'CONFIG_FILE': '/configs/loadbalancer/lb.conf',
		'CPU_CORES': multiprocessing.cpu_count(),
		'DAEMON': False,
		'SERVER_TIMEOUT': 10,
		'IGNORE': False,
		'SEND_HEARTBEATS': True,
		'VERBOSE': False
	});

	# Handle the argument parsing.
	argparser = argparse.ArgumentParser(description = 'Team CodeFire load reporting daemon.');

	argparser.add_argument('-c', '--config', help = 'the config file to use');
	argparser.add_argument('-v', '--verbose', help = 'enable verbose output', action = 'store_true');

	group = argparser.add_mutually_exclusive_group();
	group.add_argument('-d', '--daemon', help = 'enable daemon mode', action = 'store_true');
	group.add_argument('-i', '--ignore', help = 'set our heartbeat payload to "IGNORE"', action = ApiAction, nargs = 0);
	group.add_argument('-l', '--list', help = 'print the server table in a pretty way', action = ApiAction, nargs = 0);
	group.add_argument('-p', '--pause', help = 'pause heartbeats', action = ApiAction, nargs = 0);
	group.add_argument('-r', '--resume', help = 'resume heartbeats', action = ApiAction, nargs = 0);
	group.add_argument('-s', '--show', help = 'same as --list', action = ApiAction, nargs = 0);
	group.add_argument('-u', '--unignore', help = 'inverse of --ignore', action = ApiAction, nargs = 0);

	args = argparser.parse_args();

	# Load the args and config file.
	config['CONFIG_FILE'] = args.config if args.config else	str(parse_config('/etc/codefire')['DATASTORE'] + config['CONFIG_FILE']);
	config['DAEMON'] = args.daemon;
	config['VERBOSE'] = args.verbose;

	try:
		config.update(parse_config(config['CONFIG_FILE']));
	except:
		print("Error parsing config file.");
		exit(-1);

	balancer = CFBalancer(config);

if __name__ == '__main__':
	main();
