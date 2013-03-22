import argparse;
from hashlib import sha256;
import multiprocessing;
from os import getloadavg;

from twisted.internet.protocol import DatagramProtocol, Protocol, Factory;
from twisted.internet import reactor, task;

# Constants
CONFIG_FILE = '/configs/loadbalancer/lb.conf';			# Default config file

# Globals
_api = dict();			# A dict for the api functions

_config = dict({
	'CPU_CORES': multiprocessing.cpu_count(),
	'DAEMON': False,
	'SERVER_TIMEOUT': 10,
	'IGNORE': False,
	'SEND_HEARTBEATS': True,
	'VERBOSE': False
});

_heartbeat_rxpipe, _heartbeat_pipe = multiprocessing.Pipe(False);
_server_table = dict();			# A dict for the server loads



class Heartbeat(DatagramProtocol):
	"""Heartbeat Handler: Monitors for heartbeats, and updates the server_table when one is received."""
	def datagramReceived(self, data, (host, port)):
		if (data[:64] == sha256(data[64:] + _config['SHARED_SECRET']).hexdigest()):	# If the security hash matches...
			_server_table[host] = [0, data[64:]];										# ...update the server table.

class Control(Protocol):
	"""Control socket handler."""
	def dataReceived(self, data):
		self.transport.write(call_api(data[:(data.find(':') if (data.find(':') > 0) else len(data))].upper()));
		self.transport.loseConnection();

class ControlFactory(Factory):
	"""Control socket factory, to spawn Control objects for each request."""
	protocol = Control;

class ApiAction(argparse.Action):
	def __call__(self, parser, args, values, option_string=None):
		try:
			from socket import socket, AF_INET, SOCK_STREAM;

			s = socket(AF_INET, SOCK_STREAM);
			s.connect(('localhost', int(_config['CONTROL_PORT'])));
			s.sendall(option_string.translate(None, ' -')[0].upper());
			print(s.recv(4096));
		except:
			print("Error running API command: " + option_string.translate(None, ' -'));
			exit(-1);

		exit();

class ConfigAction(argparse.Action):
	def __call__(self, parser, args, values, option_string=None):
		for config_file in values:
			try:
				_config.update(parse_config(config_file));
				args.config = True;
			except:
				print("Error parsing config file: " + config_file);



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

def register_plugin(name, action):
	_api[name] = action;

def register_plugins(plugins):
	_api.update(plugins);

def start_balancer():					# CLEANUP
	# Add the default hosts.
	try:
		if type(_config['CODEFIRE_WEB_IPS']) == list:
			for ip in _config['CODEFIRE_WEB_IPS']:
				if (ip):
					_server_table[ip] = None;
		elif type(_config['CODEFIRE_WEB_IPS']) == str:
			_server_table[_config['CODEFIRE_WEB_IPS']] = None;
	except:
		exit(-1);

	# Start the heartbeat process.
	multiprocessing.Process(target = send_heartbeats, args = [_heartbeat_rxpipe, _config]).start();

	# Register the listeners and start the event loop.
	reactor.listenUDP(int(_config['HEARTBEAT_PORT']), Heartbeat());
	reactor.listenTCP(int(_config['CONTROL_PORT']), ControlFactory());
	task.LoopingCall(update_server_table).start(int(_config['HEARTBEAT_INTERVAL']) / 1000.0);
	reactor.run()

def update_server_table():				# CLEANUP
	"""Update the server_table, and send out heartbeats."""
	if (_config['VERBOSE']):
		print(_server_table);

	if (_config['SEND_HEARTBEATS']):
		# Send hosts list to the heartbeat subprocess
		_heartbeat_pipe.send({'hosts': _server_table.keys(), 'payload': ('IGNORE' if _config['IGNORE'] else False)});

	# Cleanup the server_table
	for ip in _server_table.keys():				# For every ip in the server table...
		if (_server_table[ip]):					# If the ip is still in the server_table...
												# If it's not one of the original servers, and it's been more than SERVER_TIMEOUT seconds since we got a heartbeat...
			if (_server_table[ip][0] > _config['SERVER_TIMEOUT']):
				del _server_table[ip];			# ...delete the host from server_table.
			else:								# Otherwise...
				_server_table[ip][0] += 1;		# ...increase the count since we last heard from them.

def send_heartbeats(pipe, config):		# CLEANUP
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
				if (line[:line.find(':')].strip() == _config['NETLOAD_INTERFACE']):
					netload = int(line[line.find(':'):].split()[9]) - txbytes;
					txbytes += netload;

			data = pipe.recv();

			# Build the heartbeat.
			if (data['payload']):
				heartbeat = data['payload'];
			else:
				heartbeat = str(_config['NODE_DL_CNAME'] + ',' + str(getloadavg()[0] / _config['CPU_CORES']) + ',' + str(netload));

			# And send it to every host in the list.
			try:
				for host in data['hosts']:
					socket(AF_INET, SOCK_DGRAM).sendto(sha256(heartbeat + _config['SHARED_SECRET']).hexdigest() + heartbeat, (host, int(_config['HEARTBEAT_PORT'])));
			except:
				pass;

			sleep(1);

def call_api(api_method, *args):
	if (api_method in _api):
		return str(_api[api_method](*args));

def api_list():
	loads = str();
	for ip in _server_table:
		loads += str(_server_table[ip][0]) + "," + _server_table[ip][1];
		if (_server_table[ip][1][:_server_table[ip][1].index(",")] == _config['NODE_DL_CNAME']):
			loads += ",*";
		loads += "\r\n";
	return loads;

def api_ignore():
	_config['IGNORE'] = True;
	return _config['IGNORE'];

def api_unignore():
	_config['IGNORE'] = False;
	return _config['IGNORE'];

def api_pause():
	_config['SEND_HEARTBEATS'] = False;
	return _config['SEND_HEARTBEATS'];

def api_resume():
	_config['SEND_HEARTBEATS'] = True;
	return _config['SEND_HEARTBEATS'];



def main():								# CLEANUP
	"""Initialize everything, and start the event loop."""

	# Register the default plugins
	register_plugins({
		'L': api_list,
		'S': api_list,
		'I': api_ignore,
		'U': api_unignore,
		'P': api_pause,
		'R': api_resume
	})

	# Handle arguments
	argparser = argparse.ArgumentParser(description = 'Team CodeFire load reporting daemon.');

	# Setup the config arguments
	argparser.add_argument('-c', '--config', help = 'the config file to use', action = ConfigAction, nargs = '+');
	argparser.add_argument('-v', '--verbose', help = 'enable verbose output', action = 'store_true');

	# Parse and handle config arguments
	config_args, args = argparser.parse_known_args();
	if not config_args.config:
		_config.update(parse_config(str(parse_config('/etc/codefire')['DATASTORE']));
	_config['VERBOSE'] = config_args.verbose;

	# Setup the rest of the arguments
	group = argparser.add_mutually_exclusive_group();
	group.add_argument('-d', '--daemon', help = 'enable daemon mode', action = 'store_true');
	group.add_argument('-i', '--ignore', help = 'set our heartbeat payload to "IGNORE"', action = ApiAction, nargs = 0);
	group.add_argument('-l', '--list', help = 'print the server table in a pretty way', action = ApiAction, nargs = 0);
	group.add_argument('-p', '--pause', help = 'pause heartbeats', action = ApiAction, nargs = 0);
	group.add_argument('-r', '--resume', help = 'resume heartbeats', action = ApiAction, nargs = 0);
	group.add_argument('-s', '--show', help = 'same as --list', action = ApiAction, nargs = 0);
	group.add_argument('-u', '--unignore', help = 'inverse of --ignore', action = ApiAction, nargs = 0);

	# Parse and handle the rest of the arguments
	args = argparser.parse_args(args);
	_config['DAEMON'] = args.daemon;

	# Start the balancer
	start_balancer();


if __name__ == '__main__':
	main();
