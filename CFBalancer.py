#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#



## Imports
import multiprocessing;

from hashlib import sha256;
from os import getloadavg;
from twisted.internet.protocol import DatagramProtocol, Protocol, Factory;
from twisted.internet import reactor, task;



## Utility (class independant) functions
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

def _heartbeat_dispatcher(pipe, config):		# CLEANUP
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

			sleep(1);



## Twisted handlers
class _HeartbeatProtocol(DatagramProtocol):
	"""Heartbeat Handler: Monitors for heartbeats, and updates the server_table when one is received."""
	def __init__(self, balancer):
		self.balancer = balancer;

	def datagramReceived(self, data, (host, port)):
		if (data[:64] == sha256(data[64:] + self.balancer.config['SHARED_SECRET']).hexdigest()):	# If the security hash matches...
			server_data = [int(self.balancer.config['SERVER_TIMEOUT']), data[64:]];
			self.balancer.run_hooks('pre-update-server-table', **{'server_data': server_data});
			self.balancer._server_table[host] = server_data;									# ...update the server table.

class _ControlProtocol(Protocol):
	"""Control socket handler."""
	def dataReceived(self, data):
		self.transport.write(self.factory.balancer.run_plugin(data[:(data.find(':') if (data.find(':') > 0) else len(data))].upper()));
		self.transport.loseConnection();

class _ControlFactory(Factory):
	"""Control socket factory, to spawn Control objects for each request."""
	protocol = _ControlProtocol;

	def __init__(self, balancer):
		self.balancer = balancer;



## The big show
class CFBalancer():
	def __init__(self, config = None):
		self.__heartbeat_rxpipe, self.__heartbeat_pipe = multiprocessing.Pipe(False);
		self.__hooks = dict();				# A dict for the hooks.  Currently predefined, will be dynamic in the future, hopefully.
		self.__plugins = dict();			# A dict for the plugins, these are called by the ConrtolProtofol socket, or command line flags.

		self._server_table = dict();			# A dict for the server loads

		self.config = dict({
			'CPU_CORES': multiprocessing.cpu_count(),
			'DAEMON': False,
			'SERVER_TIMEOUT': 10,
			'IGNORE': False,
			'SEND_HEARTBEATS': True,
			'VERBOSE': False
		});

		if (config):
			self.config.update(config);

	def __update_server_table(self):	# CLEANUP
		"""Update the server_table, and send out heartbeats."""
		if (self.config['VERBOSE']):
			print(self._server_table);

		if (self.config['SEND_HEARTBEATS']):
			# Send hosts list to the heartbeat subprocess
			self.__heartbeat_pipe.send({'hosts': self._server_table.keys(), 'payload': ('IGNORE' if self.config['IGNORE'] else False)});

		# Cleanup the server_table
		for ip in self._server_table.keys():							# For every ip in the server table...
			if (self._server_table[ip]):								# If the ip is still in the server_table...
				if (self._server_table[ip][0] == 1):					# If it's not one of the original servers, and it's been more than SERVER_TIMEOUT seconds since we got a heartbeat...
					del self._server_table[ip];							# ...delete the host from server_table.
				else:													# Otherwise...
					self._server_table[ip][0] -= 1;						# ...increase the count since we last heard from them.

	def load_config(self, filename):
		self.config.update(parse_config(filename));

	def start(self):	# CLEANUP
		# Add the default hosts.
		try:
			if type(self.config['CODEFIRE_WEB_IPS']) == list:
				for ip in self.config['CODEFIRE_WEB_IPS']:
					if (ip):
						self._server_table[ip] = None;
			elif type(self.config['CODEFIRE_WEB_IPS']) == str:
				self._server_table[self.config['CODEFIRE_WEB_IPS']] = None;
		except:
			exit(-1);

		# Start the heartbeat process.
		multiprocessing.Process(target = _heartbeat_dispatcher, args = [self.__heartbeat_rxpipe, self.config]).start();

		# Register the listeners and start the event loop.
		reactor.listenUDP(int(self.config['HEARTBEAT_PORT']), _HeartbeatProtocol(self));
		reactor.listenTCP(int(self.config['CONTROL_PORT']), _ControlFactory(self));
		task.LoopingCall(self.__update_server_table).start(int(self.config['HEARTBEAT_INTERVAL']) / 1000.0);
		reactor.run()

	def register_plugin(self, name, action):
		self.__plugins[name] = action;

	def register_plugins(self, plugins):
		self.__plugins.update(plugins);

	def run_plugin(self, plugin, *args, **kwargs):
		if (plugin in self.__plugins):
			return str(self.__plugins[plugin](*args, **kwargs));

	def register_hook(self, hook_type, hook):
		if (hook_type not in self.__hooks):
			self.__hooks[hook_type] = list();
		self.__hooks[hook_type].append(hook);

	def run_hooks(self, hook_type, *args, **kwargs):
		if (hook_type in self.__hooks):
			for hook in self.__hooks[hook_type]:
				hook(*args, **kwargs);
