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
import gevent;
import json;

from gevent import sleep, socket;
from gevent.server import StreamServer;
from hashlib import sha256;
from multiprocessing import cpu_count;
from os import getloadavg;



## Global constants
CPU_CORES = cpu_count();



## Utility (class independant) functions
def parse_config(filename):
		"""Load the key / value pairs from the file filename, into the global config dict."""
		config = dict();
		f = open(filename, 'r');

		for line in f.readlines():
			try:
				line = line[0:line.index('#')];
				line = line[0:line.index(';')];
			except:
				pass;

			line = line.strip().split('=');

			if (len(line) < 2):
				continue;

			key = line[0].strip();
			val = line[1].strip();

			if (val[0] == '[' and val[-1] == ']'):
				val = val[1:-1].split(',');
				for i in range(len(val)):
					val[i] = val[i].strip();

			config[key] = val;
		f.close();

		return config;



## Default plugins
def plugin_list(balancer, server_table, **kwargs):
	return json.dumps(server_table);

def plugin_ignore(balancer, **kwargs):
	balancer.config['IGNORE'] = True;
	return ("Set to IGNORE." if balancer.config['IGNORE'] == True else "Error.");

def plugin_unignore(balancer, **kwargs):
	balancer.config['IGNORE'] = False;
	return ("Set to not IGNORE." if balancer.config['IGNORE'] == False else "Error.");

def plugin_pause(balancer, **kwargs):
	balancer.config['SEND_HEARTBEATS'] = False;
	return ("Heartbeats paused." if balancer.config['IGNORE'] == False else "Error.");

def plugin_resume(balancer, **kwargs):
	balancer.config['SEND_HEARTBEATS'] = True;
	return ("Heartbeats resumed." if balancer.config['IGNORE'] == True else "Error.");



## The big show
class CFBalancer(object):
	def __init__(self, config = None):
		self.__hooks = dict();				# A dict for the hooks.  Currently predefined, will be dynamic in the future, hopefully.
		self.__netload = 0;
		self.__server_table = dict();		# A dict for the server loads

		self.__plugins = dict({				# A dict for the plugins, these are called by the ConrtolProtocol socket, or command line flags.
			'L': plugin_list,
			'S': plugin_list,
			'I': plugin_ignore,
			'U': plugin_unignore,
			'P': plugin_pause,
			'R': plugin_resume
		});

		self.config = dict({				# A dict for the balancer configuration
			'DAEMON': False,
			'SERVER_TIMEOUT': 10,
			'IGNORE': False,
			'SEND_HEARTBEATS': True,
			'VERBOSE': False
		});

		if (config):
			self.config.update(config);

	def __debug(self, msg):
		if (self.config['VERBOSE']):
			print(msg);

	def __heartbeat_dispatcher(self):
		"""Send out heartbeats, to all hosts."""
		while True:
			heartbeat = None;

			self.__debug(self.__server_table);

			if (self.config['SEND_HEARTBEATS']):
				# Build the heartbeat.
				if (self.config['IGNORE']):
					heartbeat = str(self.config['NODE_DL_HOSTNAME'] + ',IGNORE,IGNORE');
				else:
					heartbeat = str(self.config['NODE_DL_HOSTNAME'] + ',' + str(getloadavg()[0] / CPU_CORES) + ',' + str(self.__netload));

			# And send it to every host in the list.
			for host in self.__server_table.keys():
				try:
					if (self.__server_table[host]):
						if (self.__server_table[host]['yet_to_be_named_metric'] > int(self.config['SERVER_TIMEOUT'])):		# If it's been less than SERVER_TIMEOUT seconds since we got a heartbeat...
							del self.__server_table[host];												# ...delete the host from server_table.
							continue;
						else:																			# Otherwise...
							self.__server_table[host]['yet_to_be_named_metric'] += 1;					# ...update the count since we last heard from them.

					if (heartbeat):
						socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(sha256(heartbeat + self.config['SHARED_SECRET']).hexdigest() + heartbeat, (host, int(self.config['HEARTBEAT_PORT'])));
				except Exception as err:
					self.__debug("Error sending heartbeat: " + str(err));

			sleep(int(self.config['HEARTBEAT_INTERVAL']) / 1000.0);

	def __update_netload(self):
		# FIX:	Maybe put this in a separate thread / process, for more precice timing and therefore numbers.
		#		Also, this will take all of the file IO every second out of the process, removing a delay when
		#		handling requests during that time.
		txbytes = 0;

		while True:
			try:
				f = open('/proc/net/dev', 'r');
				lines = f.readlines();
				f.close();
			except:
				return False;

			for line in lines:
				if (line[:line.find(':')].strip() == self.config['NETLOAD_INTERFACE']):
					self.__netload = int(line[line.find(':'):].split()[9]) - txbytes;
					txbytes += self.__netload;

			sleep(1);

	def __heartbeat_handler(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
		sock.bind((self.config['NODE_PRIVATE_IP'], int(self.config['HEARTBEAT_PORT'])));

		while True:
			heartbeat, addr = sock.recvfrom(4096);

			if (heartbeat[:64] == sha256(heartbeat[64:] + self.config['SHARED_SECRET']).hexdigest()):	# If the security hash matches...
				server_data = heartbeat[64:].split(',');						# ...update the server table.
				self.__server_table[addr[0]] = dict({
					'hostname': server_data[0],
					'cpu_load': server_data[1],
					'net_load': server_data[2],
					'yet_to_be_named_metric': 0
				});

	def __control_handler(self, sock, (addr, _)):
		payload = sock.recv(1024);
		colon = payload.find(':');				# I hate doing it this way, but it saves cycles

		if (colon >= 0):
			sock.send(self.run_plugin(payload[:colon], payload[colon:].strip()));
		else:
			sock.send(self.run_plugin(payload.strip().upper()));

	def start(self):
		# Add the default hosts.
		for ip in self.config['CODEFIRE_WEB_IPS']:
			if (ip):
				self.__server_table[ip] = None;

		# Initialize the control server
		control_listener = StreamServer((self.config['NODE_PRIVATE_IP'], int(self.config['CONTROL_PORT'])), self.__control_handler);

		# Create and start all the Greenlets
		gevent.joinall([
			gevent.spawn(self.__update_netload),
			gevent.spawn(self.__heartbeat_handler),
			gevent.spawn(control_listener.start),
			gevent.spawn(self.__heartbeat_dispatcher),
		]);

	def load_config(self, filename):
		self.config.update(parse_config(filename));

	def register_plugin(self, name, action):
		self.__plugins[name] = action;

	def register_plugins(self, plugins):
		self.__plugins.update(plugins);

	def run_plugin(self, plugin, args = None):
		kwargs = dict({
			'args': args,
			'balancer': self,
			'server_table': self.__server_table
		});

		self.__debug("Running plugin: '" + str(plugin) + "' with arguments: '" + str(args) + "'");

		if (plugin in self.__plugins):
			return str(self.__plugins[plugin](**kwargs));

	def register_hook(self, hook_type, hook):
		if (hook_type not in self.__hooks):
			self.__hooks[hook_type] = list();
		self.__hooks[hook_type].append(hook);

	def run_hooks(self, hook_type, *args, **kwargs):
		if (hook_type in self.__hooks):
			for hook in self.__hooks[hook_type]:
				hook(*args, **kwargs);
