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
import argparse;

import CFBalancer;



## Constants
CONFIG_FILE = '/configs/loadbalancer/lb.conf';			# Default config file



## Globals
config = dict();



## Plugins
def plugin_list(config, server_table, **kwargs):
	loads = str();
	for ip in server_table:
		loads += str(server_table[ip][0]) + "," + server_table[ip][1];
		if (server_table[ip][1][:server_table[ip][1].index(",")] == config['NODE_DL_CNAME']):
			loads += ",*";
		loads += "\r\n";
	return loads;

def plugin_ignore(config, **kwargs):
	config['IGNORE'] = True;
	return config['IGNORE'];

def plugin_unignore(config, **kwargs):
	config['IGNORE'] = False;
	return config['IGNORE'];

def plugin_pause(config, **kwargs):
	config['SEND_HEARTBEATS'] = False;
	return config['SEND_HEARTBEATS'];

def plugin_resume(config, **kwargs):
	config['SEND_HEARTBEATS'] = True;
	return config['SEND_HEARTBEATS'];



## Hooks



## ArgParser actions
class ApiAction(argparse.Action):
	 def __call__(self, parser, args, values, option_string=None):
		try:
			from socket import socket, AF_INET, SOCK_STREAM;

			s = socket(AF_INET, SOCK_STREAM);
			s.connect(('localhost', int(config['CONTROL_PORT'])));
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
				config.update(CFBalancer.parse_config(config_file));
				args.config = True;
			except:
				print("Error parsing config file: " + config_file);



def main():								# CLEANUP
	"""Initialize everything, and start the event loop."""
	# Handle arguments
	argparser = argparse.ArgumentParser(description = 'Team CodeFire load reporting daemon.');

	# Setup the config arguments
	argparser.add_argument('-c', '--config', help = 'the config file to use', action = ConfigAction, nargs = '+');
	argparser.add_argument('-v', '--verbose', help = 'enable verbose output', action = 'store_true');

	# Parse and handle config arguments
	config_args, args = argparser.parse_known_args();
	if not config_args.config:
		config.update(parse_config(str(parse_config('/etc/codefire')['DATASTORE'])));
	config['VERBOSE'] = config_args.verbose;

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
	config['DAEMON'] = args.daemon;


	balancer = CFBalancer.CFBalancer(config);

	# Register the default plugins
	balancer.register_plugins({
		'L': plugin_list,
		'S': plugin_list,
		'I': plugin_ignore,
		'U': plugin_unignore,
		'P': plugin_pause,
		'R': plugin_resume
	});

	# Start the balancer
	balancer.start();

if __name__ == '__main__':
	main();
