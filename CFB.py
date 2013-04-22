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



## Global constants
CONFIG_FILE = '/configs/loadbalancer/lb.conf';			# Default config file



## Globals
config = dict();



## Hooks
def hook_log_updates(server_data, **kwargs):
	print(server_data);

def hook_log_heartbeats(**kwargs):
	print(kwargs);


## ArgParser actions
class ApiAction(argparse.Action):
	 def __call__(self, parser, args, values, option_string = None):
		try:
			from socket import socket, AF_INET, SOCK_STREAM;

			s = socket(AF_INET, SOCK_STREAM);
			s.connect(('localhost', int(config['CONTROL_PORT'])));
			s.sendall(option_string.translate(None, ' -')[0].upper());
			output = str();
			while True:
				data, addr = s.recv(1024);
				if (not data):
					break;
				output += data;
			print(output);
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
	#balancer.register_hook('pre-update-server-table', hook_log_updates);
	#balancer.register_hook('pre-send-heartbeat', hook_log_heartbeats);

	# Start the balancer
	balancer.start();

if __name__ == '__main__':
	main();
