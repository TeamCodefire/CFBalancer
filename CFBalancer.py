from hashlib import sha256;
from multiprocessing import cpu_count;
from os import getloadavg;
from psutil import network_io_counters as network;
from threading import Thread;
from time import sleep;
import socket;

# A global dict to contain the config.
config = {
	"CODEFIRE_CONFIG": '/etc/codefire',
	"CONFIG_FILE": '/config/load-balancer.conf',
	"CPU_CORES": cpu_count(),
	"SEND_HEARTBEATS": True
}

# The table that stores the server list, and their respective loads.
server_table = dict();

def load_config(filename):
	# Load the key / value pairs from the file filename, into the global config dict.
	f = open(filename, "r");

	for line in f.readlines():
		line = line.strip();
		if (line.find("#") == 0):
			continue;
		line = line.split("=");
		if (len(line) <= 1):
			continue;
		key = line[0].strip();
		val = line[1].strip();
		if (val.find(",") >= 0):
			val = val.split(",");
		config[key] = val;
	f.close();

	return;

def get_netload():
	# Returns the current txbytes for the interface, read from /proc
	try:
		f = open("/proc/net/dev", "r");
		lines = f.readlines();
		f.close();
	except:
		return;

    for line in lines:
		if (line[:line.find(":")].strip() == config['NETLOAD_IFACE']):
			return int(line[line.find(":"):].split()[9]);

def heartbeat_listener():
	# THREAD: Monitors for heartbeats, and updates the server_table with each one.
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
	s.bind((config['NODE_PRIVATE_IP'], int(config['HEARTBEAT_PORT'])));
	while True:
		packet = s.recvfrom(128);
		# Check if the security hash matches...
		if (packet[0][:64] != sha256(packet[0][64:] + config['SHARED_SECRET']).hexdigest());
			continue;											# ...if it doesnt, discard the packet.
		server_table[packet[1][0]] = [0, packet[0][64:]]		# ...otherwise, update the server_table with the data in the heartbeat data.

def control_listener():
	# THREAD: Listen for control messages, and handle them accordingly.
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
	s.bind((config['NODE_PRIVATE_IP'], int(config['HEARTBEAT_PORT'])));
	s.listen(5);

	while True:
		sock = s.accept()[0];
		packet = s.recv(128)[0];
		if (packet[0][:64] != sha256(packet[0][64:] + config['SHARED_SECRET']).hexdigest()):
			continue;		# If security hash doesn't match, discard the packet.

def webservice_listener():
	# THREAD: Listen for connections to the web service, and spawn a new thread to handle it.
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
	s.bind((config['WEBSERVICE_HOST'], int(config['WEBSERVICE_PORT'])));
	s.listen(10);
	while True:
		sock = s.accept()[0]
		Thread(target = webservice_handler, args = [sock]).start();

def webservice_handler(s):
	# THREAD: Process web service requests, and return the requested data.
	data = str();
	for ip in server_table:
		if (server_table[ip]):
			## CLEANUP -- This could be done in two lines?
			if (server_table[ip][1].split(",")[0] == config['NODE_DL_CNAME']):
				data += str(server_table[ip][0]) + "," + server_table[ip][1] + ",*\r\n";
			else:
				data += str(server_table[ip][0]) + "," + server_table[ip][1] + "\r\n";
	s.send(data);

def main():
	load_config(config['CODEFIRE_CONFIG']);
	load_config(config['DATASTORE'] + config['CONFIG_FILE']);

	## CLEANUP -- Everything below this could use some cleaning.
	if (type(config['CODEFIRE_WEB_IPS']) == list):
		for ip in config['CODEFIRE_WEB_IPS']:
			server_table[ip] = None;
	else:
		server_table[config['CODEFIRE_WEB_IPS']] = None;

	# Start the heartbeat listener
	t = Thread(target = heartbeat_listener);
	t.daemon = True;
	t.start();

	# Start the control listener
	t = Thread(target = control_listener);
	t.daemon = True;
	t.start();

	# Start the webservice listener
	t = Thread(target = webservice_listener);
	t.daemon = True;
	t.start();

	while True:
		print("Main: " + str(server_table));

		# Build the heartbeat, and sleep for 1 second.
		netload = get_netload();
		sleep(int(config['HEARTBEAT_INTERVAL']) / 1000.0);		# Sleep in milliseconds.
		if (config['SEND_HEARTBEATS']):
			heartbeat = str(config['NODE_DL_CNAME'] + "," + str(getloadavg()[0] / config['CPU_CORES']) + "," + str(get_netload() - netload));

		for ip in server_table:				# For every server ip we know about...
			# ...send our heartbeat to the host.
			if (heartbeat):
				s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
				s.sendto(sha256(heartbeat + config['SHARED_SECRET']).hexdigest() + heartbeat, (ip, int(config['HEARTBEAT_PORT'])));

			if (server_table[ip]):				# If the ip is in the server_table still...
				server_table[ip][0] += 1;		# ...increase the count since we last heard from them.
				if (server_table[ip][0] > 10):  # If it's been more than 10 seconds since we got a heartbeat...
					server_table[ip] = None;					# ...delete the host from server_table.

main();
