from hashlib import sha256;
from time import sleep, mktime, gmtime;
from threading import Thread;
from multiprocessing import cpu_count;
from os import getloadavg, path;
import socket;

HEARTBEAT_HOST = "0.0.0.0";
HEARTBEAT_PORT = 33333;
HEARTBEAT_INTERVAL = 1000;
WEBSERVICE_HOST = "localhost";
WEBSERVICE_PORT = 44444;
HOSTNAME = socket.gethostname();
CPU_CORES = cpu_count();
CODEFIRE_CONFIG = '/etc/codefire';
CONFIG_FILE = '/config/load-balancer.conf';
SHARED_SECRET = str();

del cpu_count;		# not strictly necessary, but it frees a bit of ram

server_table = dict();	# This is a local table storing the server loads

def parse_config(filename):
	f = open(filename, "r");
	config = dict();

	for line in f.readlines():
		if (line.strip() and line.strip()[0] == "#"):
			continue;
		line = line.split("=");
		if (len(line) <= 1):
			continue;
		key = line[0].strip();
		val = line[1].strip().split(",");
		if (len(val) <= 1):
			val = val[0];
		config[key] = val;
	return config;

def heartbeat_listener():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
	s.bind((HEARTBEAT_HOST, HEARTBEAT_PORT));
	while True:
		packet = s.recvfrom(512);
		if (packet[0][:64] == sha256(packet[0][64:] + str(mktime(gmtime()))[:-3] + SHARED_SECRET).hexdigest()):
			# A slightly convoluted way to get the data in place, using as little ram as possible.
			server_table[packet[1][0]] = [0, packet[0][64:]];
	return;

def webservice_listener():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
	s.bind((WEBSERVICE_HOST, WEBSERVICE_PORT));
	s.listen(10);
	while True:
		sock = s.accept()[0]
		t = Thread(target = webservice_handler, args = [sock]);
		t.start();
	return;

def webservice_handler(s):
	data = str();
	for ip in server_table:
		if (server_table[ip]):
			data += str(server_table[ip][0]) + "," + server_table[ip][1] + "\r\n";
	s.send(data);
	return

def main():
	config = parse_config(parse_config(CODEFIRE_CONFIG)['DATASTORE'] + CONFIG_FILE);
	global SHARED_SECRET;
	SHARED_SECRET = config['SHARED_SECRET'];

	for ip in config['CODEFIRE_WEB_IPS']:
		server_table[ip] = None;

	t = Thread(target = heartbeat_listener);	# Start the heartbeat listener
	t.daemon = True;
	t.start();
	t = Thread(target = webservice_listener);	# Start the webservice listener
	t.daemon = True;
	t.start();

	while True:
		print("Main: " + str(server_table));
		sleep(HEARTBEAT_INTERVAL / 1000.0);			# sleep in ms
		heartbeat = str(HOSTNAME + "," + str(getloadavg()[0] / CPU_CORES) + "," + "0.14");
		ghost_hosts = list();
		for ip in server_table:
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
			s.sendto(sha256(heartbeat + str(mktime(gmtime()))[:-3] + SHARED_SECRET).hexdigest() + heartbeat, (ip, HEARTBEAT_PORT));
			if (server_table[ip]):
				if (server_table[ip][0] > 10):
					ghost_hosts.append(ip);
					continue;
				server_table[ip][0] += 1;
		for ip in ghost_hosts:
			del server_table[ip];

if (__name__ == "__main__"):
	main();
