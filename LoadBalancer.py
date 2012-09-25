from threading import Thread;
from hashlib import sha256;
from time import sleep, mktime, gmtime;
from os import getloadavg, path;
import socket;

from multiprocessing import cpu_count;

CONFIG = {
	"CODEFIRE_CONFIG": '/etc/codefire',
	"CONFIG_FILE": '/config/load-balancer.conf',
	"CPU_CORES": cpu_count()
}

del cpu_count;		# not strictly necessary, but it frees a bit of ram

server_table = dict();	# This is a local table storing the server loads

def load_config(filename):
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
		CONFIG[key] = val;

def heartbeat_listener():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
	s.bind((CONFIG['NODE_PRIVATE_IP'], int(CONFIG['HEARTBEAT_PORT'])));
	while True:
		packet = s.recvfrom(512);
		if (packet[0][:64] == sha256(packet[0][64:] + str(mktime(gmtime()))[:-3] + CONFIG['SHARED_SECRET']).hexdigest()):
			# A slightly convoluted way to get the data in place, using as little ram as possible.
			server_table[packet[1][0]] = [0, packet[0][64:]];
	return;

def webservice_listener():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
	s.bind((CONFIG['WEBSERVICE_HOST'], int(CONFIG['WEBSERVICE_PORT'])));
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
	load_config(CONFIG['CODEFIRE_CONFIG']);
	load_config(CONFIG['DATASTORE'] + CONFIG['CONFIG_FILE']);

	if (type(CONFIG['CODEFIRE_WEB_IPS']) == list):
		for ip in CONFIG['CODEFIRE_WEB_IPS']:
			server_table[ip] = None;
	else:
		server_table[CONFIG['CODEFIRE_WEB_IPS']] = None;

	t = Thread(target = heartbeat_listener);	# Start the heartbeat listener
	t.daemon = True;
	t.start();
	t = Thread(target = webservice_listener);	# Start the webservice listener
	t.daemon = True;
	t.start();

	while True:
		print("Main: " + str(server_table));
		sleep(int(CONFIG['HEARTBEAT_INTERVAL']) / 1000.0);			# sleep in ms
		heartbeat = str(CONFIG['NODE_DL_CNAME'] + "," + str(getloadavg()[0] / CONFIG['CPU_CORES']) + "," + "0.14");
		ghost_hosts = list();
		for ip in server_table:
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
			s.sendto(sha256(heartbeat + str(mktime(gmtime()))[:-3] + CONFIG['SHARED_SECRET']).hexdigest() + heartbeat, (ip, int(CONFIG['HEARTBEAT_PORT'])));
			if (server_table[ip]):
				if (server_table[ip][0] > 10):
					ghost_hosts.append(ip);
					continue;
				server_table[ip][0] += 1;
		for ip in ghost_hosts:
			del server_table[ip];

if (__name__ == "__main__"):
	main();
