from hashlib import sha256;
from multiprocessing import cpu_count;
from os import getloadavg;
from psutil import network_io_counters as network;
from threading import Thread;
from time import sleep, mktime, gmtime;
import socket;

CONFIG = {
	"CODEFIRE_CONFIG": '/etc/codefire',
	"CONFIG_FILE": '/config/load-balancer.conf',
	"CPU_CORES": cpu_count()
}

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
	f.close();
	return;

def get_netload():
    f = open("/proc/net/dev", "r")
    try:
        lines = f.readlines()
    finally:
        f.close()

    for line in lines:
		if (line[:line.find(":")].strip() == CONFIG['NETLOAD_IFACE']):
			return int(line[line.find(":"):].split()[9]);

def heartbeat_listener():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
	s.bind((CONFIG['NODE_PRIVATE_IP'], int(CONFIG['HEARTBEAT_PORT'])));
	while True:
		packet = s.recvfrom(128);
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
		Thread(target = webservice_handler, args = [sock]).start();
	return;

def webservice_handler(s):
	data = str();
	for ip in server_table:
		if (server_table[ip]):
			if (server_table[ip][1].split(",")[0] == CONFIG['NODE_DL_CNAME']):
				data += str(server_table[ip][0]) + "," + server_table[ip][1] + ",*\r\n";
			else:
				data += str(server_table[ip][0]) + "," + server_table[ip][1] + "\r\n";
	s.send(data);
	return

def main():
	load_config(CONFIG['CODEFIRE_CONFIG']);
	load_config(CONFIG['DATASTORE'] + CONFIG['CONFIG_FILE']);

	# CLEANUP -- Everything below this could use some cleaning.
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

	# Not strictly necessary, but it frees up a bit of ram.
	#del t, cpu_count, load_config;

	while True:
		print("Main: " + str(server_table));
		netload = get_netload();
		sleep(int(CONFIG['HEARTBEAT_INTERVAL']) / 1000.0);			# sleep in ms
		heartbeat = str(CONFIG['NODE_DL_CNAME'] + "," + str(getloadavg()[0] / CONFIG['CPU_CORES']) + "," + str(get_netload() - netload));
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
