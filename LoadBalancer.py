from time import sleep;
from threading import Thread;
from multiprocessing import cpu_count;
from os import getloadavg;
import socket;

HEARTBEAT_HOST = "0.0.0.0";
HEARTBEAT_PORT = 33333;
HEARTBEAT_INTERVAL = 1000;
WEBSERVICE_HOST = "localhost";
WEBSERVICE_PORT = 44444;
CPU_CORES = cpu_count();
HOSTNAME = socket.gethostname();
HOSTS = ('127.0.0.1', '127.0.0.1');

heartbeat_thread = None;
webservice_thread = None;

del cpu_count;		# not strictly necessary, but it frees a bit of ram

server_table = {};	# This is a local table storing the server loads

def heartbeat_listener():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
	s.bind((HEARTBEAT_HOST, HEARTBEAT_PORT));
	while True:
		packet = s.recvfrom(128);
		# A very conoluted way to get the data in place, using as little ram as possible.
		server_table[packet[1][0]] = [0, packet[0]];
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
	data = "";
	for ip in server_table:
		data += server_table[0] + "," + server_table[ip][1] + "\r\n";
	s.send(data);
	return

def main():
	for ip in HOSTS:
		server_table[ip] = None;

	heartbeat_thread = Thread(target = heartbeat_listener);		# Start the heartbeat listener
	heartbeat_thread.daemon = True;
	heartbeat_thread.start();
	webservice_thread = Thread(target = webservice_listener);	# Start the webservice listener
	webservice_thread.daemon = True;
	webservice_thread.start();

	while True:
		print("Main: " + str(server_table));
		sleep(HEARTBEAT_INTERVAL / 1000.0);			# sleep in ms
		heartbeat = str(HOSTNAME + "," + str(getloadavg()[0] / CPU_CORES) + "," + "0.14");
		for ip in server_table:
			# Send a heartbeat to them.
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
			s.sendto(heartbeat, (ip, HEARTBEAT_PORT));
			if (server_table[ip]):
				if (server_table[ip][0] > 60):
					del server_table[ip];
					continue;
				server_table[ip][0] += 1;

main();
