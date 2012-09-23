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

del cpu_count;		# not strictly necessary, but it frees a bit of ram

server_table = {};	# This is a local table storing the server loads

def heartbeat_listener():
	print("Listening for heartbeats...");
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
	s.bind((HEARTBEAT_HOST, HEARTBEAT_PORT));
	while True:
		print("Looping in heartbeat thread.");
		packet = s.recvfrom(128);
		print("Got a heartbeat...");
		# A very conoluted way to get the data in place, using as little ram as possible.
		server_table[packet[0][0]] = [0] + packet[0].split(',');
	return;

def webservice_listener():
	print("Listening for connections...");
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
	s.bind((WEBSERVICE_HOST, WEBSERVICE_PORT));
	s.listen(10);
	while True:
		print("Looping in webservice thread.");
		sock = s.accept()[0]
		t = Thread(target = webservice_handler, args = [sock]);
		t.start();
	return;

def webservice_handler(s):
	print("Got a connection, handling it...");
	data = "";
	for ip in server_table:
		data += server_table[ip].join(',') + "\r\n";
	print("Returned data: " + data);
	s.send(data);
	return

def main():
	for ip in HOSTS:
		server_table[ip] = None;

	Thread(target = heartbeat_listener).start();	# Start the heartbeat listener
	Thread(target = webservice_listener).start();	# Start the webservice listener

	while True:
		print("Looping.");
		sleep(HEARTBEAT_INTERVAL / 1000.0);	# sleep in ms
		heartbeat = str(HOSTNAME + "," + str(getloadavg()[0] / CPU_CORES) + "," + "0.14");
		for ip in server_table:
			if (server_table[ip]):
				if (server_table[ip][0] > 60):
					del server_table[ip];
					continue;
				s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
				s.sendto(heartbeat, (ip, HEARTBEAT_PORT));
				server_table[ip][0] += 1;

main();
