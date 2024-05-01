#!/usr/bin/env python

import copy
import datetime
import json
import logging
import pytz
import random
import sys
import threading
import time
import string

from datetime import timezone
from kafka import KafkaProducer


DictProxyType = type(object.__dict__)

def make_hash(o):

	"""
	Makes a hash from a dictionary, list, tuple or set to any level, that 
	contains only other hashable types (including any lists, tuples, sets, and
	dictionaries). In the case where other kinds of objects (like classes) need 
	to be hashed, pass in a collection of object attributes that are pertinent. 
	For example, a class can be hashed in this fashion:

	make_hash([cls.__dict__, cls.__name__])

	A function can be hashed like so:

	make_hash([fn.__dict__, fn.__code__])
	"""

	if type(o) == DictProxyType:
		o2 = {}
		for k, v in o.items():
			if not k.startswith("__"):
				o2[k] = v
		o = o2

	if isinstance(o, (set, tuple, list)):
		return tuple([make_hash(e) for e in o])    

	elif not isinstance(o, dict):
		return hash(o)

	new_o = copy.deepcopy(o)
	for k, v in new_o.items():
		new_o[k] = make_hash(v)

	return hash(tuple(frozenset(sorted(new_o.items()))))


class OrderMessage(dict):

	def __init__(self, orders):
		dict.__init__(self, orders=orders)
		self.orders = orders


class Message(dict):
	def __init__(self, created_at, item):
		dict.__init__(self, created_at=created_at, item=item)
		self.created_at = datetime.datetime.now()
		self.item = item


class ItemProducer():
	def __init__(self, bootstrap_server, topic):
		super().__init__()
		self.starting_at = datetime.datetime.utcnow()
		self.topic = topic
		self.messages_sent = 0
		self.producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=lambda m: json.dumps(m).encode('utf-8'), acks='all')


	def run(self):
		self.produce()


	def produce(self):
		sector_0 = self.__read_sector_data('sector_0')
		sector_1 = self.__read_sector_data('sector_1')
		sector_2 = self.__read_sector_data('sector_2')
		sector_3 = self.__read_sector_data('sector_3')
		max_items = max(len(sector_0), len(sector_1), len(sector_2), len(sector_3))

		try:
			for index in range(0, max_items):
				time.sleep(1)
				created_at = int(datetime.datetime.utcnow().replace(tzinfo=pytz.utc).timestamp())
				self.messages_sent += self.__publish_item(0, created_at, index, sector_0)
				# self.messages_sent += self.__publish_item(1, created_at, index, sector_1)
				# self.messages_sent += self.__publish_item(2, created_at, index, sector_2)
				# self.messages_sent += self.__publish_item(3, created_at, index, sector_3)
				
				
				print('Sent %s' % str(self.messages_sent))
		except Exception as e:
			print(e)
			# logging.info('Started at %s' % str(self.starting_at))
			# logging.info('Finished at %s' % str(datetime.datetime.utcnow()))
			# logging.info(self.amount_of_messages_sent_by_topic)


	def __read_sector_data(self, sector):
		items = list()
		with open(sector + ".json") as json_file:
			items = json.load(json_file)
		print('Sector ' + sector + ' has ' + str(len(items)) + ' items')
		return items

	def __publish_item(self, sector, created_at, index, items):
		sent = 0
		if (index < len(items)):
			item = items[index]
			item['created_at'] = created_at
			self.producer.send(self.topic, value=Message(created_at, item), partition=sector)
			print('Sent item to sector ' + str(sector) + ': ' + str(item['id']) + ', ' + str(item['x']) + ', ' + str(item['y']) + ', slot level ' + str(item['slot_level']))
			sent = 1
		return sent


if __name__ == '__main__':
	logging.basicConfig(format='%(threadName)s:%(message)s', filename='example.log', encoding='utf-8', level=logging.INFO, filemode='w')
	bootstrap_server = sys.argv[1]
	topic = sys.argv[2]

	ItemProducer(bootstrap_server, topic).produce()