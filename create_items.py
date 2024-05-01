#!/usr/bin/env python

import json
import random
import string

#1#3#
#0#2#


# Sector 0
SECTOR_0_ITEMS            = 450
SECTOR_0_SHELF_SLOTS      = 3
SECTOR_0_MIN_WEIGHT       = 1
SECTOR_0_MAX_WEIGHT       = 149
SECTOR_0_MIN_COORDINATE_X = 0
SECTOR_0_MAX_COORDINATE_X = 14
SECTOR_0_MIN_COORDINATE_Y = 0
SECTOR_0_MAX_COORDINATE_Y = 14

# Sector 1
SECTOR_1_ITEMS            = 400
SECTOR_1_SHELF_SLOTS      = 2
SECTOR_1_MIN_WEIGHT       = 150
SECTOR_1_MAX_WEIGHT       = 349
SECTOR_1_MIN_COORDINATE_X = 0
SECTOR_1_MAX_COORDINATE_X = 14
SECTOR_1_MIN_COORDINATE_Y = SECTOR_0_MAX_COORDINATE_Y + 1
SECTOR_1_MAX_COORDINATE_Y = SECTOR_1_MIN_COORDINATE_Y + 14

# Sector 2
SECTOR_2_ITEMS            = 9
SECTOR_2_SHELF_SLOTS      = 1
SECTOR_2_MIN_WEIGHT       = 350
SECTOR_2_MAX_WEIGHT       = 499
SECTOR_2_MIN_COORDINATE_X = SECTOR_0_MAX_COORDINATE_X + 1
SECTOR_2_MAX_COORDINATE_X = SECTOR_2_MIN_COORDINATE_X + 3
SECTOR_2_MIN_COORDINATE_Y = 0
SECTOR_2_MAX_COORDINATE_Y = 3

# Sector 3
SECTOR_3_ITEMS            = 9
SECTOR_3_SHELF_SLOTS      = 1
SECTOR_3_MIN_WEIGHT       = 500
SECTOR_3_MAX_WEIGHT       = 1500
SECTOR_3_MIN_COORDINATE_X = SECTOR_1_MAX_COORDINATE_X + 1
SECTOR_3_MAX_COORDINATE_X = SECTOR_3_MIN_COORDINATE_X + 3
SECTOR_3_MIN_COORDINATE_Y = SECTOR_2_MAX_COORDINATE_Y + 1
SECTOR_3_MAX_COORDINATE_Y = SECTOR_3_MIN_COORDINATE_Y + 3

class ItemProducer():
	
	def produce(self):
		try:
			self.__create_items_sector0()
			self.__create_items_sector1()
			self.__create_items_sector2()
			self.__create_items_sector3()
		except Exception as e:
			print(e)


	def __create_items_sector0(self):
		print('Creating for sector 0 with min_x ' + str(SECTOR_0_MIN_COORDINATE_X) + ', max_x ' + str(SECTOR_0_MAX_COORDINATE_X) + ', min_y ' + str(SECTOR_0_MIN_COORDINATE_Y) + ', max_y ' + str(SECTOR_0_MAX_COORDINATE_Y))
		points_sector_0 = self.__create_points(SECTOR_0_ITEMS, SECTOR_0_MIN_COORDINATE_X, SECTOR_0_MAX_COORDINATE_X, SECTOR_0_MIN_COORDINATE_Y, SECTOR_0_MAX_COORDINATE_Y, SECTOR_0_SHELF_SLOTS)
		items_sector_0 = self.__create_items(points_sector_0, SECTOR_0_MIN_WEIGHT, SECTOR_0_MAX_WEIGHT)
		with open('sector_0.json', 'w', encoding='utf-8') as f:
			json.dump(items_sector_0, f, ensure_ascii=False)


	def __create_items_sector1(self):
		print('Creating for sector 1 with min_x ' + str(SECTOR_1_MIN_COORDINATE_X) + ', max_x ' + str(SECTOR_1_MAX_COORDINATE_X) + ', min_y ' + str(SECTOR_1_MIN_COORDINATE_Y) + ', max_y ' + str(SECTOR_1_MAX_COORDINATE_Y))
		points_sector_1 = self.__create_points(SECTOR_1_ITEMS, SECTOR_1_MIN_COORDINATE_X, SECTOR_1_MAX_COORDINATE_X, SECTOR_1_MIN_COORDINATE_Y, SECTOR_1_MAX_COORDINATE_Y, SECTOR_1_SHELF_SLOTS)
		items_sector_1 = self.__create_items(points_sector_1, SECTOR_1_MIN_WEIGHT, SECTOR_1_MAX_WEIGHT)
		with open('sector_1.json', 'w', encoding='utf-8') as f:
			json.dump(items_sector_1, f, ensure_ascii=False)


	def __create_items_sector2(self):
		print('Creating for sector 2 with min_x ' + str(SECTOR_2_MIN_COORDINATE_X) + ', max_x ' + str(SECTOR_2_MAX_COORDINATE_X) + ', min_y ' + str(SECTOR_2_MIN_COORDINATE_Y) + ', max_y ' + str(SECTOR_2_MAX_COORDINATE_Y))
		points_sector_2 = self.__create_points(SECTOR_2_ITEMS, SECTOR_2_MIN_COORDINATE_X, SECTOR_2_MAX_COORDINATE_X, SECTOR_2_MIN_COORDINATE_Y, SECTOR_2_MAX_COORDINATE_Y, SECTOR_2_SHELF_SLOTS)
		items_sector_2 = self.__create_items(points_sector_2, SECTOR_2_MIN_WEIGHT, SECTOR_2_MAX_WEIGHT)
		with open('sector_2.json', 'w', encoding='utf-8') as f:
			json.dump(items_sector_2, f, ensure_ascii=False)


	def __create_items_sector3(self):
		print('Creating for sector 3 with min_x ' + str(SECTOR_3_MIN_COORDINATE_X) + ', max_x ' + str(SECTOR_3_MAX_COORDINATE_X) + ', min_y ' + str(SECTOR_3_MIN_COORDINATE_Y) + ', max_y ' + str(SECTOR_3_MAX_COORDINATE_Y))
		points_sector_3 = self.__create_points(SECTOR_3_ITEMS, SECTOR_3_MIN_COORDINATE_X, SECTOR_3_MAX_COORDINATE_X, SECTOR_3_MIN_COORDINATE_Y, SECTOR_3_MAX_COORDINATE_Y, SECTOR_3_SHELF_SLOTS)
		items_sector_3 = self.__create_items(points_sector_3, SECTOR_3_MIN_WEIGHT, SECTOR_3_MAX_WEIGHT)
		with open('sector_3.json', 'w', encoding='utf-8') as f:
			json.dump(items_sector_3, f, ensure_ascii=False)


	def __create_items(self, points, min_weight, max_weight):
		items = list()
		for item in points:
				items.append({
					'id': ''.join(random.choices(string.ascii_uppercase + string.digits, k=7)),
					'slot_level': item[2],
					'weight': random.randint(min_weight, max_weight),
					'x': item[0],
					'y': item[1]
				})
		return items


	def __create_points(self, total_items, min_x, max_x, min_y, max_y, max_slots):
		existing_points = set()
		points = list()
		while len(existing_points) < total_items:
			point = (random.randint(min_x, max_x), random.randint(min_y, max_y), random.randint(1, max_slots))
			if point not in existing_points:
				points.append(point)
				existing_points.add(point)
		return points


if __name__ == '__main__':
	ItemProducer().produce()