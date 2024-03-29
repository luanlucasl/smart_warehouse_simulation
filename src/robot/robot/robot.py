from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
from interfaces.srv import PickUpItem
from .kafka_consumer_wrapper import KafkaConsumerWrapper
from .floyd_warshall import FloydWarshall
from .priority_entry import PriorityEntry
from rclpy import qos
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from queue import PriorityQueue
from std_msgs.msg import String

import json
import math
import time
import rclpy

class Point(dict):

    def __init__(self, x, y):
        dict.__init__(self, x=x, y=y)
        self.x = x
        self.y = y

    def move(self, dx, dy):
        self.x = self.x + dx
        self.x = self.x + dy

    def __str__(self):
        return "Point(%s, %s)" % (self.x, self.y)

    def get_x(self):
        return self.x

    def get_y(self):
        return self.y

    def distance(self, other):
        dx = self.x - other.x
        dy = self.y - other.y
        return int(math.hypot(dx, dy))

class Robot(Node):

    def __init__(self):
        super().__init__('robot')
        # declare parameters
        self.declare_parameter('delivery_stations', rclpy.Parameter.Type.INTEGER_ARRAY)
        self.declare_parameter('initial_position', rclpy.Parameter.Type.INTEGER_ARRAY)
        self.declare_parameter('sector', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('cell_length', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('battery_per_cell', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('capacity', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('speed', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('battery', rclpy.Parameter.Type.INTEGER)

        # get parameters
        delivery_stations = self.get_parameter('delivery_stations').get_parameter_value().integer_array_value
        initial_position = self.get_parameter('initial_position').get_parameter_value().integer_array_value
        self.__sector = self.get_parameter('sector').get_parameter_value().integer_value
        self.__cell_length = self.get_parameter('cell_length').get_parameter_value().integer_value
        self.__battery_per_cell = self.get_parameter('battery_per_cell').get_parameter_value().integer_value

        # kafka
        self.get_logger().info("Robot sector {}".format(self.__sector))
        self.__kafka_consumer = KafkaConsumerWrapper('kafka:29092', 'my-topic-1', self.__sector)
        self.__latest_offset_processed = 0

        # robot attributes
        # m/s
        self.__speed = self.get_parameter('speed').get_parameter_value().integer_value
        self.__total_battery = self.get_parameter('battery').get_parameter_value().integer_value
        self.__current_battery = self.__total_battery
        self.__total_capacity = self.get_parameter('capacity').get_parameter_value().integer_value
        self.__current_capacity = 0
        self.__charging = False
        self.__delivery_stations = list((Point(x, y)) for x, y in zip(*[iter(delivery_stations)]*2))
        self.__initial_position = Point(initial_position[0], initial_position[1])
        self.__current_position = self.__initial_position
        self.__sector_data = dict()
        self.__items_to_be_picked_up = PriorityQueue()

        # ros domain
        internal_sector_topic = 'internal_' + str(self.__sector)
        self.__check_new_item_callback_group = MutuallyExclusiveCallbackGroup()
        self.__update_data_callback_group = MutuallyExclusiveCallbackGroup()
        self.__publish_data_callback_group = MutuallyExclusiveCallbackGroup()
        self.__process_item_callback_group = MutuallyExclusiveCallbackGroup()
        self.__battery_handler_callback_group = MutuallyExclusiveCallbackGroup()

        # create a timer to request kafka for new available item
        self.__check_new_item_timer = self.create_timer(0.5, self.process_item, callback_group=self.__check_new_item_callback_group)
        # create a timer to process items
        self.__process_item_timer = self.create_timer(0.5, self.real_process_item, callback_group=self.__process_item_callback_group)
        # process data sent by other robots within the same sector
        self.create_subscription(String, internal_sector_topic, self.update_sector_data, qos.qos_profile_sensor_data, callback_group=self.__update_data_callback_group)

        # share robot attributes with other robots within the sector
        self.__sector_publisher = self.create_publisher(String, internal_sector_topic, qos.qos_profile_sensor_data)
        self.__publish_robot_data_timer = self.create_timer(0.1, self.publish_robot_data, callback_group=self.__publish_data_callback_group)

        # charging
        self.__battery_handler_timer = self.create_timer(0.5, self.battery_handler, callback_group=self.__process_item_callback_group)

        # metrics
        self.__metrics_publisher = self.create_publisher(String, 'metrics', qos.qos_profile_sensor_data)

    def process_item(self):
        if self.__charging:
            return

        message = self.__kafka_consumer.poll_next()
        if not message:
            return

        # self.get_logger().info("Message: {}".format(message))
        latest_processed_offset_within_sector = self.get_latest_processed_offset_within_sector()
        if latest_processed_offset_within_sector > message.offset:
            self.__kafka_consumer.seek(latest_processed_offset_within_sector)
            return

        item = message.value.get('item')
        destination = Point(item['x'], item['y'])
        should_process_item = self.should_process_item(item['weight'], destination, item['id'])
        if should_process_item[0]:
            # we need to update the values right after the decision so it affects next items/decisions
            self.__current_battery -= self.get_estimated_battery_to_process_item(self.__sector_data[self.get_name()], destination)
            self.__current_capacity += item['weight']

            distance_to_destination = self.__current_position.distance(destination)
            # TODO: if we go for the nearest always it might be that some itens take too long to get picked up, maybe the priority should change based on when it was added
            self.__items_to_be_picked_up.put((distance_to_destination, PriorityEntry(message.timestamp, item)), block=True)
            # self.get_logger().info("Added destination: {}, with id {}".format(destination, item['id']))
            self.__kafka_consumer.commit(message)
            self.__kafka_consumer.seek(message.offset + 1)
            self.__latest_offset_processed = message.offset + 1
            # self.get_logger().info("Distance: {}, item: {}".format(distance_to_destination, item))
        else:
            # if there's no robot available to process the task, keep the offset here until one is available
            if should_process_item[1] == 0:
                self.__kafka_consumer.seek(message.offset)
                # self.get_logger().info("[seek] Skipping item {}, {}".format(item['x'], item['y']))
            else:
                # self.__kafka_consumer.commit(message)
                self.__kafka_consumer.seek(message.offset + 1)
                # self.get_logger().info("[commit] Skipping item {}, {}".format(item['x'], item['y']))

    def real_process_item(self):
        if self.__items_to_be_picked_up.empty() or self.__charging:
            return

        if not self.__process_item_timer.is_canceled():
            self.__process_item_timer.cancel()
        if not self.__battery_handler_timer.is_canceled():
            self.__battery_handler_timer.cancel()

        item = self.__items_to_be_picked_up.get(block=True)[1].data
        # self.get_logger().info("Processing new item with id: {}, located at x: {}, y: {}; current battery: {}; current capacity: {}".format(item['id'], item['x'], item['y'], self.__current_battery, self.__current_capacity))

        destination = Point(item['x'], item['y'])
        self.get_logger().info("Moving to {} to pick up item with id {}".format(destination, item['id']))
        from_position = self.__current_position
        self.__current_position = destination
        # self.__current_battery -= self.estimate_battery_capacity(from_position, destination, self.__battery_per_cell)
        # self.get_logger().info("Battery to move to destination: {}, current: {}".format(self.estimate_battery_capacity(from_position, destination, self.__battery_per_cell), self.__current_battery))
        # TODO: its possible that an item is on the way, do we want to handle that case?
        time.sleep(self.estimate_time_to_move(from_position, destination, self.__speed, self.__cell_length))

        self.get_logger().info("Picking up item at {} with id {}".format(destination, item['id']))
        time.sleep(2)

        # TODO: more delivery stations?
        self.get_logger().info("Bringing item with id {} to delivery station at {}".format(item['id'], self.__delivery_stations[0]))
        from_current_position = self.__current_position
        self.__current_position = self.__delivery_stations[0]
        self.__current_capacity -= item['weight']
        # self.__current_battery -= self.estimate_battery_capacity(from_current_position, self.__delivery_stations[0], self.__battery_per_cell)
        # self.get_logger().info("Battery to delivery: {}, current: {}".format(self.estimate_battery_capacity(from_current_position, self.__delivery_stations[0], self.__battery_per_cell), self.__current_battery))
        time.sleep(self.estimate_time_to_move(from_current_position, self.__delivery_stations[0], self.__speed, self.__cell_length))

        self.get_logger().info("Finished processing item with id {}".format(item['id']))
        self.get_logger().info("Battery after processing item with id {}: {}".format(item['id'], self.__current_battery))
        # TODO: currently the robot stays at the delivery station until a new task is picked up

        metrics = {'robot_id': self.get_name(), 'item_id': item['id'],
                  'item_created_at': 1, 'started_to_process_item_at': 1,
                  'sector': self.__sector, 'processing_lasted_in_seconds': 1,
                  'carrying_weight': 1, 'carrying_items': 1,
                  'battery_after_processing': 1, 'battery_before_processing': 1}
        metrics_message = String()
        metrics_message.data = metrics
        self.__metrics_publisher.publish(metrics)

        if self.__process_item_timer.is_canceled():
            self.__process_item_timer.reset()
        if self.__battery_handler_timer.is_canceled():
            self.__battery_handler_timer.reset()

    def update_sector_data(self, message):
        data = json.loads(message.data)
        robot_name = next(iter(data))
        self.__sector_data[robot_name] = data[robot_name]
        self.__sector_data[robot_name]['current_position'] = Point(data[robot_name]['current_position']['x'], data[robot_name]['current_position']['y'])
        self.__sector_data[robot_name]['delivery_station'] = Point(data[robot_name]['delivery_station']['x'], data[robot_name]['delivery_station']['y'])
        self.__sector_data[robot_name]['initial_position'] = Point(data[robot_name]['initial_position']['x'], data[robot_name]['initial_position']['y'])
        # self.get_logger().info("Data updated")

    def publish_robot_data(self):
        ros_message = String()
        ros_message.data = json.dumps({self.get_name(): {'battery': self.__current_battery, 'total_capacity': self.__total_capacity,
                                                         'current_capacity': self.__current_capacity, 'current_position': self.__current_position,
                                                         'charging': self.__charging, 'delivery_station': self.__delivery_stations[0],
                                                         'battery_per_cell': self.__battery_per_cell, 'initial_position': self.__initial_position,
                                                         'latest_offset_processed': self.__latest_offset_processed}}, default=vars)
        self.__sector_publisher.publish(ros_message)
        # self.get_logger().info("Sent updated data")

    def should_process_item(self, item_weight, destination, item_id):
        # when spinning up the node it might take a second for the producer to start sending data, and therefore it will be empty. Better wait to warm up and receive items
        # self.get_logger().info("Sector data: {}".format(self.__sector_data))
        available_robots = [key for key, value in self.__sector_data.items() if not value['charging'] and value['current_capacity'] + item_weight <= value['total_capacity'] and value['battery'] > self.get_estimated_battery_to_process_item(value, destination) + 1]
        # self.get_logger().info("Available to process item with id {}: {}".format(item_id, available_robots))
        # self.get_logger().info("Estimated battery to pick-up item: {}, current battery: {}".format(self.get_estimated_battery_to_process_item(destination), self.__current_battery))
        # self.get_logger().info("{}".format(self.__sector_data))

        if len(available_robots) == 0 or self.get_name() not in available_robots:
            return (False, len(available_robots))
        if len(available_robots) == 1 and self.get_name() in available_robots:
            return (True, len(available_robots))
        # it's a fallback in case there is more than one robot that can run the task
        # in that case, we just sort and get the first one
        # TODO: we can also prioritize the robot based on available battery, current weight, etc.
        return (self.get_name() == sorted(available_robots)[0], len(available_robots))

    def get_estimated_battery_to_process_item(self, value, destination):
        return (self.estimate_battery_capacity(value['current_position'], destination, value['battery_per_cell']) +
                self.estimate_battery_capacity(destination, value['delivery_station'], value['battery_per_cell']) +
                self.estimate_battery_capacity(value['delivery_station'], value['initial_position'],
                                               value['battery_per_cell']))

    def battery_handler(self):
        if not self.__charging:
            # self.get_logger().info("Decreasing battery")
            self.__current_battery -= 1

        battery_to_go_to_charging_station = self.estimate_battery_capacity(self.__current_position, self.__initial_position, self.__battery_per_cell)
        if not self.__charging and battery_to_go_to_charging_station + 1 >= self.__current_battery:
            self.__charging = True
            from_current_position = self.__current_position
            self.__current_battery -= battery_to_go_to_charging_station
            self.__current_position = self.__initial_position
            self.get_logger().info("Going from {} to charging station: {}".format(from_current_position, self.__initial_position))
            time.sleep(self.estimate_time_to_move(from_current_position, self.__initial_position, self.__speed, self.__cell_length))
            self.get_logger().info("Starting to charge with battery: {}".format(self.__current_battery))

        if self.__charging:
            self.__current_battery = min(self.__current_battery + 2, self.__total_battery)

            if self.__current_battery == self.__total_battery:
                self.__charging = False
                latest_processed_offset_within_sector = self.get_latest_processed_offset_within_sector()
                if latest_processed_offset_within_sector > 0:
                    self.__kafka_consumer.seek(latest_processed_offset_within_sector)
                self.get_logger().info("Battery fully charged.")

    def get_latest_processed_offset_within_sector(self):
        robot_name = max(self.__sector_data, key=lambda x: self.__sector_data[x]['latest_offset_processed'])
        return self.__sector_data[robot_name]['latest_offset_processed']

    def estimate_battery_capacity(self, source, destination, cells_per_battery_unit):
        return math.ceil(source.distance(destination) / cells_per_battery_unit)

    def estimate_time_to_move(self, source, destination, speed, cell_length):
        time_to_move_one_cell = cell_length / speed
        return math.ceil(source.distance(destination) * time_to_move_one_cell)


def main(args=None):
    rclpy.init(args=args)
    try:
        robot = Robot()
        executor = MultiThreadedExecutor(num_threads=5)
        executor.add_node(robot)

        try:
            executor.spin()
        finally:
            executor.shutdown()
            robot.destroy_node()
    finally:
        rclpy.shutdown()


if __name__ == '__main__':
    main()
