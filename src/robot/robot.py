import json
import rclpy
import time
from collections import deque
from rclpy import qos
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from std_msgs.msg import String

from .calculation import additional_battery_cost, necessary_battery_to_move, necessary_battery_to_process_item, \
    necessary_time_to_move
from .decision import is_recharge_threshold_met, should_accept_item
from .kafka_consumer_wrapper import KafkaConsumerWrapper
from .point import Point


class Robot(Node):

    def __init__(self):
        super().__init__('robot')
        # declare parameters
        self.declare_parameter('delivery_station', rclpy.Parameter.Type.INTEGER_ARRAY)
        self.declare_parameter('initial_position', rclpy.Parameter.Type.INTEGER_ARRAY)
        self.declare_parameter('sector', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('cell_length', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('battery_per_cell', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('capacity', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('speed', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('battery', rclpy.Parameter.Type.INTEGER)

        # get parameters
        delivery_station = self.get_parameter('delivery_station').get_parameter_value().integer_array_value
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
        self.__delivery_station = Point(delivery_station[0], delivery_station[1])
        self.__initial_position = Point(initial_position[0], initial_position[1])
        self.__current_position = self.__initial_position
        self.__sector_data = dict()
        self.__items_to_be_picked_up = deque()
        self.__latest_picked_up_position = None
        self.__previous_capacity = None

        # ros domain
        internal_sector_topic = 'internal_' + str(self.__sector)
        self.__check_new_item_callback_group = MutuallyExclusiveCallbackGroup()
        self.__update_data_callback_group = MutuallyExclusiveCallbackGroup()
        self.__publish_data_callback_group = MutuallyExclusiveCallbackGroup()
        self.__process_item_callback_group = MutuallyExclusiveCallbackGroup()

        # create a timer to request kafka for new available item
        self.__check_new_item_timer = self.create_timer(0.5, self.process_item,
                                                        callback_group=self.__check_new_item_callback_group)
        # create a timer to process items
        self.__process_item_timer = self.create_timer(0.5, self.real_process_item,
                                                      callback_group=self.__process_item_callback_group)
        # process data sent by other robots within the same sector
        self.create_subscription(String, internal_sector_topic, self.update_sector_data, qos.qos_profile_sensor_data,
                                 callback_group=self.__update_data_callback_group)

        # share robot attributes with other robots within the sector
        self.__sector_publisher = self.create_publisher(String, internal_sector_topic, qos.qos_profile_sensor_data)
        self.__publish_robot_data_timer = self.create_timer(0.1, self.publish_robot_data,
                                                            callback_group=self.__publish_data_callback_group)

        # battery
        self.__battery_handler_timer = self.create_timer(2, self.battery_handler,
                                                         callback_group=self.__process_item_callback_group)

        # metrics
        self.__metrics_publisher = self.create_publisher(String, 'metrics', qos.qos_profile_parameters)

    def process_item(self):
        if self.__charging or is_recharge_threshold_met(self.__current_battery, self.__total_battery):
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
        item_weight = item['weight']
        destination = Point(item['x'], item['y'])
        should_process_item = should_accept_item(self.__sector_data, self.get_name(), item_weight, destination, self.get_logger(), item['id'])
        if should_process_item[0]:
            # we need to update the values right after the decision so it affects next items/decisions
            self.__current_battery -= necessary_battery_to_process_item(self.__sector_data[self.get_name()],
                                                                        destination, item_weight, self.get_logger(), item['id'])
            self.__previous_capacity = self.__current_capacity
            self.__current_capacity += item_weight
            # self.get_logger().info(
            #     "After accepting item {}: battery {}; weight: {}".format(item['id'], self.__current_battery,
            #                                                              self.__current_capacity))
            self.__items_to_be_picked_up.append(item)
            self.__latest_picked_up_position = destination

            self.__kafka_consumer.commit(message)
            self.__kafka_consumer.seek(message.offset + 1)
            self.__latest_offset_processed = message.offset + 1
        else:
            # if there's no robot available to process the task, keep the offset here until one is available
            if should_process_item[1] == 0:
                self.__kafka_consumer.seek(message.offset)
            else:
                self.__kafka_consumer.seek(message.offset + 1)

    def real_process_item(self):
        if not self.__items_to_be_picked_up or self.__charging:
            return

        if not self.__process_item_timer.is_canceled():
            self.__process_item_timer.cancel()
        if not self.__battery_handler_timer.is_canceled():
            self.__battery_handler_timer.cancel()

        processing_start_seconds = self.get_clock().now().seconds_nanoseconds()[0]
        starting_from = self.__current_position
        items_data = list()
        total_distance_for_items = 0
        task_battery = 0
        task_weight = 0
        amount_of_items_processed = 0
        while len(self.__items_to_be_picked_up) > 0:
            item = self.__items_to_be_picked_up.popleft()
            # self.get_logger().info("Processing new item with id: {}, located at x: {}, y: {}; current battery: {}; current capacity: {}".format(item['id'], item['x'], item['y'], self.__current_battery, self.__current_capacity))

            destination = Point(item['x'], item['y'])
            items_data.append({'id': item['id'], 'created_at': item['created_at'], 'position': destination})
            from_position = self.__current_position
            distance = from_position.distance(destination)
            self.get_logger().info(
                "Moving to {} to pick up item with id {}, distance {}".format(destination, item['id'], distance))
            self.__current_position = destination
            # self.get_logger().info("Battery to move to destination: {}, current: {}".format(necessary_battery_to_move(from_position, destination, self.__battery_per_cell), self.__current_battery))

            total_distance_for_items += distance
            time.sleep(necessary_time_to_move(from_position, destination, self.__speed, self.__cell_length))

            self.get_logger().info("Picking up item at {} with id {}".format(destination, item['id']))
            task_battery += necessary_battery_to_move(from_position, destination, self.__battery_per_cell,
                                                      additional_battery_cost(task_weight,
                                                                              self.__total_capacity)) + 1
            task_weight += item['weight']
            self.get_logger().info("Weight so far: {}, battery so far: {}".format(task_weight, task_battery))
            time.sleep(2)
            amount_of_items_processed += 1

        from_current_position = self.__current_position
        self.get_logger().info(
            "Bringing item(s) to delivery station at {} with distance {}".format(self.__delivery_station,
                                                                                 from_current_position.distance(
                                                                                     self.__delivery_station)))
        self.__current_position = self.__delivery_station
        task_battery += necessary_battery_to_move(from_current_position, self.__delivery_station,
                                                  self.__battery_per_cell,
                                                  additional_battery_cost(task_weight,
                                                                          self.__total_capacity)) + 1
        # self.get_logger().info("Total battery including going to delivery station {}".format(task_battery))
        # self.get_logger().info("Distance to docking station {}".format(self.__delivery_station.distance(self.__initial_position)))
        task_battery += necessary_battery_to_move(self.__delivery_station, self.__initial_position,
                                                  self.__battery_per_cell,
                                                  0)
        self.__current_capacity = 0
        self.__latest_picked_up_position = None
        self.__latest_item_weight = None
        # self.get_logger().info("Battery to delivery: {}, current: {}".format(necessary_battery_to_move(from_current_position, self.__delivery_station, self.__battery_per_cell), self.__current_battery))
        # it takes another 2 seconds to move the items from the robot onto the table
        time_to_deliver_items = necessary_time_to_move(from_current_position, self.__delivery_station,
                                                       self.__speed,
                                                       self.__cell_length) + 2
        time.sleep(time_to_deliver_items)
        processing_end_seconds = self.get_clock().now().seconds_nanoseconds()[0]
        self.get_logger().info(
            "Total battery after processing including to go to docking station: {}".format(task_battery))
        self.get_logger().info("Battery after processing item(s): {}".format(self.__current_battery))

        metrics_message = String()
        metrics_message.data = json.dumps({'robot_id': self.get_name(), 'items_data': items_data,
                                           'started_to_process_item_at': processing_start_seconds,
                                           'finished_processing_item_at': processing_end_seconds,
                                           'sector': self.__sector,
                                           'robot_started_from': starting_from,
                                           'delivery_station': self.__delivery_station,
                                           'total_distance': total_distance_for_items + from_current_position.distance(
                                               self.__delivery_station),
                                           'processing_lasted_in_seconds': processing_end_seconds - processing_start_seconds,
                                           'battery_used_for_task': task_battery,
                                           'amount_of_items_processed': amount_of_items_processed},
                                          default=vars)
        self.__metrics_publisher.publish(metrics_message)

        if self.__process_item_timer.is_canceled():
            self.__process_item_timer.reset()
        if self.__battery_handler_timer.is_canceled():
            self.__battery_handler_timer.reset()

    def update_sector_data(self, message):
        data = json.loads(message.data)
        robot_name = next(iter(data))
        self.__sector_data[robot_name] = data[robot_name]
        self.__sector_data[robot_name]['current_position'] = Point(data[robot_name]['current_position']['x'],
                                                                   data[robot_name]['current_position']['y'])
        self.__sector_data[robot_name]['delivery_station'] = Point(data[robot_name]['delivery_station']['x'],
                                                                   data[robot_name]['delivery_station']['y'])
        self.__sector_data[robot_name]['initial_position'] = Point(data[robot_name]['initial_position']['x'],
                                                                   data[robot_name]['initial_position']['y'])
        latest_item_position = None
        if self.__sector_data[robot_name]['latest_item_position']:
            latest_item_position = Point(data[robot_name]['latest_item_position']['x'],
                                         data[robot_name]['latest_item_position']['y'])
        self.__sector_data[robot_name]['latest_item_position'] = latest_item_position

    def publish_robot_data(self):
        ros_message = String()
        ros_message.data = json.dumps(
            {self.get_name(): {'battery': self.__current_battery, 'total_battery': self.__total_battery,
                               'total_capacity': self.__total_capacity,
                               'current_capacity': self.__current_capacity, 'current_position': self.__current_position,
                               'charging': self.__charging, 'delivery_station': self.__delivery_station,
                               'battery_per_cell': self.__battery_per_cell, 'initial_position': self.__initial_position,
                               'latest_offset_processed': self.__latest_offset_processed,
                               'latest_item_position': self.__latest_picked_up_position,
                               'previous_capacity': self.__previous_capacity}}, default=vars)
        self.__sector_publisher.publish(ros_message)

    def battery_handler(self):
        if not self.__charging:
            self.get_logger().info("Decreasing battery")
            self.__current_battery -= 1

        # it will never have items while this is running
        # this can only be executed when robot is idle, which means
        # all items have been delivered
        battery_to_go_to_charging_station = necessary_battery_to_move(self.__current_position,
                                                                      self.__initial_position,
                                                                      self.__battery_per_cell,
                                                                      0) + 1
        if (not self.__charging and (is_recharge_threshold_met(self.__current_battery, self.__total_battery) or
                                     battery_to_go_to_charging_station >= self.__current_battery)):
            self.__charging = True
            from_current_position = self.__current_position
            self.get_logger().info(
                "Distance to docking station: {}".format(from_current_position.distance(self.__initial_position)))
            self.__current_position = self.__initial_position
            self.get_logger().info(
                "Going from {} to charging station: {}".format(from_current_position, self.__initial_position))
            time.sleep(necessary_time_to_move(from_current_position, self.__initial_position, self.__speed,
                                              self.__cell_length))
            self.get_logger().info("Starting to charge with battery: {}".format(self.__current_battery))

        if self.__charging:
            self.__current_battery = min(self.__current_battery + 2, self.__total_battery)

            if self.__current_battery == self.__total_battery:
                self.__charging = False
                self.get_logger().info("Battery fully charged with {}.".format(self.__total_battery))

    def get_latest_processed_offset_within_sector(self):
        robot_name = max(self.__sector_data, key=lambda x: self.__sector_data[x]['latest_offset_processed'])
        return self.__sector_data[robot_name]['latest_offset_processed']


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
