import json
import rclpy
import os

from collections import deque
from .calculation import battery_ratio
from rclpy.duration import Duration
from rclpy import qos
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.qos import QoSHistoryPolicy
from rclpy.qos import QoSReliabilityPolicy
from rclpy.qos import QoSDurabilityPolicy
from std_msgs.msg import String

from .calculation import additional_battery_cost, necessary_battery_to_move, \
     necessary_battery_to_move_to_docking_station_with_safety_from_delivery_station, necessary_battery_to_process_item, necessary_time_to_move, \
     necessary_battery_to_lift_arm, necessary_time_to_lift_arm
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
        self.declare_parameter('cell_length', rclpy.Parameter.Type.DOUBLE)
        self.declare_parameter('battery_per_cell', rclpy.Parameter.Type.DOUBLE)
        self.declare_parameter('capacity', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('speed', rclpy.Parameter.Type.INTEGER)
        self.declare_parameter('battery', rclpy.Parameter.Type.INTEGER)

        # get parameters
        delivery_station = self.get_parameter('delivery_station').get_parameter_value().integer_array_value
        initial_position = self.get_parameter('initial_position').get_parameter_value().integer_array_value
        self.__sector = self.get_parameter('sector').get_parameter_value().integer_value
        self.__cell_length = self.get_parameter('cell_length').get_parameter_value().double_value
        self.__battery_per_cell = self.get_parameter('battery_per_cell').get_parameter_value().double_value

        # kafka
        self.__kafka_consumer = KafkaConsumerWrapper('localhost:9092', 'my-topic-1', self.__sector)
        self.get_logger().info("Robot sector {}".format(self.__sector))
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
        self.__delivering = False

        # ros domain
        internal_sector_topic = 'internal_' + str(self.__sector)
        self.__check_new_item_callback_group = MutuallyExclusiveCallbackGroup()
        self.__update_data_callback_group = MutuallyExclusiveCallbackGroup()
        self.__publish_data_callback_group = MutuallyExclusiveCallbackGroup()
        self.__process_item_callback_group = MutuallyExclusiveCallbackGroup()
        qos_profile = QoSProfile(
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=50,
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
            durability=QoSDurabilityPolicy.VOLATILE
        )

        # create a timer to request kafka for new available item
        self.__check_new_item_timer = self.create_timer(0.6, self.accept_item,
                                                        callback_group=self.__check_new_item_callback_group)
        # create a timer to process items
        self.__process_item_timer = self.create_timer(0.5, self.process_item,
                                                      callback_group=self.__process_item_callback_group)
        # process data sent by other robots within the same sector
        self.create_subscription(String, internal_sector_topic, self.update_sector_data, qos.qos_profile_parameters,
                                 callback_group=self.__update_data_callback_group)

        # share robot attributes with other robots within the sector
        self.__sector_publisher = self.create_publisher(String, internal_sector_topic, qos.qos_profile_parameters)
        self.__publish_robot_data_timer = self.create_timer(0.3, self.publish_robot_data,
                                                            callback_group=self.__publish_data_callback_group)

        # battery
        self.__battery_handler_timer = self.create_timer(1, self.battery_handler,
                                                         callback_group=self.__process_item_callback_group)

        # metrics
        self.__metrics_publisher = self.create_publisher(String, 'metrics', qos.qos_profile_parameters)
        self.__item_accepted_metrics_publisher = self.create_publisher(String, 'item_accepted_metrics',
                                                                       qos.qos_profile_parameters)
        self.__idle_metrics_publisher = self.create_publisher(String, 'idle_metrics', qos.qos_profile_parameters)

    def accept_item(self):
        if self.__delivering or self.__charging or is_recharge_threshold_met(self.__current_battery,
                                                                             self.__total_battery):
            return

        message = self.__kafka_consumer.poll_next()

        if message:
            latest_processed_offset_within_sector = self.get_latest_processed_offset_within_sector()
            if latest_processed_offset_within_sector > message.offset:
                # self.get_logger().info("Item {} already processed, seeking to latest offset".format(message.value.get('item')['id']))
                self.__kafka_consumer.seek(latest_processed_offset_within_sector)
                return
        else:
            # self.get_logger().info("No message found")
            return

        # self.get_logger().info("Message: {}".format(message))

        item = message.value.get('item')
        item_weight = item['weight']
        slot_level = item['slot_level']
        destination = Point(item['x'], item['y'])
        sector_data = self.__sector_data.copy()
        should_process_item = should_accept_item(sector_data, self.get_name(), item_weight, slot_level,
                                                 destination, self.get_logger(), item['id'])
        if should_process_item[0]:
            # we need to update the values right after the decision so it affects next items/decisions
            self.__current_battery -= necessary_battery_to_process_item(sector_data[self.get_name()],
                                                                        destination, item_weight, slot_level,
                                                                        self.get_logger(), item['id'],
                                                                        self.get_name())
            self.__latest_picked_up_position = destination
            self.__current_capacity += item_weight
            self.get_logger().info(
                "After accepting item {}: battery {}; weight: {}".format(item['id'], self.__current_battery,
                                                                         self.__current_capacity))
            self.__items_to_be_picked_up.append(item)
            item_accepted_at = self.get_clock().now().seconds_nanoseconds()[0]

            self.__kafka_consumer.commit(message)
            self.__kafka_consumer.seek(message.offset + 1)
            self.__latest_offset_processed = message.offset + 1
            # publish new robot data right after accepting item
            self.publish_robot_data()
            self.__publish_robot_data_timer.reset()

            metrics_message = String()
            metrics_message.data = json.dumps({'robot_id': self.get_name(),
                                               'item_id': item['id'],
                                               'item_created_at': item['created_at'],
                                               'item_accepted_at': item_accepted_at,
                                               'time_to_get_accepted_in_seconds': item_accepted_at - item[
                                                   'created_at']},
                                              default=vars)
            self.__item_accepted_metrics_publisher.publish(metrics_message)
        else:
            # if there's no robot available to process the task, keep the offset here until one is available
            # if should_process_item[1] == 0:
            self.__kafka_consumer.seek(message.offset)
            # else:
            #     self.get_logger().info("Item {} accepted by another robot, moving to next offset".format(item['id']))
            #     self.__kafka_consumer.seek(message.offset + 1)

    def process_item(self):
        if not self.__items_to_be_picked_up or self.__charging:
            return

        if not self.__process_item_timer.is_canceled():
            self.__process_item_timer.cancel()
        if not self.__battery_handler_timer.is_canceled():
            self.__battery_handler_timer.cancel()

        starting_from = self.__current_position
        total_distance_for_items = 0
        task_battery = 0
        task_weight = 0
        amount_of_items_processed = 0
        processing_start_seconds = self.get_clock().now().seconds_nanoseconds()[0]
        while len(self.__items_to_be_picked_up) > 0:
            item = self.__items_to_be_picked_up.popleft()
            # self.get_logger().info("Processing new item with id: {}, located at x: {}, y: {}; current battery: {}; current capacity: {}".format(item['id'], item['x'], item['y'], self.__current_battery, self.__current_capacity))

            destination = Point(item['x'], item['y'])
            from_position = self.__current_position
            distance = from_position.distance(destination)
            self.__current_position = destination
            self.get_logger().info("Moving to {} to pick up item with id {}, distance {}".format(destination, item['id'], distance))
            #self.get_logger().info("Battery to move to destination: {}, current: {}".format(necessary_battery_to_move(from_position, destination, self.__battery_per_cell, additional_battery_cost(task_weight, self.__total_capacity)), self.__current_battery))
            total_distance_for_items += distance
            task_battery += necessary_battery_to_move(from_position, destination, self.__battery_per_cell,
                                                      additional_battery_cost(task_weight, self.__total_capacity)
                                                      )
            time_to_move = necessary_time_to_move(from_position, destination, self.__speed, self.__cell_length)
            self.get_clock().sleep_for(Duration(seconds=time_to_move))

            self.get_logger().info("Picking up item at {} with id {} and battery {}".format(destination, item['id'], necessary_battery_to_lift_arm(item['slot_level'])))
            task_battery += necessary_battery_to_lift_arm(item['slot_level'])
            task_weight += item['weight']
            #self.get_logger().info("Weight so far: {}, battery so far: {}".format(task_weight, task_battery))
            time_to_lift_arm = necessary_time_to_lift_arm(item['slot_level'])
            self.get_clock().sleep_for(Duration(seconds=time_to_lift_arm))
            amount_of_items_processed += 1

        from_current_position = self.__current_position
        self.__current_position = self.__delivery_station
        self.__delivering = True
        self.get_logger().info(
            "Bringing item(s) to delivery station at {} with distance {}".format(self.__delivery_station,
                                                                                 from_current_position.distance(
                                                                                     self.__delivery_station)))
        # 1 is the cost to deliver the items at the delivery station
        task_battery += necessary_battery_to_move(from_current_position, self.__delivery_station,
                                                  self.__battery_per_cell,
                                                  additional_battery_cost(task_weight,
                                                                          self.__total_capacity)) + 1
        #self.get_logger().info("Total battery including going to delivery station {}".format(task_battery))
        self.__current_capacity = 0
        self.__latest_picked_up_position = None
        # it takes another 2 seconds to move the items from the robot onto the table
        time_to_deliver_items = necessary_time_to_move(from_current_position, self.__delivery_station,
                                                       self.__speed,
                                                       self.__cell_length) + 2
        self.get_clock().sleep_for(Duration(seconds=time_to_deliver_items))
        self.__delivering = False
        processing_end_seconds = self.get_clock().now().seconds_nanoseconds()[0]

        self.get_logger().info("Battery after processing item(s): {}".format(self.__current_battery))

        metrics_message = String()
        metrics_message.data = json.dumps({'robot_id': self.get_name(),
                                           'started_to_process_item_at': processing_start_seconds,
                                           'finished_processing_item_at': processing_end_seconds,
                                           'sector': self.__sector,
                                           'robot_started_from': starting_from,
                                           'delivery_station': self.__delivery_station,
                                           'total_distance': total_distance_for_items + from_current_position.distance(
                                               self.__delivery_station),
                                           'processing_lasted_in_seconds': processing_end_seconds - processing_start_seconds,
                                           'battery_used_for_task': task_battery,
                                           'battery_after_processing_items': self.__current_battery,
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

        data[robot_name]['current_position'] = Point(data[robot_name]['current_position']['x'],
                                                     data[robot_name]['current_position']['y'])
        data[robot_name]['delivery_station'] = Point(data[robot_name]['delivery_station']['x'],
                                                     data[robot_name]['delivery_station']['y'])
        data[robot_name]['initial_position'] = Point(data[robot_name]['initial_position']['x'],
                                                     data[robot_name]['initial_position']['y'])

        latest_item_position = None
        if data[robot_name]['latest_item_position']:
            latest_item_position = Point(data[robot_name]['latest_item_position']['x'],
                                         data[robot_name]['latest_item_position']['y'])
        data[robot_name]['latest_item_position'] = latest_item_position

        self.__sector_data[robot_name] = data[robot_name]
        # self.get_logger().info("Received {}".format(self.__sector_data[robot_name]))

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
                               'delivering': self.__delivering,
                               'items_waiting_to_be_processed': len(self.__items_to_be_picked_up)}},
            default=vars)
        # self.get_logger().info("Sent {}".format(ros_message.data))
        self.__sector_publisher.publish(ros_message)

    def battery_handler(self):
        # it will never have items while this is running
        # this can only be executed when robot is idle, which means
        # all items have been delivered
        battery_to_go_to_charging_station = necessary_battery_to_move_to_docking_station_with_safety_from_delivery_station(self.__current_position,
                                                                                                                           self.__delivery_station,
                                                                                                                           self.__battery_per_cell)
        if not self.__charging:
            if (is_recharge_threshold_met(self.__current_battery, self.__total_battery) or
                                     battery_to_go_to_charging_station >= self.__current_battery):
                self.__charging = True
                from_current_position = self.__current_position
                #self.get_logger().info(
                #    "Distance to docking station: {}".format(from_current_position.distance(self.__initial_position)))
                self.__current_position = self.__initial_position
                # subtract values to get the real value
                #self.get_logger().info("Battery to docking station: {}".format(battery_to_go_to_charging_station - 1 - self.__battery_per_cell))
                self.__current_battery -= necessary_battery_to_move(from_current_position, self.__delivery_station, self.__battery_per_cell, 0)
                self.get_logger().info(
                    "Going from {} to docking station: {}".format(from_current_position, self.__initial_position))
                time_to_move = necessary_time_to_move(from_current_position, self.__initial_position, self.__speed,
                                                self.__cell_length)
                self.get_clock().sleep_for(Duration(seconds=time_to_move))
                self.get_logger().info("Starting to charge with battery: {}".format(self.__current_battery))
            elif self.__current_position != self.__initial_position:
                #self.get_logger().info("Decreasing battery")
                self.__current_battery -= 1
        
        if self.__charging:
            self.__current_battery = min(self.__current_battery + (self.__battery_per_cell * 0.8), self.__total_battery)

            if self.__current_battery == self.__total_battery:
                self.__charging = False
                # immediately catch up to items/offset on kafka broker
                self.publish_robot_data()
                self.__publish_robot_data_timer.reset()

                self.__check_new_item_timer.cancel()
                self.accept_item()
                self.__check_new_item_timer.reset()
                self.get_logger().info("Battery fully charged with {}.".format(self.__total_battery))

    def get_latest_processed_offset_within_sector(self):
        sector_data = self.__sector_data.copy()
        robot_name = max(self.__sector_data, key=lambda x: sector_data[x]['latest_offset_processed'])
        return self.__sector_data[robot_name]['latest_offset_processed']


def main(args=None):
    rclpy.init(args=args)
    try:
        robot = Robot()
        executor = MultiThreadedExecutor(num_threads=os.cpu_count())
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
