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

class MetricsCollector(Node):

    def __init__(self):
        super().__init__('metrics_collector')

        self.__metrics_subscription(String, 'metrics', self.write_data, qos.qos_profile_sensor_data)

    def write_data(self):
        return


def main(args=None):
    rclpy.init(args=args)
    metrics_collector = MetricsCollector()

    try:
        rclpy.spin(metrics_collector)
    finally:
        metrics_collector.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
