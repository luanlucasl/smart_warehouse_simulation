import csv
import datetime
import json
import rclpy
from rclpy import qos
from rclpy.node import Node
from std_msgs.msg import String


class MetricsCollector(Node):

    def __init__(self):
        super().__init__('metrics_collector')

        self.create_subscription(String, 'metrics', self.write_data, qos.qos_profile_parameters)
        file_name = 'simulation_{date:%Y-%m-%d_%H:%M:%S}.csv'.format(date=datetime.datetime.now())
        header_v1 = ['robot_id', 'item_id', 'item_created_at', 'started_to_process_item_at', 'finished_processing_item_at',
                  'sector', 'robot_position', 'item_position', 'delivery_station', 'total_distance',
                  'processing_lasted_in_seconds', 'number_of_items', 'battery_used_for_task']
        header_v2 = ['robot_id', 'items_data', 'started_to_process_item_at', 'finished_processing_item_at',
                  'sector', 'robot_started_from', 'delivery_station', 'total_distance',
                  'processing_lasted_in_seconds', 'battery_used_for_task', 'amount_of_items_processed']
        self.__csv_file = open(file_name, 'w', newline='', encoding='utf-8-sig')
        self.__csv_writer = csv.DictWriter(self.__csv_file, fieldnames=header_v2)
        self.__csv_writer.writeheader()

    def write_data(self, message):
        metrics = json.loads(message.data)
        self.__csv_writer.writerow(metrics)

    def close_file(self):
        self.__csv_file.close()


def main(args=None):
    rclpy.init(args=args)
    metrics_collector = MetricsCollector()

    try:
        rclpy.spin(metrics_collector)
    finally:
        metrics_collector.close_file()
        metrics_collector.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
