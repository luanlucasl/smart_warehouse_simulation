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

        # metrics regarding task completion
        self.create_subscription(String, 'metrics', self.write_task_completion_data, qos.qos_profile_parameters)
        task_completion_file_name = 'simulation_{date:%Y-%m-%d_%H:%M:%S}_task_completion.csv'.format(date=datetime.datetime.now())
        header_v1 = ['robot_id', 'item_id', 'item_created_at', 'started_to_process_item_at', 'finished_processing_item_at',
                  'sector', 'robot_position', 'item_position', 'delivery_station', 'total_distance',
                  'processing_lasted_in_seconds', 'number_of_items', 'battery_used_for_task']
        header_v2 = ['robot_id', 'started_to_process_item_at', 'finished_processing_item_at',
                  'sector', 'robot_started_from', 'delivery_station', 'total_distance',
                  'processing_lasted_in_seconds', 'battery_used_for_task', 'battery_after_processing_items', 'amount_of_items_processed']
        self.__csv_task_completion_file = open(task_completion_file_name, 'w', newline='', encoding='utf-8-sig')
        self.__csv_task_completion_writer = csv.DictWriter(self.__csv_task_completion_file, fieldnames=header_v2)
        self.__csv_task_completion_writer.writeheader()

        # metrics regarding delay to accept item
        self.create_subscription(String, 'item_accepted_metrics', self.write_item_accepted_data, qos.qos_profile_parameters)
        item_accepted_file_name = 'simulation_{date:%Y-%m-%d_%H:%M:%S}_item_accepted.csv'.format(date=datetime.datetime.now())
        header = ['robot_id', 'item_id', 'item_created_at', 'item_accepted_at', 'time_to_get_accepted_in_seconds']
        self.__csv_item_accepted_file = open(item_accepted_file_name, 'w', newline='', encoding='utf-8-sig')
        self.__csv_item_accepted_writer = csv.DictWriter(self.__csv_item_accepted_file, fieldnames=header)
        self.__csv_item_accepted_writer.writeheader()

    def write_task_completion_data(self, message):
        metrics = json.loads(message.data)
        self.__csv_task_completion_writer.writerow(metrics)

    def write_item_accepted_data(self, message):
        metrics = json.loads(message.data)
        self.__csv_item_accepted_writer.writerow(metrics)

    def close_task_completion_file(self):
        self.__csv_task_completion_file.close()

    def close_item_accepted_file(self):
        self.__csv_item_accepted_file.close()


def main(args=None):
    rclpy.init(args=args)
    metrics_collector = MetricsCollector()

    try:
        rclpy.spin(metrics_collector)
    finally:
        metrics_collector.close_task_completion_file()
        metrics_collector.close_item_accepted_file()
        metrics_collector.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
