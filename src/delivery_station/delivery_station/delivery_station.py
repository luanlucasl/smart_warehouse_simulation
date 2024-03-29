from interfaces.srv import PickUpItem

import time
import rclpy
from rclpy.node import Node

class DeliveryStation(Node):

    def __init__(self):
        super().__init__('delivery_station')
        self.__pickup_item = self.create_service(PickUpItem, 'pick_up_item', self.pickup_item)

    def pickup_item(self, request, response):
        response.sum = request.a + request.b
        self.get_logger().info('Incoming request\na: %d b: %d' % (request.a, request.b))
        time.sleep(5)

        return response


def main(args=None):
    rclpy.init(args=args)

    delivery_station = DeliveryStation()
    try:
        rclpy.spin(delivery_station)
    finally:
        delivery_station.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
