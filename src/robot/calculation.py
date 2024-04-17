import math


def normal_round(n):
    if n - math.floor(n) < 0.5:
        return math.floor(n)
    return math.ceil(n)


def necessary_battery_to_move(source, destination, battery_per_cell, additional_battery):
    return normal_round(source.distance(destination) * battery_per_cell) + additional_battery


def additional_battery_cost(weight, weight_capacity):
    weight_ratio = weight / weight_capacity

    if 0.4 < weight_ratio < 0.8:
        return 1
    elif weight_ratio > 0.8:
        return 2

    return 0


def necessary_time_to_move(source, destination, speed, cell_length):
    time_to_move_one_cell = cell_length / speed
    return normal_round(source.distance(destination) * time_to_move_one_cell)


def necessary_battery_to_process_item(value, destination, item_weight, logger, item_id):
    current_latest_item_to_be_picked_up = value['latest_item_position']
    battery_to_move_hand_and_pick_up_item = 2
    source = value['current_position']
    previously_calculated_battery_to_pick_up_item = 0

    # self.get_logger().info("Item {}, latest item position: {}".format(item_id, current_latest_item_to_be_picked_up))
    if current_latest_item_to_be_picked_up and current_latest_item_to_be_picked_up != destination:
        source = current_latest_item_to_be_picked_up
        previously_calculated_battery_to_move_to_destination = additional_battery_cost(value['current_capacity'],
                                                                                       value['total_capacity'])
        # plus 1 represents the battery cost to deliver the items at the delivery station
        previously_calculated_battery_to_pick_up_item = (necessary_battery_to_move(source,
                                                                                   value['delivery_station'],
                                                                                   value['battery_per_cell'],
                                                                                   previously_calculated_battery_to_move_to_destination) +
                                                         necessary_battery_to_move(value['delivery_station'],
                                                                                   value['initial_position'],
                                                                                   value['battery_per_cell'],
                                                                                   0) + 1)

    battery_to_move_to_destination = additional_battery_cost(value['current_capacity'], value['total_capacity'])
    battery_to_move_to_item_destination = necessary_battery_to_move(source, destination,
                                                                    value['battery_per_cell'],
                                                                    battery_to_move_to_destination)
    logger.info("Item {}, Battery to go to item destination {}".format(item_id, battery_to_move_to_item_destination))
    additional_battery = additional_battery_cost(value['current_capacity'] + item_weight,
                                                 value['total_capacity'])
    battery_to_move_to_delivery_station = necessary_battery_to_move(destination, value['delivery_station'],
                                                                    value['battery_per_cell'],
                                                                    additional_battery)
    logger.info("Item {}, Battery to go to delivery station {}".format(item_id, battery_to_move_to_delivery_station))
    battery_to_move_to_docking_station = necessary_battery_to_move(value['delivery_station'],
                                                                   value['initial_position'],
                                                                   value['battery_per_cell'],
                                                                   0)
    logger.info("Item {}, Battery to go to docking station {}".format(item_id, battery_to_move_to_docking_station))
    logger.info(
        "Item {}, Previously calculated battery {}".format(item_id, previously_calculated_battery_to_pick_up_item))

    return (battery_to_move_to_item_destination +
            battery_to_move_to_delivery_station +
            battery_to_move_to_docking_station +
            battery_to_move_hand_and_pick_up_item -
            previously_calculated_battery_to_pick_up_item)
