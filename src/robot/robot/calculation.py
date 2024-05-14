import math


def normal_round(n):
    if n - math.floor(n) < 0.5:
        return math.floor(n)
    return math.ceil(n)


def battery_ratio(current_battery, total_battery):
    return current_battery / total_battery


def weight_ratio(current_weight, weight_capacity):
    return current_weight / weight_capacity


def necessary_battery_to_move(source, destination, battery_per_cell, additional_battery):
    return (source.distance(destination) * battery_per_cell) + additional_battery
    #return source.distance(destination) * (battery_per_cell + (battery_per_cell * additional_battery))


def necessary_battery_to_lift_arm(slot_level):
    return slot_level


def additional_battery_cost(weight, weight_capacity):
    ratio = weight_ratio(weight, weight_capacity)

    if 0.4 < ratio < 0.8:
        return 0.05
    elif ratio > 0.8:
        return 0.08

    return 0


def necessary_time_to_move(source, destination, speed, cell_length):
    time_to_move_one_cell = cell_length / speed
    return source.distance(destination) * time_to_move_one_cell


def necessary_time_to_lift_arm(slot_level):
    return slot_level


def necessary_battery_to_move_to_docking_station_with_safety_from_delivery_station(delivery_station, docking_station, battery_per_cell):
    return necessary_battery_to_move_to_docking_station_from_delivery_station(delivery_station, docking_station, battery_per_cell) + (2 * battery_per_cell)


def necessary_battery_to_move_to_docking_station_from_delivery_station(delivery_station, docking_station, battery_per_cell):
    return necessary_battery_to_move(delivery_station,
                                     docking_station,
                                     battery_per_cell,
                                     0)


def necessary_battery_to_process_item(value, destination, item_weight, item_slot, logger, item_id, robot_name):
    current_latest_item_to_be_picked_up = value['latest_item_position']
    # 1 is the cost to deliver the items at the delivery station
    battery_to_lift_arm = necessary_battery_to_lift_arm(item_slot) + 1
    source = value['current_position']
    previously_calculated_battery_to_pick_up_item = 0

    #logger.info("Evaluating Robot {}, Item {}, latest item position: {}".format(robot_name, item_id, current_latest_item_to_be_picked_up))
    if current_latest_item_to_be_picked_up:
        source = current_latest_item_to_be_picked_up
        previously_calculated_battery_to_move_to_destination = additional_battery_cost(value['current_capacity'],
                                                                                       value['total_capacity'])
        # 1 represents the battery cost to deliver the items at the delivery station
        previously_calculated_battery_to_pick_up_item = (necessary_battery_to_move(source,
                                                                                   value['delivery_station'],
                                                                                   value['battery_per_cell'],
                                                                                   previously_calculated_battery_to_move_to_destination) + 1
                                                                                   )

    battery_to_move_to_destination = additional_battery_cost(value['current_capacity'], value['total_capacity'])
    battery_to_move_to_item_destination = necessary_battery_to_move(source, destination,
                                                                    value['battery_per_cell'],
                                                                    battery_to_move_to_destination)
    #logger.info("Evaluating Robot {}, Item {}, Battery to go to item destination {} and pick up {}".format(robot_name, item_id, battery_to_move_to_item_destination, battery_to_lift_arm))
    additional_battery = additional_battery_cost(value['current_capacity'] + item_weight,
                                                 value['total_capacity'])
    battery_to_move_to_delivery_station = necessary_battery_to_move(destination, value['delivery_station'],
                                                                    value['battery_per_cell'],
                                                                    additional_battery)
    #logger.info("Evaluating Robot {}, Item {}, Battery to go to delivery station {}".format(robot_name, item_id, battery_to_move_to_delivery_station))
    #logger.info("Evaluating Robot {}, Item {}, Previously calculated battery {}".format(robot_name, item_id, previously_calculated_battery_to_pick_up_item))

    return (battery_to_move_to_item_destination +
            battery_to_move_to_delivery_station +
            battery_to_lift_arm -
            previously_calculated_battery_to_pick_up_item)
