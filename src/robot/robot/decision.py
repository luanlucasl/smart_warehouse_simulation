from .calculation import battery_ratio, necessary_battery_to_move_to_docking_station_with_safety_from_delivery_station, necessary_battery_to_move_to_docking_station_from_delivery_station, necessary_battery_to_process_item
from .priority_entry import PriorityEntry

BATTERY_THRESHOLD = 0.12


def should_accept_item(sector_data, robot_name, item_weight, item_slot, destination, logger, item_id):
    available_robots = dict()
    for key, value in sector_data.items():
        if (not value['charging'] and not value['delivering'] and value['current_capacity'] + item_weight <= value['total_capacity']):
            battery_to_accept_item = (necessary_battery_to_process_item(value,
                                                                        destination,
                                                                        item_weight,
                                                                        item_slot,
                                                                        logger,
                                                                        item_id,
                                                                        key) +
                                     necessary_battery_to_move_to_docking_station_from_delivery_station(value['delivery_station'],
                                                                                                        value['initial_position'],
                                                                                                        value['battery_per_cell']))
            battery_to_docking_station = necessary_battery_to_move_to_docking_station_with_safety_from_delivery_station(value['delivery_station'], 
                                                                                                                        value['initial_position'],
                                                                                                                        value['battery_per_cell'])
            if (not is_recharge_threshold_met(value['battery'] - battery_to_accept_item, value['total_battery']) 
                and value['battery'] - battery_to_accept_item > battery_to_docking_station):
                available_robots[key] = PriorityEntry(key, value, destination)

    #logger.info('Available robots to process id {}: {}'.format(item_id, ', '.join(str(x.get_id()) for x in sorted(available_robots.values()))))

    if len(available_robots) == 0 or robot_name not in available_robots.keys():
        return (False, len(available_robots))
    if len(available_robots) == 1 and robot_name in available_robots.keys():
        return (True, len(available_robots))

    return (robot_name == sorted(available_robots.values())[0].get_id(), len(available_robots))


def is_recharge_threshold_met(current_battery, total_battery):
    return battery_ratio(current_battery, total_battery) <= BATTERY_THRESHOLD
