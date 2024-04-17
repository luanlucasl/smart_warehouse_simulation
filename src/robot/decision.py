from .calculation import necessary_battery_to_process_item
from .priority_entry import PriorityEntry

BATTERY_THRESHOLD = 0.12


def should_accept_item(sector_data, robot_name, item_weight, destination, logger, item_id):
    # when spinning up the node it might take a second for the producer to start sending data, and therefore it will
    # be empty. Better wait to warm up and receive items
    available_robots = dict()
    for key, value in sector_data.items():
        necessary_battery = necessary_battery_to_process_item(value, destination, item_weight, logger, item_id)
        if (not value['charging'] and value['current_capacity'] + item_weight <= value['total_capacity']
                and not is_recharge_threshold_met(value['battery'] - necessary_battery, value['total_battery'])):
            available_robots[key] = PriorityEntry(key, value)

    if len(available_robots) == 0 or robot_name not in available_robots.keys():
        return (False, len(available_robots))
    if len(available_robots) == 1 and robot_name in available_robots.keys():
        return (True, len(available_robots))

    return (robot_name == sorted(available_robots.values())[0].get_id(), len(available_robots))


def is_recharge_threshold_met(current_battery, total_battery):
    return current_battery / total_battery <= BATTERY_THRESHOLD

