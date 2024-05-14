from .calculation import battery_ratio, normal_round


class PriorityEntry(object):

    def __init__(self, id, item, destination):
        self.id = id
        self.item = item
        self.destination = destination

    def __str__(self):
        return "%s" % self.id

    def __lt__(self, other):
        if normal_round(self.item['current_position'].distance(self.destination)) == normal_round(other.item['current_position'].distance(self.destination)):
            if normal_round(battery_ratio(self.item['battery'], self.item['total_battery'])) == normal_round(battery_ratio(other.item['battery'], other.item['total_battery'])):
                if self.item['current_capacity'] == other.item['current_capacity']:
                    return self.id < other.id
                return self.item['current_capacity'] < other.item['current_capacity']
            return normal_round(battery_ratio(self.item['battery'], self.item['total_battery'])) < normal_round(battery_ratio(other.item['battery'], other.item['total_battery']))
        return normal_round(self.item['current_position'].distance(self.destination)) < normal_round(other.item['current_position'].distance(self.destination))

    def get_id(self):
        return self.id
