from .calculation import battery_ratio, capacity_ratio, normal_round


class PriorityEntry(object):

    def __init__(self, id, item, destination):
        self.id = id
        self.item = item
        self.destination = destination

    def __str__(self):
        return "%s" % self.id

    def __lt__(self, other):
        self_origin = self.item['latest_item_position'] if self.item['latest_item_position'] else self.item['current_position']
        other_origin = self.item['latest_item_position'] if self.item['latest_item_position'] else self.item['current_position']

        if normal_round(self_origin.distance(self.destination)) == normal_round(other_origin.distance(self.destination)):
            if normal_round(battery_ratio(self.item['battery'], self.item['total_battery'])) == normal_round(battery_ratio(other.item['battery'], other.item['total_battery'])):
                if normal_round(capacity_ratio(self.item['current_capacity'], self.item['total_capacity'])) == normal_round(capacity_ratio(other.item['current_capacity'], other.item['total_capacity'])):
                    return self.item['priority'] < other.item['priority']
                return normal_round(capacity_ratio(self.item['current_capacity'], self.item['total_capacity'])) < normal_round(capacity_ratio(other.item['current_capacity'], other.item['total_capacity']))
            return normal_round(battery_ratio(self.item['battery'], self.item['total_battery'])) < normal_round(battery_ratio(other.item['battery'], other.item['total_battery']))
        return normal_round(self_origin.distance(self.destination) < other_origin.distance(self.destination))

    def get_id(self):
        return self.id
