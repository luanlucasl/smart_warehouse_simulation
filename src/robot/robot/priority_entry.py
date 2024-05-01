from .calculation import battery_ratio


class PriorityEntry(object):

    def __init__(self, id, item):
        self.id = id
        self.item = item

    def __str__(self):
        return "%s" % self.id

    def __lt__(self, other):
        if self.item['battery'] == other.item['battery']:
            if self.item['current_capacity'] == other.item['current_capacity']:
                return self.id < other.id
            return self.item['current_capacity'] < other.item['current_capacity']
        return (battery_ratio(self.item['battery'], self.item['total_battery']) <
                battery_ratio(other.item['battery'], other.item['total_battery']))

    def get_id(self):
        return self.id
