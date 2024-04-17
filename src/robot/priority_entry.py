class PriorityEntry(object):

    def __init__(self, id, item):
        self.id = id
        self.item = item

    def __str__(self):
        return "%s" % self.item

    def __lt__(self, other):
        if self.item['battery'] == other.item['battery']:
            return self.item['current_capacity'] < other.item['current_capacity']
        return self.item['battery'] < other.item['battery']

    def get_id(self):
        return self.id
