class PriorityEntry(object):

    def __init__(self, timestamp, data):
        self.data = data
        self.timestamp = timestamp

    def __lt__(self, other):
        return self.timestamp > other.timestamp
