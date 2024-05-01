import math


class Point(dict):

    def __init__(self, x, y):
        dict.__init__(self, x=x, y=y)
        self.x = x
        self.y = y

    def move(self, dx, dy):
        self.x = self.x + dx
        self.x = self.x + dy

    def __str__(self):
        return "Point(%s, %s)" % (self.x, self.y)

    def __eq__(self, other):
        if isinstance(other, Point):
            return self.x == other.x and self.y == other.y
        return False

    def get_x(self):
        return self.x

    def get_y(self):
        return self.y

    def distance(self, other):
        dx = self.x - other.x
        dy = self.y - other.y
        return int(self.__normal_round(math.hypot(dx, dy)))

    def __normal_round(self, n):
        if n - math.floor(n) < 0.5:
            return math.floor(n)
        return math.ceil(n)
