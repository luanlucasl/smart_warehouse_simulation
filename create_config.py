import math


SECTORS = 4
# Sector 0
SECTOR_0_MIN_COORDINATE_X = 0
SECTOR_0_MAX_COORDINATE_X = 29
SECTOR_0_MIN_COORDINATE_Y = 0
SECTOR_0_MAX_COORDINATE_Y = 29

# Sector 1
SECTOR_1_MIN_COORDINATE_X = 0
SECTOR_1_MAX_COORDINATE_X = 29
SECTOR_1_MIN_COORDINATE_Y = SECTOR_0_MAX_COORDINATE_Y + 1
SECTOR_1_MAX_COORDINATE_Y = SECTOR_1_MIN_COORDINATE_Y + 29

# Sector 2
SECTOR_2_MIN_COORDINATE_X = SECTOR_0_MAX_COORDINATE_X + 1
SECTOR_2_MAX_COORDINATE_X = SECTOR_2_MIN_COORDINATE_X + 29
SECTOR_2_MIN_COORDINATE_Y = 0
SECTOR_2_MAX_COORDINATE_Y = 29

# Sector 3
SECTOR_3_MIN_COORDINATE_X = SECTOR_1_MAX_COORDINATE_X + 1
SECTOR_3_MAX_COORDINATE_X = SECTOR_3_MIN_COORDINATE_X + 29
SECTOR_3_MIN_COORDINATE_Y = SECTOR_2_MAX_COORDINATE_Y + 1
SECTOR_3_MAX_COORDINATE_Y = SECTOR_3_MIN_COORDINATE_Y + 29

def generate(sector, number_of_robots, delivery_station_position, startX, startY, limitX, limitY):
    delivery_station = [0, 0]
    robotNumber = 1
    # bottom will have one additional robot in case the number of robots is odd
    robots = math.floor(number_of_robots / SECTORS)
    remainder = number_of_robots % SECTORS

    print("")
    print("sector " + sector)
    # bottom
    robots_bottom = robots if remainder == 0 else robots + 1
    # calculation on the right-hand side tries to find the starting position
    # to keep robots balanced in the "middle"
    start_bottom = startY + 1 + math.ceil(((limitY - startY + 1 - robots_bottom) / 2))
    remainder -= 1
    for j in range(start_bottom, min(start_bottom + robots_bottom, limitY)):
        print("/warehouse/robot_" + str(robotNumber) + str(sector) + ":")
        print("  ros__parameters:")
        print("    initial_position: [" + str(startX) + ", " + str(j) + "]")
        robotNumber += 1
		
    # left-hand side
    robots_left = robots if remainder <= 0 else robots + 1
    robots_left = robots_left + 1 if delivery_station_position == 0 else robots_left
    start_left = startX + 1 + math.ceil(((limitX - startX + 1 - robots_left) / 2))
    remainder -= 1
    for j in range(start_left, min(start_left + robots_left, limitX)):
        if delivery_station_position == 0 and j == (start_left + min(start_left + robots_left, limitX)) // 2:
            delivery_station[0] = j
            delivery_station[1] = startY
            continue
        print("/warehouse/robot_" + str(robotNumber) + str(sector) + ":")
        print("  ros__parameters:");
        print("    initial_position: [" + str(j) + ", " + str(startY) + "]")
        robotNumber += 1

    # right-hand side
    robots_right = robots if remainder <= 0 else robots + 1
    robots_right = robots_right + 1 if delivery_station_position == 1 else robots_right
    start_right = startX + 1 + math.ceil(((limitX - startX + 1 - robots_right) / 2))
    for j in range(start_right, min(start_right + robots_right, limitX)):
        if delivery_station_position == 1 and j == (start_right + min(start_right + robots_right, limitX)) // 2:
            delivery_station[0] = j
            delivery_station[1] = limitY
            continue
        print("/warehouse/robot_" + str(robotNumber) + str(sector) + ":")
        print("  ros__parameters:");
        print("    initial_position: [" + str(j) + ", " + str(limitY) + "]")
        robotNumber += 1

    # top side
    start_top = startY + 1 + math.ceil(((limitY - startY + 1 - robots) / 2))
    for j in range(start_top, min(start_top + robots, limitY)):
        print("/warehouse/robot_" + str(robotNumber) + str(sector) + ":");
        print("  ros__parameters:");
        print("    initial_position: [" + str(limitX) + ", " + str(j) + "]");
        robotNumber += 1
    
    print("delivery_station: " + str(delivery_station))


if __name__ == '__main__':
    result = list()
    generate('a', 50, 0, SECTOR_0_MIN_COORDINATE_X, SECTOR_0_MIN_COORDINATE_Y, SECTOR_0_MAX_COORDINATE_X, SECTOR_0_MAX_COORDINATE_Y)
    generate('b', 50, 1, SECTOR_1_MIN_COORDINATE_X, SECTOR_1_MIN_COORDINATE_Y, SECTOR_1_MAX_COORDINATE_X, SECTOR_1_MAX_COORDINATE_Y)
    generate('c', 50, 0, SECTOR_2_MIN_COORDINATE_X, SECTOR_2_MIN_COORDINATE_Y, SECTOR_2_MAX_COORDINATE_X, SECTOR_2_MAX_COORDINATE_Y)
    generate('d', 50, 1, SECTOR_3_MIN_COORDINATE_X, SECTOR_3_MIN_COORDINATE_Y, SECTOR_3_MAX_COORDINATE_X, SECTOR_3_MAX_COORDINATE_Y)
