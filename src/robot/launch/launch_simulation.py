import os

from ament_index_python.packages import get_package_share_directory

from launch import LaunchDescription
from launch.actions import GroupAction
from launch.actions import IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.actions import Node
from launch_ros.actions import PushRosNamespace


def generate_launch_description():
    metrics_collector = Node(
        package='metrics_collector',
        namespace='warehouse',
        executable='metrics_collector',
        name='metrics_collector'
    )

    # sector 0
    n = 20
    nodes_sector_a = [None] * (n + 1)
    nodes_sector_a[0] = PushRosNamespace('warehouse')
    for i in range(1, n + 1):
        nodes_sector_a[i] = IncludeLaunchDescription(
            PythonLaunchDescriptionSource([os.path.join(
                get_package_share_directory('robot'), 'launch'),
                '/launch_sector_a_robot.py']),
            launch_arguments={'robot_name': 'robot_' + str(i) + 'a'}.items()
        )
    sector_a_robots = GroupAction(actions=nodes_sector_a)

    # sector 1
    n = 10
    nodes_sector_b = [None] * (n + 1)
    nodes_sector_b[0] = PushRosNamespace('warehouse')
    for i in range(1, n + 1):
        nodes_sector_b[i] = IncludeLaunchDescription(
            PythonLaunchDescriptionSource([os.path.join(
                get_package_share_directory('robot'), 'launch'),
                '/launch_sector_b_robot.py']),
            launch_arguments={'robot_name': 'robot_' + str(i) + 'b'}.items()
        )
    sector_b_robots = GroupAction(actions=nodes_sector_b)

    # sector 2
    n = 50
    nodes_sector_c = [None] * (n + 1)
    nodes_sector_c[0] = PushRosNamespace('warehouse')
    for i in range(1, n + 1):
        nodes_sector_c[i] = IncludeLaunchDescription(
            PythonLaunchDescriptionSource([os.path.join(
                get_package_share_directory('robot'), 'launch'),
                '/launch_sector_c_robot.py']),
            launch_arguments={'robot_name': 'robot_' + str(i) + 'c'}.items()
        )
    sector_c_robots = GroupAction(actions=nodes_sector_c)

    # sector 3
    n = 50
    nodes_sector_d = [None] * (n + 1)
    nodes_sector_d[0] = PushRosNamespace('warehouse')
    for i in range(1, n + 1):
        nodes_sector_d[i] = IncludeLaunchDescription(
            PythonLaunchDescriptionSource([os.path.join(
                get_package_share_directory('robot'), 'launch'),
                '/launch_sector_d_robot.py']),
            launch_arguments={'robot_name': 'robot_' + str(i) + 'd'}.items()
        )
    sector_d_robots = GroupAction(actions=nodes_sector_d)

    return LaunchDescription([
        metrics_collector,
        sector_a_robots,
        #sector_b_robots,
        #sector_c_robots,
        #sector_d_robots
    ])
