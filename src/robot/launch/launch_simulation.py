import os

from ament_index_python.packages import get_package_share_directory

from launch import LaunchDescription
from launch.actions import GroupAction
from launch.actions import IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.actions import PushRosNamespace


def generate_launch_description():
    sector_a_robot_one = IncludeLaunchDescription(
        PythonLaunchDescriptionSource([os.path.join(
            get_package_share_directory('robot'), 'launch'),
            '/launch_sector_a_robot.py']),
        launch_arguments={'robot_name': 'robot_1a'}.items()
    )

    sector_a_robot_two = IncludeLaunchDescription(
        PythonLaunchDescriptionSource([os.path.join(
            get_package_share_directory('robot'), 'launch'),
            '/launch_sector_a_robot.py']),
        launch_arguments={'robot_name': 'robot_2a'}.items()
    )

    sector_a_robots = GroupAction(
        actions=[
            PushRosNamespace('sector_a'),
            sector_a_robot_one,
            sector_a_robot_two,
        ]
    )

    sector_b_robot_one = IncludeLaunchDescription(
        PythonLaunchDescriptionSource([os.path.join(
            get_package_share_directory('robot'), 'launch'),
            '/launch_sector_b_robot.py']),
        launch_arguments={'robot_name': 'robot_1b'}.items()
    )

    sector_b_robot_two = IncludeLaunchDescription(
        PythonLaunchDescriptionSource([os.path.join(
            get_package_share_directory('robot'), 'launch'),
            '/launch_sector_b_robot.py']),
        launch_arguments={'robot_name': 'robot_2b'}.items()
    )

    sector_b_robots = GroupAction(
        actions=[
            PushRosNamespace('sector_b'),
            sector_b_robot_one,
            sector_b_robot_two
        ]
    )

    # turtlesim_world_1 = IncludeLaunchDescription(
    #     PythonLaunchDescriptionSource([os.path.join(
    #         get_package_share_directory('launch_tutorial'), 'launch'),
    #         '/turtlesim_world_1_launch.py'])
    # )
    # broadcaster_listener_nodes = IncludeLaunchDescription(
    #     PythonLaunchDescriptionSource([os.path.join(
    #         get_package_share_directory('launch_tutorial'), 'launch'),
    #         '/broadcaster_listener_launch.py']),
    #     launch_arguments={'target_frame': 'carrot1'}.items(),
    # )
    # mimic_node = IncludeLaunchDescription(
    #     PythonLaunchDescriptionSource([os.path.join(
    #         get_package_share_directory('launch_tutorial'), 'launch'),
    #         '/mimic_launch.py'])
    # )
    # fixed_frame_node = IncludeLaunchDescription(
    #     PythonLaunchDescriptionSource([os.path.join(
    #         get_package_share_directory('launch_tutorial'), 'launch'),
    #         '/fixed_broadcaster_launch.py'])
    # )
    # rviz_node = IncludeLaunchDescription(
    #     PythonLaunchDescriptionSource([os.path.join(
    #         get_package_share_directory('launch_tutorial'), 'launch'),
    #         '/turtlesim_rviz_launch.py'])
    # )

    return LaunchDescription([
        sector_a_robots,
        # sector_b_robots
    ])