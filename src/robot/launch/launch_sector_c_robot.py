import os

from ament_index_python.packages import get_package_share_directory

from launch import LaunchDescription
from launch_ros.actions import Node
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    robot_name = LaunchConfiguration('robot_name')

    config = os.path.join(
        get_package_share_directory('robot'),
        'config',
        'sector_c_common.yaml'
    )

    return LaunchDescription([
        Node(
            package='robot',
            executable='robot',
            name=robot_name,
            parameters=[config]
        )
    ])