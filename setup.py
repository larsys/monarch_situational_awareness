from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=['sam_helpers'],
    package_dir={'': ''},
    scripts=['sam_node'],
    requires=['std_msgs','rospy']
)

setup(**d)
