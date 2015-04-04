import functools
#ROS level
import rospy
import roslib.message
#package level
from common import *
from monarch_situational_awareness.srv import *


class Bridge():
    def __init__(self):
        self.opened = True
        self.pub = None
        self.sub = None

    def open(self):
        self.opened = True

    def close(self):
        self.opened = False


class MultimasterBridge():
    def __init__(self):
        self._prefix = rospy.get_param("~prefix", "multimaster_bridge")
        self._q_size_in = rospy.get_param("~input_queue_size", 1)
        self._q_size_out = rospy.get_param("~output_queue_size", 1)
        self._open_out_srv = \
            rospy.Service('multimaster_open_bridge_out',
                          MultimasterBridgeCmd,
                          self._open_out_callback)
        self._open_in_srv = \
            rospy.Service('multimaster_open_bridge_in',
                          MultimasterBridgeCmd,
                          self._open_in_callback)
        self._close_out_srv = \
            rospy.Service('multimaster_close_bridge_out',
                          MultimasterBridgeCmd,
                          self._close_out_callback)
        self._close_in_srv = \
            rospy.Service('multimaster_close_bridge_in',
                          MultimasterBridgeCmd,
                          self._close_in_callback)
        self._out_bridges = {}  # dict of Bridges
        self._in_bridges = {}  # dict of Bridges
        self._node_name = rospy.names.get_name()

    def _open_out_callback(self, msg):
        channel_name = "/" + sanitize_name("%s_%s" % (self._prefix, msg.key))
        """
        Must be an absolute topic name, so that all SAM instances refer to the same topic
        regardless of the namespace
        """
        reply = MultimasterBridgeCmdResponse()
        if msg.target_topic in self._out_bridges.keys():
            self._out_bridges[msg.target_topic].open()
        else:
            self._out_bridges[msg.target_topic] = \
                self._bridge(msg.target_topic,
                             channel_name,
                             msg.type_str)
        reply.success = True
        return reply

    def _open_in_callback(self, msg):
        channel_name = "/" + sanitize_name("%s_%s" % (self._prefix, msg.key))
        """
        Must be an absolute topic name, so that all SAM instances refer to the same topic
        regardless of the namespace
        """
        reply = MultimasterBridgeCmdResponse()
        if msg.target_topic in self._in_bridges.keys():
            self._in_bridges[msg.target_topic].open()
        else:
            self._in_bridges[msg.target_topic] = \
                self._bridge(channel_name,
                             msg.target_topic,
                             msg.type_str)
        reply.success = True
        return reply

    def _close_out_callback(self, msg):
        reply = MultimasterBridgeCmdResponse()
        try:
            self._out_bridges[msg.target_topic].close()
            reply.success = True
        except KeyError:
            pass
        return reply

    def _close_in_callback(self, msg):
        reply = MultimasterBridgeCmdResponse()
        try:
            self._in_bridges[msg.target_topic].close()
            reply.success = True
        except KeyError:
            pass
        return reply

    def _data_callback(self, m, p, b):
        try:
            if (b.opened is True
                    and m._connection_header['callerid'] != self._node_name):
                p.publish(m)
        except rospy.exceptions.ROSException as e:
            rospy.logerr(e.message)

    def _bridge(self, input_topic, output_topic, type_str):
        type_class = roslib.message.get_message_class(type_str)
        bridge = Bridge()
        pub = rospy.Publisher(output_topic,
                              type_class,
                              queue_size=self._q_size_out)
        sub = rospy.Subscriber(input_topic,
                               type_class,
                               functools.partial(self._data_callback,
                                                 p=pub, b=bridge),
                               queue_size=self._q_size_in)
        bridge.pub = pub
        bridge.sub = sub

        return bridge
