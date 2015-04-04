#system level
import exceptions
import sys
#ROS level
import rospy
import roslib
import rosgraph  # rosgraph.masterapi.Master('/rostopic')
#package level
from common import *

anonymize = True


class Slot(rospy.SubscribeListener):
    """
    A Situational Awareness Slot is a container of abstract data that
    can be made accessible to clients operating in different
    network locations. The same Slot can be replicated in different
    Situational Awareness Repositories, in which case SAM tries to
    synhcronize the data contained in each copy. Slots can be associated
    with a set of desired access characteristics.

    The access to a Slot is mediated by a Manager.

    The Slot class inherits from rospy.SubscribeListener so that it can
    validate incoming connections to its output topic.
    """
    def get_name(self):
        return self._name

    def set_name(self, value):
        if value is not None:
            n = value
            if self.host is not None:
                n = specify_name(value, self.host)
            self._name = n
            self._alias = sanitize_name(n)
    name = property(get_name, set_name)

    def __init__(self,
                 props,
                 hostname=None):
        self.host = hostname
        """The *absolute* name of the slot"""

        self.name = props.name
        """A string specifying the type of the data that is to be contained in
        the slot"""
        self.type_str = props.type_str
        """A readable, short explanation of the slot's
        purpose"""
        self.description = None
        try:
            self.description = props.description
        except exceptions.AttributeError:
            pass
        """Whether or not this Slot is shared across multiple SARs"""
        self.is_shared = False
        try:
            self.is_shared = props.is_shared
        except exceptions.AttributeError:
            pass
        """Whether or not the output topic is latched"""
        self.is_latched = False
        try:
            self.is_latched = props.is_latched
        except exceptions.AttributeError:
            pass

        """An integer identifier that is unique in the local SAR"""
        self.sid = None
        """
        The name of the client that is registered to write into this
        Slot
        """
        self.writer = None
        """
        The set of names of the clients that are registered to read from
        this Slot
        """
        self.readers = set()

        self._allowed_publisher = None
        self._allowed_subscribers = set()
        self._allow_recording = rospy.get_param("~allow_recording", True)
        
        try:
            type_class = roslib.message.get_message_class(self.type_str)
            if type_class is None:  # 'None' isn't actually an exception
                raise
        except:
            raise SAMTypeException(self.type_str, "Unknown data type.")

        s_in = '%s_input' % self._alias
        s_out = '%s_output' % self._alias

        anonymize_topics = rospy.get_param('~anonymize_topics', False)
        self.mm_bridge_node_name = rospy.get_param('~mm_bridge_node_name',
                                                   None)

        if anonymize_topics is True:
            s_in = scramble_string(s_in)
            s_out = scramble_string(s_out)

        self._input_topic_name = ('%ssar/' % rospy.get_namespace() + s_in)
        self._output_topic_name = ('%ssar/' % rospy.get_namespace() + s_out)

        """Slot data subscriber"""
        self._slot_sub = rospy.Subscriber(
            self._input_topic_name,
            type_class,
            self._data_callback)
        """Slot data publisher"""
        self._slot_pub = rospy.Publisher(
            self._output_topic_name,
            type_class,
            subscriber_listener=self,
            latch=self.is_latched,
            queue_size=1)

        self._conn_update_callback = None
        self._rate_window = rospy.get_param('~rate_window', 10)
        self._rate_counter = 0
        self._avg_rate = None
        self._rate_update_time = None

    def _data_callback(self, msg):
        """
        The callback that is triggered when new data is written into the
        Slot. The data is ignored if the sender is not registered as a
        writer.
        """
        sender = msg._connection_header['callerid']

        if (sender != self.mm_bridge_node_name
            and (self._allowed_publisher is None
                 or sender != self._allowed_publisher)):
            rospy.logerr("Slot '%s' received data from unregistered writer "
                         "'%s'. Ignoring incoming data.",
                         self.name, sender)
            return

        self._slot_pub.publish(msg)
        self._rate_counter -= 1
        if self._rate_counter <= 0:
            self._rate_counter = self._rate_window
            self._update_rate()

    def _get_active_subscribers(self):
        master = rosgraph.masterapi.Master('/rostopic')
        try:
            state = master.getSystemState()
        except socket.error:
            raise SAMConnectionException("Unable to communicate with master!")

        _, all_subs, _ = state

        for x in all_subs:
            if x[0] == self._output_topic_name:
                return x[1]

        return []

    def peer_subscribe(self, topic_name, topic_publish, peer_publish):
        """
        The callback that is triggered whenever a new client connects to the
        output topic of this slot.
        """
        subs = self._get_active_subscribers()
        if len(subs) > 0:
            new_sub = subs[-1]
            """the newest subscriber is guaranteed to be the last of those with
            our topic name."""
            if (new_sub not in self._allowed_subscribers
                    and new_sub != self.mm_bridge_node_name
                    and not (self._allow_recording is True
                             and new_sub.find("/record_") != -1)):
                raise rospy.exceptions.TransportTerminated(
                    "Slot %s: Unregistered SA Read request from client '%s'"
                    % (self.name, new_sub))

    def peer_unsubscribe(self, topic_name, num_peers):
        """
        The callback that is triggered whenever an existing client
        unregisters as a subscriber of the output topic of this slot.
        """
        if self._conn_update_callback is not None:
            self._conn_update_callback(self.sid)

    def register_reader(self, client_id, direct_access=False):
        self.readers.add(client_id)
        if direct_access is True:
            self._allowed_subscribers.add(client_id.name)

    def register_writer(self, client_id, direct_access=False):
        if self.writer is not None:
            rospy.logwarn("Slot '%s' already had a writer (%s). It is being "
                          "replaced with '%s'", self._name, self.writer.name,
                          client_id.name)
        self.writer = client_id
        if direct_access is True:
            self._allowed_publisher = client_id.name

    def remove_reader(self, reader_id):
        try:
            r = next(c for c in self.readers
                     if c.name == reader_id.name and c.host == reader_id.host)
            self.readers.remove(r)
            rospy.loginfo("'%s' is no longer a Reader of Slot '%s'",
                          reader_id.name, self.name)
        except:
            pass

    def remove_writer(self):
        old_writer = self.writer
        if old_writer is not None:
            rospy.loginfo("'%s' is no longer the Writer of Slot '%s'",
                          old_writer.name, self.name)
            self.writer = None

    def get_alias(self):
        return self._alias

    def get_input_topic_name(self):
        return self._input_topic_name

    def get_output_topic_name(self):
        return self._output_topic_name

    def set_conn_update_callback(self, callback):
        self._conn_update_callback = callback

    def _update_rate(self):
        if self._rate_update_time is None:
            self._rate_update_time = rospy.Time.now()
            return

        t = rospy.Time.now()
        delta = t - self._rate_update_time
        self._avg_rate = self._rate_window/delta.to_sec()
        self._rate_update_time = rospy.Time.now()

    def get_readable_rate(self):
        if self._avg_rate is not None:
            return '%0.1f Hz' % self._avg_rate
        else:
            return '--'

    def get_readable_throughput(self):
        if self._avg_rate is None:
            return '--'
        tp = self._avg_rate * sys.getsizeof(
            roslib.message.get_message_class(self.type_str))
        if tp > 1024**2:
            return '%0.1f %s' % (tp/(1024**2), 'MB/s')
        if tp > 1024:
            return '%0.1f %s' % (tp/1024, 'KB/s')
        return '%0.1f %s' % (tp, 'B/s')
