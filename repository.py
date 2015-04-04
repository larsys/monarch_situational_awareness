# system level
import collections
import yaml
import exceptions
# ROS level
import rospy
import std_msgs.msg  # Header
import genpy
# package level
from slot import Slot
from monarch_situational_awareness.msg import SlotProperties, GroupProperties
from common import unspecify_name

class Repository(collections.MutableMapping):
    """
    A repository is a mutable collection of SA Slots that has the following
    nice properties:
    - Can be initialized using a YAML bagy of SlotProperties
      (see sam_client_tests/slot_config.yaml for an example);
    - Can be accessed as a dictionary using the name of the slot as key
      (average O(1));
    - Can be accessed as a list using the Slot ID
      (guaranteed O(1));
    - Iterates orderly over Slots (list behavior);
    - Does not allow duplicate Slots, Slot reassignments, or Slots that map
      to the same "alias" (i.e. with the same topic names).
    """
    def __init__(self, yaml_path=None):
        self._smap = {}
        self._slist = []
        self._aset = set()
        self._cb = None
        self._groups = {}
        self._sortl = None

        if yaml_path is not None:
            f = open(yaml_path, 'r')
            d = yaml.load(f)
            if type(d) is not dict:
                raise IOError("Bad syntax in the YAML configuration file.")
            singles = d.values()[0]
            groups = []
            if len(d.values()) > 1:
                groups = d.values()[1]
            any_shared = False
            for sp_args in singles:
                # Taken from rostopic / rosservice implementations
                try:
                    sp = SlotProperties()
                    now = rospy.get_rostime()
                    keys = {'now': now, 'auto': std_msgs.msg.Header(stamp=now)}
                    genpy.message.fill_message_args(sp, sp_args, keys=keys)
                    self.append(sp)
                    if sp.is_shared is True:
                        any_shared = True
                except genpy.MessageException as e:
                    raise IOError("Bad syntax in the YAML configuration file.")
                #-----------------------------------------------
            for gp_args in groups:
                try:
                    gp = GroupProperties()
                    now = rospy.get_rostime()
                    keys = {'now': now, 'auto': std_msgs.msg.Header(stamp=now)}
                    if 'hosts' not in gp_args.keys():
                        gp_args['hosts'] = []
                    genpy.message.fill_message_args(gp, gp_args, keys=keys)
                    self.append(gp)
                    if gp.representative.is_shared is True:
                        any_shared = True
                except genpy.MessageException as e:
                    raise IOError("Bad syntax in the YAML configuration file.")
            if any_shared is True:
                self._sort_shared_slots()

    def __contains__(self, key):
        if type(key) is int:
            return key < len(self._slist)
        elif type(key) is str:
            return key in self._smap
        else:
            raise TypeError("Invalid key type. You should access the SAR using"
                            " 'int' or 'str' keys.")

    def __getitem__(self, key):
        if type(key) is int:
            return self._slist[key]
        elif type(key) is str:
            return self._smap[key]
        else:
            raise TypeError("Invalid key type. You should access the SAR using"
                            " 'int' or 'str' keys.")

    def __setitem__(self, key, value):

        if type(key) is not str:
            raise TypeError("Invalid key type. You should use the Slot name "
                            "when adding new Slots.")

        if type(value) is not Slot:
            raise TypeError("Invalid value type (should be a Slot).")

        a = value.get_alias()
        if a in self._aset:
            raise IndexError("A Slot with the same alias already exists!"
                             " ('%s')" % a)
        if key in self._smap:
            raise IndexError("Slot '%s' already exists. It cannot be "
                             "reassigned!" % key)

        value.sid = len(self._slist)
        if value.host is not None:
            g_name = unspecify_name(value.name)
            if g_name in self._groups.keys():
                self._groups[g_name].add(value.sid)
            else:
                self._groups[g_name] = set()
                self._groups[g_name].add(value.sid)
        self._slist.append(value)
        self._smap[key] = value
        self._aset.add(a)

        rospy.loginfo("Slot '%s' created ( ID = %d ).",
                      value.name, value.sid)

    def __delitem__(self, key):
        if type(key) is int:
            s = self._slist[key]
            self._slist.remove(key)
            self._smap.pop(s.name)
            self._aset.remove(s.get_alias())
            if s.host is not None:
                self._groups[s.get_group_name()].remove(s.sid)
        elif type(key) is str:
            s = self._smap[key]
            sid = s.sid
            a = s.get_alias()
            self._slist.remove(sid)
            self._smap.pop(key)
            self._aset.remove(a)
            if s.host is not None:
                self._groups[s.get_group_name()].remove(s.sid)
        else:
            raise TypeError("Invalid key type. You should access the SAR using"
                            " 'int' or 'str' keys.")

    def __len__(self):
        return len(self._slist)

    def __iter__(self):
        return iter(self._slist)

    def append(self, item):
        if type(item) is SlotProperties:
            try:
                s = Slot(item)
                if self._cb is not None:
                    s.set_conn_update_callback(self._cb)
                self[item.name] = s
                if s.is_shared:
                    self._sort_shared_slots()
            except Exception as e:
                rospy.logerr(e)
                raise e
        elif type(item) is GroupProperties:
            try:
                hostnames = item.hosts
                if len(item.hosts) == 0:
                    hostnames = rospy.get_param("~default_group_hosts", [])
                any_shared = False
                for h in hostnames:
                    rep = item.representative
                    s = Slot(rep, h)
                    if self._cb is not None:
                        s.set_conn_update_callback(self._cb)
                    self[s.name] = s
                    if s.is_shared is True:
                        any_shared = True
                if any_shared is True:
                    self._sort_shared_slots()
            except Exception as e:
                rospy.logerr(e)
                raise e
        else:
            raise TypeError("Invalid item type ('%s')" % type(item))

    def bind_update_cb(self, cb):
        self._cb = cb
        for s in self._slist:
            s.set_conn_update_callback(cb)

    def get_repo_props(self):
        props = {}
        props['singles'] = []
        props['groups'] = []

        for s in self._slist:
            if s.host is None:
                sp = SlotProperties(name=s.name,
                                    description=s.description,
                                    type_str=s.type_str,
                                    is_shared=s.is_shared,
                                    is_latched=s.is_latched)
                props['singles'].append(sp)

        for g_name, g_set in self._groups.items():
            sid = g_set.pop()
            g_set.add(sid)
            rep = self[sid]
            sp = SlotProperties(name=unspecify_name(rep.name),
                                description=rep.description,
                                type_str=rep.type_str,
                                is_shared=rep.is_shared,
                                is_latched=rep.is_latched)
            gp = GroupProperties(representative=sp)
            for s in g_set:
                gp.hosts.append(self[s].host)
            props['groups'].append(gp)

        return props

    def get_slot_props(self, slot):
        return SlotProperties(name=slot.name,
                              description=slot.description,
                              type_str=slot.type_str,
                              is_shared=slot.is_shared,
                              is_latched=slot.is_latched)

    def get_group_names(self):
        return self._grnames

    def _sort_shared_slots(self):
        shared = [s for s in self._slist if s.is_shared is True]
        self._sortl = sorted(shared, key=Slot.get_name)

    def get_shared_slots(self):
        props = []
        if self._sortl is not None:
            for s in self._sortl:
                props.append(self.get_slot_props(s))
        return props
