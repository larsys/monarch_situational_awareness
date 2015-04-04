#system level
import socket  # gethostname
import copy
import hashlib
import threading
#ROS level
import rospy
import roslib
import roslib.message
import rosgraph   # rosgraph.masterapi.Master('/rostopic')
import rosservice  # get_service_node()
import std_srvs.srv
#package level
from monarch_situational_awareness.srv import *
from monarch_situational_awareness.msg import *
from slot import Slot, specify_name
from common import *

try:
    import pydot  # print_dep_graph
    import rospkg
    has_pydot = True
except ImportError:
    has_pydot = False


class TarjanNode():
    """
    A struct that is for loop-checking purposes, as part of the Tarjan SCC
    algorithm.
    """
    def __init__(self, idx):
        self.index = idx
        self.lowlink = idx


class Manager():
    """
    The SAR Manager manages connections to and from a Repository.
    It provides the interface through which clients can access the SAM, as a
    set of ROS services. It also ensures that the graph resulting from these
    connections is loopless.
    """
    def __init__(self, repo):
        self._lock = threading.Lock()
        self._lock.acquire()
        
        self.host = rospy.get_param("~host", socket.gethostname())
        allowed_hosts = rospy.get_param("~agent_names", [])
        if len(allowed_hosts) == 0:
            self._known_hosts = {self.host:0}
            self._static_hosts = False
        else:
            self._known_hosts = {}
            for (h,i) in zip(sorted(allowed_hosts),range(len(allowed_hosts)+1)):
                self._known_hosts[h] = i
            self._static_hosts = True
        
        self._hb_period = rospy.Duration(rospy.get_param("~heartbeat_period", 5.0))
        self._ka_period = rospy.Duration(rospy.get_param('~keepalive_period', 20))
        connect_mm_bridge = rospy.get_param('~connect_to_multimaster_bridge',
                                            False)
        self._allow_asynch_comms = rospy.get_param("~allow_asynch_comms",
                                                   True)
        self._synch_timeout = rospy.Duration(rospy.get_param('~synch_timeout',
                                                             20))
        """This is the actual SA Repository"""
        self.repo = repo

        """
        Information regarding known clients, a dictionary of ClientProps
        accessible via ClientIDs
        """
        self.clients = {}

        """
        The dependency graph, a dicts of sets
        {slot ID | (dependent slot IDs)}. Slots are indexed by IDs to
        minimize the complexity of the graph validation process.
        """
        self.slot_dep_graph = {}

        """
        The strongly connected components for each slot. This lets us know
        which slots are in a loop.
        """
        self.slot_scc = {}
        self.scc_count = 0
        """SAM Services"""
        """Service to create a reader of a Slot"""
        self._create_reader_server = \
            rospy.Service('create_reader',
                          CreateReader,
                          self._create_reader_srv_callback)
        """Service to create a writer to a Slot"""
        self._create_writer_server = \
            rospy.Service('create_writer',
                          CreateWriter,
                          self._create_writer_srv_callback)
        """Service to remove a reader from a Slot"""
        self._remove_reader_server = \
            rospy.Service('remove_reader',
                          RemoveReader,
                          self._remove_reader_srv_callback)
        """Service to remove the writer of a Slot"""
        self._remove_writer_server = \
            rospy.Service('remove_writer',
                          RemoveWriter,
                          self._remove_writer_srv_callback)
        self._remove_client_server = \
            rospy.Service('remove_client',
                          RemoveClient,
                          self._remove_client_srv_callback)
        """Service to create a Slot"""
        self._create_slot_server = \
            rospy.Service('create_slot',
                          CreateSlot,
                          self._create_slot_srv_callback)
        """
        Service to list out the information regarding all of the
        Slots in the local SAR
        """
        self._get_slot_info_server = \
            rospy.Service('get_slot_info',
                          GetSlotInfo,
                          self._get_slot_info_srv_callback)

        """
        Service to list out the information regarding all of the Slots in
        the local SAR
        """
        self._list_slot_info_server = \
            rospy.Service('list_slot_info',
                          ListSlotInfo,
                          self._list_slot_info_srv_callback)

        """
        Service to print the SAR connection graph for visualization and
        debugging
        """
        self._print_dep_graph_server = \
            rospy.Service('print_dep_graph',
                          std_srvs.srv.Empty,
                          self._print_dep_graph_srv_callback)

        self.repo.bind_update_cb(self._update_slot_connections)

        q_size = 10  # len(self._known_hosts)
        self._state_sub = rospy.Subscriber("manager_states_in",
                                           ManagerState,
                                           self._state_callback,
                                           queue_size=q_size)
        self._state_pub = rospy.Publisher("manager_states_out",
                                          ManagerState,
                                          queue_size=q_size)
        self._active_hosts_pub = rospy.Publisher("active_hosts",
                                                 HostnameArray,
                                                 queue_size=1,
                                                 latch=True)

        if connect_mm_bridge:
            from multimaster_msgs_fkie.msg import MasterState
            self._master_discovery_sub = rospy.Subscriber("master_discovery/changes",
                                                          MasterState,
                                                          self._master_discovery_callback,
                                                          queue_size=1)
            
            connected = False
            interrupted = False
            while not connected and not interrupted:
                try:
                    rospy.wait_for_service('multimaster_open_bridge_in', 2.0)
                    rospy.wait_for_service('multimaster_open_bridge_out', 2.0)
                    rospy.wait_for_service('multimaster_close_bridge_in', 2.0)
                    rospy.wait_for_service('multimaster_close_bridge_out', 2.0)
                    connected = True
                    self._mm_bridge_in = \
                        rospy.ServiceProxy('multimaster_open_bridge_in',
                                           MultimasterBridgeCmd,
                                           persistent=True)
                    self._mm_bridge_out = \
                        rospy.ServiceProxy('multimaster_open_bridge_out',
                                           MultimasterBridgeCmd,
                                           persistent=True)
                    self._mm_close_in = \
                        rospy.ServiceProxy('multimaster_close_bridge_in',
                                           MultimasterBridgeCmd,
                                           persistent=True)
                    self._mm_close_out = \
                        rospy.ServiceProxy('multimaster_close_bridge_out',
                                           MultimasterBridgeCmd,
                                           persistent=True)
                    self._mm_bridge_out(
                        target_topic="manager_states_out",
                        type_str="monarch_situational_awareness/ManagerState",
                        key="manager_states")
                    self._mm_bridge_in(
                        target_topic="manager_states_in",
                        type_str="monarch_situational_awareness/ManagerState",
                        key="manager_states")

                    rospy.loginfo("SAM:: Connected to the multimaster bridge.")
                    mm_bridge_node_name = rosservice.get_service_node(
                        rospy.names.get_namespace() +
                        'multimaster_open_bridge_in')
                    rospy.set_param("~mm_bridge_node_name",
                                    mm_bridge_node_name)
                    for slot in self.repo:
                        slot.mm_bridge_node_name = mm_bridge_node_name

                except rospy.exceptions.ROSInterruptException:
                    interrupted = True
                except rospy.exceptions.ROSException as e:
                    rospy.logwarn(e.message)
        else:
            self._mm_bridge_in = None
            self._mm_bridge_out = None
            self._mm_close_in = None
            self._mm_close_out = None
            rospy.logwarn("SAM:: Not connected to the multimaster bridge. "
                          "Proceeding in local mode.")

        for slot in self.repo:
            self.slot_dep_graph[slot.sid] = set()

        rel = len(self._known_hosts)*[ManagerRelations.NO_DATA]
        self._state = ManagerState(host=self.host,
                                   status_flag=ManagerFlags.SYNCH_ATTEMPT,
                                   relations=rel)
        self._update_public_checksum()

        try:
            hostid = self._known_hosts[self.host]
        except KeyError:
            err = ("SAM:: The allowed_hosts list does not contain the name of this "
                   "Managers' host (%s)! Exiting." % self.host)
            rospy.logfatal(err)
            raise RuntimeError(err)
        self._state.relations[hostid] = ManagerRelations.SYNCHRONIZED

        self._keepalive_timer = rospy.Timer(self._ka_period,
                                            self._conn_keepalive_callback)
        self._heartbeat_timer = rospy.Timer(self._hb_period,
                                            self._heartbeat_callback)
        self._synch_timeout_timer = None
        self._host_times = [rospy.Time.now()]*len(self._known_hosts)
        self._publish_active_hosts()
        self._lock.release()
        
    def _create_writer(self, client_id, slot_name):
        if slot_name not in self.repo:
            rospy.logerr("SAM:: Trying to create a Writer to a Slot that "
                         "does not exist (%s).", slot_name)
            return False

        slot = self.repo[slot_name]

        if slot.writer is not None:
            if slot.writer == client_id:
                rospy.logwarn("SAM:: '%s' was already the Writer of Slot '%s'",
                              client_id.name, slot.name)
                return True
            else:
                rospy.logerr("SAM:: Slot '%s' already has a writer ('%s'). "
                             "Ignoring request.", slot_name, slot.writer.name)
                return False

        if client_id in self.clients.keys():
            parent_sids = self.clients[client_id].reads_from
            new_connections = set()
            for p_sid in parent_sids:
                if p_sid not in self.slot_dep_graph:
                    rospy.logerr("SAM:: Error reading slot '%s'. "
                                 "The dependency graph is not initialized.",
                                 slot_name)
                    return False
                p_deps = self.slot_dep_graph[p_sid]
                """We check the connections that already exist because we
                don't want to remove those in case the graph check fails"""
                if slot.sid not in p_deps:
                    p_deps.add(slot.sid)
                    new_connections.add(p_sid)
            for p_sid in parent_sids:
                if not self._check_loops(p_sid):
                    rospy.logwarn("SAM:: ATTENTION: Registering '%s' as a Writer to Slot "
                                 "'%s' forms a cyclic dependency!",
                                 client_id.name, slot_name)
                    rospy.logwarn("SAM:: Cyclic dependencies can cause serious functional problems.")
                    rospy.logwarn("SAM:: Proceeding, but please verify that this behavior is intended.")
                    
            self.clients[client_id].writes_to.add(slot.sid)
        else:
            self.clients[client_id] = ClientProps(writes_to=set([slot.sid]))

        is_local = client_id.host == self.host
        slot.register_writer(client_id, is_local)
        self._reenable_keepalive()
        if is_local:
            n = client_id.name
        else:
            n = specify_name(client_id.name, client_id.host)
        rospy.loginfo("SAM:: Client '%s' is now the Writer of Slot '%s'",
                      n, slot_name)
        return True

    def _create_reader(self, client_id, slot_name):
        if slot_name not in self.repo:
            rospy.logerr("SAM:: Trying to create a Reader of a Slot that "
                         "does not exist (%s).", slot_name)
            return False

        slot = self.repo[slot_name]
        if slot.sid not in self.slot_dep_graph:
            rospy.logerr("SAM:: Error reading slot '%s'. "
                         "The dependency graph is not initialized.", slot_name)
            return False

        slot_deps = self.slot_dep_graph[slot.sid]
        client_keys = self.clients.keys()
        if client_id in client_keys:
            if slot.sid in self.clients[client_id].reads_from:
                rospy.logwarn("SAM:: The Reader '%s' was already registered for "
                              "Slot '%s'.", client_id.name, slot_name)
                return True

            write_targets = self.clients[client_id].writes_to.copy()
            for child_sid in write_targets:
                if child_sid is not None and child_sid not in slot_deps:
                #there is a write target, so there will be a new dependency
                    slot_deps.add(child_sid)

                    if not self._check_loops(slot.sid):
                        rospy.logwarn("SAM:: ATTENTION: Registering '%s' as a Reader of Slot "
                                      "'%s' forms a cyclic dependency!",
                                      client_id.name, slot_name)
                        rospy.logwarn("SAM:: Cyclic dependencies can cause serious functional problems.")
                        rospy.logwarn("SAM:: Proceeding, but please verify that this behavior is intended.")
                        
            self.clients[client_id].reads_from.add(slot.sid)
        else:
            self.clients[client_id] = ClientProps(reads_from=
                                                  set([slot.sid]))

        is_local = client_id.host == self.host
        slot.register_reader(client_id, is_local)
        self._reenable_keepalive()
        if is_local:
            n = client_id.name
        else:
            n = specify_name(client_id.name, client_id.host)
        rospy.loginfo("SAM:: Client '%s' is now a Reader of Slot '%s'",
                      n, slot_name)
        return True

    def _create_reader_srv_callback(self, req):
        """
        Callback for the reader creation service. It succeeds if the following
        conditions are satisfied:
        - The requested Slot exists;
        - Associating the caller as a Reader does not form a cyclic dependency
          between SA Slots.
        """
        self._lock.acquire()
        c_name = req._connection_header['callerid']
        c_id = ClientID(c_name, self.host)
        reply = CreateReaderResponse()

        s_name = req.properties.slot_name
        if len(req.properties.agent_name) > 0:
            s_name = specify_name(req.properties.slot_name,
                                  req.properties.agent_name)

        reply.success = self._create_reader(c_id, s_name)
        if reply.success:
            s = self.repo[s_name]
            reply.topic_name = s.get_output_topic_name()
            if s.is_shared:
                self._create_reader_state_update(c_id, s_name)
                if self._mm_bridge_in is not None:
                    self._mm_bridge_in(target_topic=s.get_input_topic_name(),
                                       type_str=s.type_str,
                                       key=s.name)
        self._lock.release()
        return reply

    def _create_reader_state_update(self, client_id, slot_name):
        self._update_public_checksum()
        if self._state.status_flag != ManagerFlags.SYNCH_ATTEMPT:
            self._state.status_flag = ManagerFlags.UPDATE_PENDING
            cp = ClientProperties(name=client_id.name,
                                  host=client_id.host,
                                  reads_from_names=[slot_name])
            add_clients = self._state.update.add_clients
            if len(add_clients) > 0:
                try:
                    c_old = next(c for c in add_clients
                                 if c.name == client_id.name)
                    c_old.reads_from_names.append(slot_name)
                except StopIteration:
                    self._state.update.add_clients.append(cp)
            else:
                self._state.update.add_clients.append(cp)
            self._prepare_state_publication()

    def _create_writer_srv_callback(self, req):
        """
        Callback for the writer creation service. It succeeds if the following
        conditions are satisfied:
        - The requested Slot exists;
        - The requested Slot does not have a Writer;
        - Associating the caller as a Writer does not form a cyclic dependency
          between SA Slots.
        """
        self._lock.acquire()
        reply = CreateWriterResponse()
        c_name = req._connection_header['callerid']
        c_id = ClientID(c_name, self.host)
        s_name = req.properties.slot_name
        if len(req.properties.agent_name) > 0:
            s_name = specify_name(req.properties.slot_name,
                                  req.properties.agent_name)

        reply.success = self._create_writer(c_id, s_name)

        if reply.success:
            s = self.repo[s_name]
            reply.topic_name = s.get_input_topic_name()

            if s.is_shared:
                self._create_writer_state_update(c_id, s_name)
                if self._mm_bridge_out is not None:
                    self._mm_bridge_out(target_topic=s.get_output_topic_name(),
                                        type_str=s.type_str,
                                        key=s.name)
        self._lock.release()
        return reply

    def _create_writer_state_update(self, client_id, slot_name):
        self._update_public_checksum()
        if self._state.status_flag != ManagerFlags.SYNCH_ATTEMPT:
            self._state.status_flag = ManagerFlags.UPDATE_PENDING
            cp = ClientProperties(name=client_id.name,
                                  host=client_id.host,
                                  writes_to_names=[slot_name])
            add_clients = self._state.update.add_clients
            if len(add_clients) > 0:
                try:
                    c_old = next(c for c in add_clients
                                 if c.name == client_id.name)
                    c_old.writes_to_names.append(slot_name)
                except StopIteration:
                    self._state.update.add_clients.append(cp)
            else:
                self._state.update.add_clients.append(cp)
            self._prepare_state_publication()

    def _create_slot(self, props):
        slot_name = props.name

        if slot_name in self.repo:
            rospy.logerr("SAM:: Trying to create a slot that already exists (%s)",
                         slot_name)
            return False

        try:
            self.repo.append(props)
        except IndexError, ex:
            rospy.logerr(str(ex))
            return False

        s = self.repo[slot_name]
        self.slot_dep_graph[s.sid] = set()

        return True

    def _create_slot_srv_callback(self, req):
        """
        Callback for the Slot creation service. This process will fail if a
        Slot with the requested name *or* with a name that will map onto the
        same alias already exists.
        """
        self._lock.acquire()
        reply = CreateSlotResponse()

        try:
            reply.success = self._create_slot(req.properties)
        except IndexError, ex:
            pass

        if reply.success and req.properties.is_shared:
            self._create_slot_state_update(req.properties)
            
        self._lock.release()
        return reply

    def _create_slot_state_update(self, props):
        self._update_public_checksum()
        if self._state.status_flag != ManagerFlags.SYNCH_ATTEMPT:
            self._state.status_flag = ManagerFlags.UPDATE_PENDING
            self._state.update.add_slots.append(props)
            self._prepare_state_publication()

    def _get_slot_properties(self, slot):
        return monarch_situational_awareness.msg.SlotProperties(
            name=slot.name,
            description=slot.description,
            type_str=slot.type_str,
            is_shared=slot.is_shared,
            is_latched=slot.is_latched)

    def _get_slot_info_srv_callback(self, req):
        """
        Callback for the SAR list-info service.
        """
        self._lock.acquire()
        reply = GetSlotInfoResponse()
        if len(req.agent_name) > 0:
            n = specify_name(req.slot_name, req.agent_name)
        else:
            n = req.slot_name
        if n in self.repo:
            reply.properties = self.repo.get_slot_props(self.repo[n])
        self._lock.release()
        return reply

    def _list_slot_info_srv_callback(self, req):
        """
        Callback for the SAR list-info service.
        """
        self._lock.acquire()
        reply = ListSlotInfoResponse()
        props = self.repo.get_repo_props()
        reply.singles = props['singles']
        reply.groups = props['groups']
        self._lock.release()
        return reply

    def _check_loops(self, s):
        scc_list = self._tarjan_alg(s, {}, [])
        if len(scc_list) > 0:
            scc = scc_list[-1]
            if len(scc) <= 1:
                return True
            else:
                rospy.logdebug("SAM:: Strongly connected component detected.")
                scc_label = 0
                for sid in scc:
                    if self.slot_scc.has_key(sid):
                        scc_label = self.slot_scc[sid]
                if scc_label == 0:
                    self.scc_count += 1
                    scc_label = self.scc_count
                for sid in scc:
                    self.slot_scc[sid] = scc_label
                return False
        return True

    def _tarjan_alg(self, s, m, exp):
        """Tarjan's strongly connected
        components algorithm"""
        m[s] = TarjanNode(len(m))
        exp.append(s)
        scc_list = []
        for r in self.slot_dep_graph[s]:
            if r not in m:
                scc_list.extend(self._tarjan_alg(r, m, exp))
                m[s].lowlink = min(m[s].lowlink, m[r].lowlink)
            elif r in exp:
                m[s].lowlink = min(m[s].lowlink, m[r].lowlink)

        if m[s].lowlink == m[s].index:            
            r = 0
            scc = set()
            while s != r:
                r = exp.pop()
                scc.add(r)
            if len(scc) > 0:
                scc_list.append(scc)
        return scc_list

    def _print_dep_graph_srv_callback(self, req):
        """
        Callback for the graph-printing service.
        """
        self._lock.acquire()
        reply = std_srvs.srv.EmptyResponse()
        if has_pydot:
            rospy.loginfo("SAM:: Printing the SAR dependency graph.")
            graph = pydot.Dot("Local_SAR_Dependency_Graph",
                              graph_type="digraph",
                              rankdir="LR",
                              ranksep="1.2 equally")
            clusters = {}
            names = self._known_hosts.keys()
            for h in names:
                clusters[h] = pydot.Cluster(sanitize_name(h))
            used_slots = []
            unused_slots = []
            for slot in self.repo:
                if slot.writer is not None or len(slot.readers) > 0:
                    used_slots.append(slot)
                else:
                    unused_slots.append(slot)
            slots_to_print = unused_slots
            slots_to_print.extend(used_slots)
            for slot in slots_to_print:
                r = slot.get_readable_rate()
                tp = slot.get_readable_throughput()

                if self.slot_scc.has_key(slot.sid):
                    if slot.is_shared:
                        color = "orange2"
                    else:
                        color = "orangered"
                    l = ('<%s<BR/><FONT POINT-SIZE="12">--In Loop %s--</FONT>'
                         '<BR/><FONT POINT-SIZE="8">%s<BR/>r: %s T: %s</FONT>>'
                         % (slot.name, self.slot_scc[slot.sid], slot.type_str, r, tp))
                else:
                    if slot.is_shared:
                        color = "yellow"
                    else:
                        color = "white"
                    l = ('<%s<BR/><FONT POINT-SIZE="8">%s<BR/>r: %s T: %s</FONT>>'
                         % (slot.name, slot.type_str, r, tp))
                    
                n = pydot.Node(slot.name, label=l,
                               fillcolor=color,
                               style="filled",
                               shape="hexagon")
                if slot.host is not None and slot.host in clusters.keys():
                    clusters[slot.host].add_node(n)
                else:
                    graph.add_node(n)
            for k, c in clusters.items():
                if c.get_node_list() > 0:
                    graph.add_subgraph(c)
            for c_id, c_obj in self.clients.items():
                if c_id.host != self.host:
                    name = specify_name(c_id.name, c_id.host)
                else:
                    name = c_id.name
                graph.add_node(pydot.Node(name,
                                          style='filled',
                                          shape='box',
                                          fillcolor='gray'))
                for wsid in c_obj.writes_to:
                    sw_name = self.repo[wsid].name
                    ew = pydot.Edge(name,
                                    sw_name)
                    graph.add_edge(ew)
                for rsid in c_obj.reads_from:
                    sr_name = self.repo[rsid].name
                    er = pydot.Edge(sr_name,
                                    name)
                    graph.add_edge(er)

            r = rospkg.RosPack()
            graph.write(path=r.get_path('monarch_situational_awareness') +
                        "/static/SAR_graph.gif", format='gif')
        else:
            rospy.logwarn("SAM:: Module 'pydot' is needed to print the SAR "
                          "dependency graph "
                          "(try 'sudo-apt get install python-pydot').")
        self._lock.release()
        return reply

    def _remove_reader(self, client_id, slot_name):
        slot = self.repo[slot_name]
        if (client_id not in self.clients.keys() or
                slot.sid not in self.clients[client_id].reads_from):
            rospy.logwarn("SAM:: '%s' is not a Reader of Slot '%s'. ",
                          client_id.name, slot_name)
            return False

        self.clients[client_id].reads_from.remove(slot.sid)
        slot.remove_reader(client_id)

        write_targets = self.clients[client_id].writes_to.copy()
        for c_sid in write_targets:
            if c_sid is not None and c_sid in self.slot_dep_graph[slot.sid]:
                self.slot_dep_graph[slot.sid].remove(c_sid)

        if (len(self.clients[client_id].writes_to) == 0 and
                len(self.clients[client_id].reads_from) == 0):
            self.clients.pop(client_id)

        if (slot.is_shared and client_id.host == self.host
                and self._mm_close_in is not None):
            self._mm_close_in(target_topic=slot.get_input_topic_name(),
                              type_str=slot.type_str,
                              key=slot.name)

        return True

    def _remove_reader_srv_callback(self, req):
        self._lock.acquire()
        reply = RemoveReaderResponse()
        c_name = req.reader_name
        c_id = ClientID(c_name, self.host)
        try:
            s = self.repo[req.slot_name]
            reply.success = self._remove_reader(c_id, s.name)
            if reply.success is True and s.is_shared:
                self._remove_reader_state_update(c_id, s.name)

        except KeyError:
            pass

        self._lock.release()
        return reply

    def _remove_reader_state_update(self, client_id, slot_name):
        self._update_public_checksum()
        if self._state.status_flag != ManagerFlags.SYNCH_ATTEMPT:
            self._state.status_flag = ManagerFlags.UPDATE_PENDING
            cp = ClientProperties(name=client_id.name,
                                  host=client_id.host,
                                  reads_from_names=[slot_name])
            del_clients = self._state.update.del_clients
            if len(del_clients) > 0:
                try:
                    c_old = next(c for c in del_clients
                                 if c.name == client_id.name)
                    c_old.reads_from_names.append(slot_name)
                except StopIteration:
                    self._state.update.del_clients.append(cp)
            else:
                self._state.update.del_clients.append(cp)
            self._prepare_state_publication()

    def _remove_writer(self, slot_name):
        slot = self.repo[slot_name]
        if slot.writer is None:
            rospy.logwarn("SAM:: Slot '%s' does not have a Writer. ",
                          slot_name)
            return False

        old_writer = slot.writer
        if slot.sid in self.clients[old_writer].writes_to:
            self.clients[old_writer].writes_to.remove(slot.sid)

        slot.remove_writer()

        deps = self.clients[old_writer].reads_from
        for s in deps:
            if slot.sid in self.slot_dep_graph[s]:
                self.slot_dep_graph[s].remove(slot.sid)

        if (len(self.clients[old_writer].writes_to) == 0 and
                len(self.clients[old_writer].reads_from) == 0):
            self.clients.pop(old_writer)

        if (slot.is_shared and old_writer.host == self.host
                and self._mm_close_out is not None):
            self._mm_close_out(target_topic=slot.get_output_topic_name(),
                               type_str=slot.type_str,
                               key=slot_name)
        return True

    def _remove_writer_srv_callback(self, req):
        self._lock.acquire()
        reply = RemoveWriterResponse()
        c_name = req.reader_name
        c_id = ClientID(c_name, self.host)
        try:
            s = self.repo[req.slot_name]
            w = s.writer

            if w is not None:
                reply.success = self._remove_writer(s.name)
                if (reply.success is True
                        and s.is_shared):
                    self._remove_writer_state_update(c_id, s.name)

        except KeyError:
            pass
        self._lock.release()
        return reply

    def _remove_writer_state_update(self, client_id, slot_name):
        self._update_public_checksum()
        if self._state.status_flag != ManagerFlags.SYNCH_ATTEMPT:
            self._state.status_flag = ManagerFlags.UPDATE_PENDING
            cp = ClientProperties(name=client_id.name,
                                  host=client_id.host,
                                  writes_to_names=[slot_name])
            del_clients = self._state.update.del_clients
            if len(del_clients) > 0:
                try:
                    c_old = next(c for c in del_clients
                                 if c.name == client_id.name)
                    c_old.writes_to_names.append(slot_name)
                except StopIteration:
                    self._state.update.del_clients.append(cp)
            else:
                self._state.update.del_clients.append(cp)
            self._prepare_state_publication()

    def _remove_client(self, client_id):
        any_shared = False
        if client_id not in self.clients.keys():
            rospy.logwarn("SAM:: '%s' is not a SAM Client.",
                          specify_name(client_id.name, client_id.host))
            return (False, False)
        rs = self.clients[client_id].reads_from.copy()
        for r in rs:
            if not self._remove_reader(client_id, self.repo[r].name):
                return (False, any_shared)
            elif self.repo[r].is_shared:
                any_shared = True

        if client_id in self.clients.keys():
            ws = self.clients[client_id].writes_to.copy()
            for w in ws:
                if not self._remove_writer(self.repo[w].name):
                    return (False, any_shared)
                elif self.repo[r].is_shared:
                    any_shared = True

        return (True, any_shared)

    def _remove_client_srv_callback(self, req):
        self._lock.acquire()
        reply = RemoveClientResponse()
        c_id = ClientID(req.client_name, self.host)
        if c_id in self.clients.keys():
            c_props = copy.copy(self.clients[c_id])
        (reply.success, any_shared) = self._remove_client(c_id)

        if any_shared:
            self._remove_client_state_update(c_id, c_props)
        self._lock.release()
        return reply

    def _remove_client_state_update(self, client_id, client_props):
        self._update_public_checksum()
        if self._state.status_flag != ManagerFlags.SYNCH_ATTEMPT:
            self._state.status_flag = ManagerFlags.UPDATE_PENDING
            cp = ClientProperties(name=client_id.name,
                                  host=client_id.host)
            for sid in client_props.reads_from:
                cp.reads_from_names.append(self.repo[sid].name)
            for sid in client_props.writes_to:
                cp.writes_to_names.append(self.repo[sid].name)

            del_clients = self._state.update.del_clients
            if len(del_clients) > 0:
                try:
                    c_old = next(c for c in del_clients
                                 if c.name == client_id.name)
                    c_old.reads_from_names = cp.reads_from_names
                    c_old.writes_from_names = cp.writes_from_names
                except StopIteration:
                    self._state.update.del_clients.append(cp)
            else:
                self._state.update.del_clients.append(cp)
            self._prepare_state_publication()

    def _conn_keepalive_callback(self, event):
        rospy.logdebug("SAM:: Checking connections... "
                       "(connection_keepalive_period)")
        self._update_slot_connections()

    def _update_slot_connections(self, sid=None):
        master = rosgraph.masterapi.Master('/rostopic')
        try:
            state = master.getSystemState()
        except socket.error:
            raise SAMConnectionException("Unable to communicate with master!")

        all_pubs, all_subs, _ = state
        pubs = []
        subs = []

        if sid is not None:
            sid_list = [sid]
        else:
            sid_list = range(1, len(self.repo))

        for sid in sid_list:
            slot = self.repo[sid]
            in_topic = slot.get_input_topic_name()
            out_topic = slot.get_output_topic_name()
            for x in all_pubs:
                if x[0] == in_topic:
                    pubs = x[1]

            if slot.writer is not None:
                try:
                    w = slot.writer
                    if w.host == self.host and w.name not in pubs:
                        rospy.loginfo("SAM:: Unregistering '%s' as a Writer of "
                                      "Slot '%s'", w.name, slot.name)
                        self._remove_writer(slot.name)
                        if slot.is_shared:
                            self._remove_writer_state_update(w, slot.name)
                except KeyError:
                    pass

            for x in all_subs:
                if x[0] == out_topic:
                    subs = x[1]

            rs = slot.readers.copy()
            for r in rs:
                try:
                    if r.host == self.host and r.name not in subs:
                        rospy.loginfo("SAM:: Unregistering '%s' as a Reader of "
                                      "Slot '%s'", r.name, slot.name)
                        self._remove_reader(r, slot.name)
                        if slot.is_shared:
                            self._remove_reader_state_update(r, slot.name)
                except KeyError:
                    pass

    def _state_callback(self, msg):
        self._lock.acquire()
        if not self._validate_host(msg):
            self._lock.release()
            return
        
        """
        Process the sent message, adding / removing slots and clients if necessary
        """
        updated = self._process_update(msg)
        c_test = (self._state.checksum == msg.checksum)
        sender_hostid = self._known_hosts[msg.host]
        my_hostid = self._known_hosts[self.host] 

        """
        Reset the state to neutral if everyone is happy
        """
        if self._state.status_flag != ManagerFlags.OK:
            try:
                n = next(r for r in self._state.relations
                         if r != ManagerRelations.SYNCHRONIZED)
            except StopIteration:
                rospy.loginfo("SAM:: Update successful. "
                              "Synchronized with all hosts.")
                self._state.status_flag = ManagerFlags.OK
                self._state.update = ManagerUpdate()
                self._disable_synch_timeout()
                if self._static_hosts is True:
                    self._disable_heartbeat()

        if msg.status_flag == ManagerFlags.SYNCH_ATTEMPT:
            """
            Sender is trying to synchronize.
            We only need to respond if we're not already synch'ed or
            if the host does not know us yet.
            """
            if (len(msg.relations) < len(self._known_hosts) or 
                msg.relations[my_hostid] != ManagerRelations.SYNCHRONIZED):
                self._respond_synch_req()
            rospy.logdebug("SAM:: [%s]: Received synch attempt from host '%s'",
                           self.host, msg.host)
            self._lock.release()
            return

        if msg.status_flag == ManagerFlags.UPDATE_PENDING:
            """
            Sender is trying to receive update confirmations.
            Note that it is possible that we receive an inconsistent update, resulting in 
            a wrong checksum. We should aknowledge an update if the sender is out of synch so that 
            the sender becomes aware of this error.
            """
            if updated is True or c_test is False:
                self._respond_update_req(sender_hostid)
            rospy.logdebug("SAM:: [%s]: Received update request from host '%s'",
                           self.host, msg.host)
            self._lock.release()
            return

        if msg.status_flag == ManagerFlags.OK:
            """
            Sender is replying to someone's request (maybe ours).
            """
            if (c_test is False and msg.relations[my_hostid] ==
                ManagerRelations.UPDATE_ACKNOWLEDGE):
                """
                Sender is responding to our request, but the checksum is
                wrong. Enable the synch timeout timer.
                """
                self._enable_synch_timeout()
                rospy.logwarn("Checksum error in update acknowledgement."
                              " Synchronization timeout counting...")
            rospy.logdebug("SAM:: [%s]: Received a state reply from host '%s'",
                           self.host, msg.host)
            self._lock.release()
            return
        
        self._lock.release()

    def _process_update(self, msg):
        updated = False
        if (len(msg.update.add_slots) == 0 and
                len(msg.update.add_clients) == 0 and
                len(msg.update.del_slots) == 0 and
                len(msg.update.del_clients) == 0):
            return updated
        for sp in msg.update.add_slots:
            if sp.name not in self.repo:
                updated = True
                self._create_slot(sp)
        for cp in msg.update.add_clients:
            c_id = ClientID(cp.name, cp.host)
            try:
                c = self.clients[c_id]
            except KeyError:
                self.clients[c_id] = ClientProps()
                c = self.clients[c_id]
            for n in cp.reads_from_names:
                try:
                    if self.repo[n].sid not in c.reads_from:
                        updated = True
                        self._create_reader(c_id, n)
                except KeyError:
                    rospy.logwarn("SAM:: Non-local client '%s' "
                                  "reads from unknown slot '%s'.",
                                  cp.name, n)
            for n in cp.writes_to_names:
                try:
                    if self.repo[n].sid not in c.writes_to:
                        updated = True
                        self._create_writer(c_id, n)
                except KeyError:
                    rospy.logwarn("SAM:: Non-local client '%s' "
                                  "writes to unknown slot '%s'.",
                                  cp.name, n)
        for cp in msg.update.del_clients:
            c_id = ClientID(cp.name, cp.host)
            try:
                c = self.clients[c_id]
            except KeyError:
                continue
            for n in cp.reads_from_names:
                try:
                    if self.repo[n].sid in c.reads_from:
                        updated = True
                        self._remove_reader(c_id, n)
                except KeyError:
                    rospy.logwarn("SAM:: Non-local client '%s' "
                                  "reads from unknown slot '%s'.",
                                  cp.name, n)
            for n in cp.writes_to_names:
                try:
                    if self.repo[n].sid in c.writes_to:
                        updated = True
                        self._remove_writer(n)
                except KeyError:
                    rospy.logwarn("SAM:: Non-local client '%s' "
                                  "writes to unknown slot '%s'.",
                                  cp.name, n)
        if updated is True:
            self._update_public_checksum()
            
        if msg.checksum == self._state.checksum:
            rospy.logdebug("SAM:: [%s]: Positive checksum test with host '%s'",
                           self.host, msg.host)
            if updated is True:
                rospy.loginfo("Synchronized with host '%s'", msg.host)

            """
            If the sender checksum is the same as ours, we are synchronized.
            """
            self._state.relations[self._known_hosts[msg.host]] = \
                ManagerRelations.SYNCHRONIZED
            
        else:
            rospy.logdebug("SAM:: [%s]: Negative checksum test with host '%s'",
                           self.host, msg.host)
            self._state.relations[self._known_hosts[msg.host]] = \
                ManagerRelations.CHECKSUM_ERROR
            if not (self._state.status_flag == ManagerFlags.SYNCH_ATTEMPT or 
                    msg.status_flag == ManagerFlags.SYNCH_ATTEMPT):
                """
                If we are trying to synchronize, checksum errors are expected at first.
                """
                rospy.logwarn("SAM:: Checksum error. Out of synch with host '%s'",
                              msg.host)

        return updated

    def _respond_synch_req(self):
        u = ManagerUpdate()
        cs = set()
        for s in self.repo:
            if s.is_shared:
                u.add_slots.append(self.repo.get_slot_props(s))
                if s.writer is not None:
                    cs.add(s.writer)
                for r in s.readers:
                    cs.add(r)
        for c in cs:
            cp = self._get_client_props(c)
            if cp is not None:
                u.add_clients.append(cp)
        self._state.update = u
        self._prepare_state_publication()

    def _respond_update_req(self, hostid):
        self._state.relations[hostid] = ManagerRelations.UPDATE_ACKNOWLEDGE
        self._prepare_state_publication()

    def _get_client_props(self, client):
        cp = ClientProperties()
        cp.name = client.name
        cp.host = client.host
        try:
            c = self.clients[client]
            for s_sid in c.reads_from:
                if self.repo[s_sid].is_shared:
                    cp.reads_from_names.append(self.repo[s_sid].name)
            for s_sid in c.writes_to:
                if self.repo[s_sid].is_shared:
                    cp.writes_to_names.append(self.repo[s_sid].name)
        except KeyError:
            return None
        return cp

    def _disable_heartbeat(self):
        self._heartbeat_timer.shutdown()

    def _disable_synch_timeout(self):
        if self._synch_timeout_timer is not None:
            self._synch_timeout_timer.shutdown()

    def _heartbeat_enabled(self):
        return self._heartbeat_timer._shutdown is False

    def _enable_heartbeat(self):
        if not self._heartbeat_enabled():
            self._heartbeat_timer = rospy.Timer(self._hb_period,
                                                self._heartbeat_callback)

    def _reenable_keepalive(self):
        self._keepalive_timer.shutdown()
        self._keepalive_timer = rospy.Timer(self._ka_period,
                                            self._conn_keepalive_callback)

    def _enable_synch_timeout(self):
        if (self._synch_timeout_timer is None or
                self._synch_timeout_timer._shutdown is True):
            self._synch_timeout_timer = \
                rospy.Timer(self._synch_timeout,
                            self._synch_timeout_callback,
                            oneshot=True)

    def _heartbeat_callback(self, event):
        self._lock.acquire()
        self._publish_state()
        self._update_host_connections()
        self._lock.release()

    def _publish_state(self):
        self._state_pub.publish(self._state)
        if self._state.status_flag == ManagerFlags.OK:
            self._state.update = ManagerUpdate()
            if self._static_hosts is True:
                self._disable_heartbeat()
            

    def _prepare_state_publication(self):
        if self._allow_asynch_comms or not self._heartbeat_enabled():
            self._publish_state()
        self._enable_heartbeat()

    def _update_public_checksum(self):
        checksum = hashlib.sha1()
        shared_slot_props = self.repo.get_shared_slots()
        if len(shared_slot_props) == 0:
            self._state.checksum = ""
        else:
            readers = []
            writers = []
            for sprop in shared_slot_props:
                s = self.repo[sprop.name]
                w = None
                r = []
                if s.writer is not None:
                    w = s.writer.name
                for c in s.readers:
                    r.append(c.name)
                writers.append(w)
                readers.append(r)

            checksum.update(str(shared_slot_props)+str(readers)+str(writers))
            self._state.checksum = checksum.hexdigest()

    def _synch_timeout_callback(self, event):
        self._state.status_flag = ManagerFlags.SYNCH_ATTEMPT
        rospy.logwarn("SAM:: Persistent checksum error detected. "
                      "Forcing re-synchronization.")
        
    def _register_host(self, host):
        allowed_hosts = self._known_hosts.keys()
        if host not in allowed_hosts:
            rospy.loginfo("SAM:: New SA Manager found: '%s'.", host)
            allowed_hosts.append(host)
            relations = self._state.relations
            times = self._host_times
            self._state.relations = []
            self._host_times = []
            for h in sorted(allowed_hosts):
                if h == host:
                    self._state.relations.append(ManagerRelations.NO_DATA)
                    self._host_times.append(rospy.Time.now())
                else:
                    i = self._known_hosts[h]
                    self._state.relations.append(relations[i])
                    self._host_times.append(times[i])
            for (h,i) in zip(sorted(allowed_hosts),range(len(allowed_hosts)+1)):
                self._known_hosts[h] = i
            self._publish_active_hosts()
            
    def _update_host_connections(self):
        any_down = False
        for h in self._known_hosts.keys():
            i = self._known_hosts[h]
            if h != self.host and self._state.relations[i] != ManagerRelations.CONNECTION_DOWN:
                age = rospy.Time.now() - self._host_times[i]
                if age > self._ka_period:
                    self._state.relations[i] = ManagerRelations.CONNECTION_DOWN
                    any_down = True
                    
        if any_down is True:
            self._publish_active_hosts()
                
    def _publish_active_hosts(self):
        active_hosts = []
        for h in sorted(self._known_hosts.keys()):
            i = self._known_hosts[h]
            if self._state.relations[i] != ManagerRelations.CONNECTION_DOWN:
                active_hosts.append(h)
        self._active_hosts_pub.publish(active_hosts)
        
    def _validate_host(self, msg):
        if msg.host == self.host:
            return False

        if msg.host not in self._known_hosts.keys():
            if self._static_hosts is True:
                rospy.logdebug("SAM:: Received state message from unknown host '%s'."
                               "Ignoring.", msg.host)
                return False
            else:
                rospy.logwarn("SAM:: Received state message from new host '%s'.", msg.host)
                self._register_host(msg.host)
        else:
            self._host_times[self._known_hosts[msg.host]] = rospy.Time.now()
            if self._state.relations[self._known_hosts[msg.host]] == ManagerRelations.CONNECTION_DOWN:
                rospy.loginfo("SAM:: Restored connection with host %s", msg.host) 
                self._state.relations[self._known_hosts[msg.host]] = ManagerRelations.NO_DATA
                self._publish_active_hosts()
            if len(msg.relations) > len(self._state.relations):
                rospy.logerr("SAM:: State message from host '%s' has an incompatible"
                             " relational vector.", msg.host)
                rospy.logerr("SAM:: This can mean that the sender has knowledge of other hosts" 
                             " that are unknown to this Manager.")
                return False
        return True

    def _master_discovery_callback(self, msg):
        self._lock.acquire()
        if len(msg.state) > 2 and msg.state[1:-1] == "removed": # [1:-1] takes the silly quotes off
            host = msg.master.name
            if len(host.split('.')) == 4:
                #In this case we have an IP address instead of a hostname
                try:
                    host = socket.gethostbyaddr(host)[0]
                except socket.herror:
                    self._lock.release()
                    return
            known_hosts = self._known_hosts.keys()
            for kh in known_hosts:
                if kh.lower() == host.lower(): # Multimaster FKIE hosts come in lowercase somehow
                    self._state.relations[self._known_hosts[kh]] = ManagerRelations.CONNECTION_DOWN
                    rospy.logwarn("SAM: Host %s requested a clean shutdown.", kh)
                    self._publish_active_hosts()
                    self._lock.release()
                    return
        self._lock.release()
