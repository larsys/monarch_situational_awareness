import rospy
import roslib, roslib.message
import rosnode

from monarch_situational_awareness.msg import WriterProperties
from monarch_situational_awareness.srv import CreateWriter
from monarch_situational_awareness.srv import CreateWriterResponse
from monarch_situational_awareness.srv import GetSlotInfo

class SAMWriter():
    def __init__(self, 
                 slot_name, 
                 agent_name="", 
                 sam_name="sam_node", 
                 q_size=1):
        d = 5.0 #sleep duration
        found_sam = False
        if sam_name[0] == '/': #fully-qualified name supplied 
            found_sam = True
            ns = rospy.names.namespace(sam_name)
            
        while found_sam == False:
            try:
                names = rosnode.get_node_names()
                l = len(sam_name)
                ns = ""
                for n in names:
                    if len(n) > l and n[-l:] == sam_name:
                        ns = rospy.names.namespace(n)
                        found_sam = True
                        break
                if found_sam == False:
                    rospy.logwarn("Could not find the SAM node in the ROS namespace (%s)",sam_name)
                    rospy.logwarn("Retrying in %d secs.",d)
                    rospy.sleep(d)
            except rosnode.ROSNodeIOException as e:
                rospy.logwarn(e)
                rospy.logwarn("Retrying in %d secs.",d)
                rospy.sleep(d)

        rospy.wait_for_service(ns+'get_slot_info')
        rospy.wait_for_service(ns+'create_writer')
        create_writer = rospy.ServiceProxy(ns+'create_writer', CreateWriter)
        get_slot_info = rospy.ServiceProxy(ns+'get_slot_info', GetSlotInfo)
        
        success = False
        d = 5.0 #sleep duration
        while success == False:
            sp = get_slot_info(slot_name, agent_name)
            if len(sp.properties.name) > 0:
                type_str = sp.properties.type_str
                rp = WriterProperties(slot_name, agent_name)    
                r = create_writer(rp)
                success = r.success
                if success == True:
                    type_class = roslib.message.get_message_class(type_str)
                    self._pub = rospy.Publisher(r.topic_name, type_class, queue_size = q_size)
                else:
                    if len(agent_name) == 0:
                        rospy.logwarn("Could not create SAM Writer for Slot '%s'." 
                                      " Retrying...",slot_name)
                    else:
                        rospy.logwarn("Could not create SAM Writer for Slot '%s'" 
                                      " ('%s'). Retrying...",slot_name, agent_name)
                    rospy.sleep(d)
            else:
                if len(agent_name) == 0:
                    rospy.logerr("Could not create SAM Writer for Slot '%s'. The Slot does not exist."
                                 " Retrying...",slot_name)
                else:
                    rospy.logerr("Could not create SAM Writer for Slot '%s'. The Slot does not exist"
                                 " ('%s'). Retrying...",slot_name, agent_name)
                rospy.sleep(d)
                            
                            
    def publish(self, data):
        self._pub.publish(data)