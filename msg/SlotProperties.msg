#file: SlotProperties.msg
#author: Joao Messias (jmessias@isr.ist.utl.pt)
#
#This message describes the properties of a Slot.
#

#The name of the slot. Should be descriptive
#of the information that is associated to it
#(e.g. "Robot Pose")
string name

#A readable, short explanation of the slot's
#purpose (e.g. "The local estimate of the pose of
#the robot")
string description

#The type of data that this slot will 'contain',
#spelled out as a string (e.g. "geometry_msgs/PoseWithCovariance")
string type_str

#A boolean flag indicating whether this slot is shared.
#False: not shared (is only visible and accessible by the local Master)
#True: shared (readable by other Masters)
bool is_shared

#A boolean flag indicating whether the output of this slot is latched.
#False: not latched (the last datum is not sent to new clients)
#True: latched (new clients receive the most current datum)
bool is_latched
