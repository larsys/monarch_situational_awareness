#file: WriterProperties.msg
#author: Joao Messias (jmessias@isr.ist.utl.pt)
#
#This message describes the properties of a Writer.
#

#The name of the slot that the Writer should write to.
string slot_name

#Optionally, the name of the agent to which the Slot "belongs",
#in case the name specified above identifies a group of slots
string agent_name