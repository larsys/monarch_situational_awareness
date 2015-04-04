#file: ReaderProperties.msg
#author: Joao Messias (jmessias@isr.ist.utl.pt)
#
#This message describes the properties of a Reader.
#

#The name of the slot that the Reader should read from
string slot_name

#Optionally, the name of the agent to which the Slot "belongs",
#in case the name specified above identifies a group of slots
string agent_name