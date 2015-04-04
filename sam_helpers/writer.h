#include <string>

#include <ros/ros.h>
#include <ros/exceptions.h>

#include <monarch_situational_awareness/CreateWriter.h>

using namespace ros;
using namespace std;
using namespace monarch_situational_awareness;

template <class T> class SAMWriter
{    
  public:
    SAMWriter (const string& slot_name, 
               const string& agent_name="",
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    pub_ ()
    {
        Duration d(5.0);
        string s;
        ros::V_string node_list;
        string srv_path;
        bool found_sam = false;
        if(!sam_name.empty() && sam_name[0] == '/')
        { //fully-qualified name
            found_sam = true;
            srv_path = ros::names::parentNamespace(sam_name);
        }
        
        while(!found_sam)
        {
            try
            {
                if(ros::master::getNodes(node_list))
                {
                    size_t l = sam_name.size();
                    for(size_t i = 0; i < node_list.size(); i++)
                    {
                        size_t m = node_list[i].size();
                        if(m > l && node_list[i].substr(m-l) == sam_name)
                        {
                            found_sam = true;
                            srv_path = ros::names::parentNamespace(node_list[i]);
                            break;
                        }
                    }
                }
                if(!found_sam)
                {
                    ROS_WARN_STREAM("Could not find the SAM node in the ROS namespace (" << sam_name << ")");
                    ROS_WARN_STREAM("Retrying in " << d.toSec() << " secs.");
                    d.sleep();
                }
            }
            catch(InvalidNameException e)
            {
                ROS_ERROR_STREAM(e.what());
                ROS_WARN_STREAM("Retrying in " << d.toSec() << " secs.");
                d.sleep();
            }
        }
        
        srv_path.append("/create_writer");
        service::waitForService(srv_path);

        CreateWriter cw;
        string topic_name;
        while(!cw.response.success)
        {
            cw.request.properties.slot_name = slot_name;
            cw.request.properties.agent_name = agent_name;
            service::call(srv_path,cw);
            if(!cw.response.success)
            {
                ROS_WARN_STREAM("Failed to create a writer for slot " <<
                                slot_name << ". Retrying...");
                d.sleep();
            }
            else
                topic_name = cw.response.topic_name;
        }
        pub_ = nh_.advertise<T> (topic_name,q_size);
    }
    
    //Overriding the ROS publisher methods
    void publish(const boost::shared_ptr<T>& message) const
    {
        pub_.publish(message);
    }
    void publish(const T& message) const
    {
        pub_.publish(message);
    }
  private:
    NodeHandle nh_;
    Publisher pub_;
};