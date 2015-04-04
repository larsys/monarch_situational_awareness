#include <boost/function.hpp>

#include <string>

#include <ros/ros.h>
#include <ros/exceptions.h>

#include <monarch_situational_awareness/CreateReader.h>

using namespace ros;
using namespace std;
using namespace monarch_situational_awareness;

class SAMReader
{    
  public:
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(M), 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(M) const, 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(const boost::shared_ptr< M const > &), 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(const boost::shared_ptr< M const > &) const, 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(M), 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(M) const, 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(const boost::shared_ptr< M const > &), 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               void(T::*fp)(const boost::shared_ptr< M const > &) const, 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M>
    SAMReader (const string& slot_name,
               void(*fp)(M),
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M>(topic_name,q_size,fp);
    }
    
    template <class M>
    SAMReader (const string& slot_name,
               void(*fp)(const boost::shared_ptr< M const > &),
               const int q_size=1,
               const string& sam_name="sam_node") :
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M>(topic_name,q_size,fp);
    }
    
    template <class M>
    SAMReader (const string& slot_name,
               const boost::function< void (const boost::shared_ptr< M const >&)> &callback,
               const VoidConstPtr &tracked_object=VoidConstPtr(),
               const int q_size=1,
               const string& sam_name="sam_node") :
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M>(topic_name,q_size,callback,tracked_object);
    }
    
    template <class M, class C>
    SAMReader (const string& slot_name,
               const boost::function< void(C) > &callback,
               const VoidConstPtr &tracked_object=VoidConstPtr(),
               const int q_size=1,
               const string& sam_name="sam_node") :
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name);
        sub_ = nh_.subscribe<M,C>(topic_name,q_size,callback,tracked_object);
    }
    
    //--- with agent_name
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(M), 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(M) const, 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(const boost::shared_ptr< M const > &), 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(const boost::shared_ptr< M const > &) const, 
               T *obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(M), 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(M) const, 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(const boost::shared_ptr< M const > &), 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M , class T >
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(T::*fp)(const boost::shared_ptr< M const > &) const, 
               const boost::shared_ptr< T > &obj,
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,T>(topic_name,q_size,fp,obj);
    }
    
    template <class M>
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(*fp)(M),
               const int q_size=1,
               const string& sam_name="sam_node"):
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M>(topic_name,q_size,fp);
    }
    
    template <class M>
    SAMReader (const string& slot_name,
               const string& agent_name,
               void(*fp)(const boost::shared_ptr< M const > &),
               const int q_size=1,
               const string& sam_name="sam_node") :
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M>(topic_name,q_size,fp);
    }
    
    template <class M>
    SAMReader (const string& slot_name,
               const string& agent_name,
               const boost::function< void (const boost::shared_ptr< M const >&)> &callback,
               const VoidConstPtr &tracked_object=VoidConstPtr(),
               const int q_size=1,
               const string& sam_name="sam_node") :
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M>(topic_name,q_size,callback,tracked_object);
    }
    
    template <class M, class C>
    SAMReader (const string& slot_name,
               const string& agent_name,
               const boost::function< void(C) > &callback,
               const VoidConstPtr &tracked_object=VoidConstPtr(),
               const int q_size=1,
               const string& sam_name="sam_node") :
    nh_(),
    sub_ ()
    {
        string topic_name = createReader(slot_name,
                                         sam_name,
                                         agent_name);
        sub_ = nh_.subscribe<M,C>(topic_name,q_size,callback,tracked_object);
    }
  private:
    string createReader(const string& slot_name,
                        const string& sam_name,
                        const string& agent_name="")
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
        
        srv_path.append("/create_reader");
        service::waitForService(srv_path);
        
        CreateReader cr;
        string topic_name;
        while(!cr.response.success)
        {
            cr.request.properties.slot_name = slot_name;
            cr.request.properties.agent_name = agent_name;
            service::call(srv_path,cr);
            if(!cr.response.success)
            {
                ROS_WARN_STREAM("Failed to create a reader for slot " <<
                slot_name << ". Retrying...");
                d.sleep();
            }
            else
                return cr.response.topic_name;
        }
    }
      
    NodeHandle nh_;
    Subscriber sub_;
};