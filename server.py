"""
This file contains a simple HTTP Server that allows us to visualize the
SAM dependency graph via a common web browser.

This code is adapted from the SocRob@Home webconsole designed by Rodrigo Ventura.
"""
import re
import shutil
import os
import sys
import os.path as osp
import urllib
import mimetypes
from BaseHTTPServer import *
import socket
import roslib
import rospy

from std_srvs.srv import Empty


ROOT_PATH = osp.dirname(osp.realpath(__file__))
STATIC_PATH = osp.join(ROOT_PATH, "static")
DEFAULT_HOME = "home_sam.html"
MINIMUM_REFRESH_INTERVAL = 1.0

class Server(BaseHTTPRequestHandler):
    def log_request(self, *args):
        pass
    
    def do_GET(self):
        self.serve('GET')
        
    def do_HEAD(self):
        self.serve('HEAD')

    def serve(self, method):
        for (pat, func) in self.URLS:
            m = re.match(pat, self.path)
            if m is not None:
                func(self, method, m)
                return
        self.send_error(404, "Not found: %s"%(self.path))

    def handle_home(self, method, match):
        self.send_response(301)
        self.send_header("Location", "/static/"+DEFAULT_HOME)
        self.end_headers()

    def handle_static(self, method, match):
	proxy.refresh_graph()
	
        fn = osp.normpath(osp.join(STATIC_PATH, match.group(1)))
        if fn.startswith(STATIC_PATH):
            try:
                ctype = mimetypes.guess_type(fn)
                stat = os.stat(fn)
                with open(fn) as fh:
                    self.send_response(200)
                    self.send_header("Content-type", ctype)
                    self.send_header("Content-Length", str(stat[6]))
                    self.end_headers()
                    if method=='GET':
                        shutil.copyfileobj(fh, self.wfile)
            except (IOError, OSError):
                self.send_error(404, "Unknown URL: %s"%(match.group()))
        else:
            self.send_error(404, "Unauthorized")

    def handle_do(self, method, match):
        reqs = match.group(1).split('?', 1)
        cmd  = reqs[0]
        args = reqs[1].split('&') if len(reqs)>1 else []
        for (c, f) in self.CMDS:
            if cmd==c:
                f(self, method, args)
                return
        self.send_error(404, "Unrecognized request")

    def header(self, type):
        self.send_response(200)
        self.send_header("Content-type", type)
        self.end_headers()

    # Keep this to the end of class definition
    URLS = (('^/$', handle_home),
            ('^/static/(.*)$', handle_static),
            ('^/do/(.*)$', handle_do))

    CMDS = ()


class Proxy:
    def __init__(self):
        rospy.init_node("sam_webconsole", argv=sys.argv)
        self.print_srv = rospy.ServiceProxy('print_dep_graph', Empty)
        rospy.wait_for_service('print_dep_graph')
        rospy.loginfo("Proxy ready")
        self.last_refresh_time = rospy.Time.now()

    def refresh_graph(self):
        duration = rospy.Time.now() - self.last_refresh_time
        if duration.to_sec() > MINIMUM_REFRESH_INTERVAL:
            self.print_srv()
            self.last_refresh_time = rospy.Time.now()

def main():
    global proxy
    proxy = Proxy()
    p = rospy.get_param('~port',9999)
    server_address = ('',p)
    try:
        httpd = HTTPServer(server_address, Server)
    except socket.error:
        rospy.logerr("Can't open web socket")
        return
    httpd.serve_forever()

if __name__=="__main__":
    main()

# EOF
