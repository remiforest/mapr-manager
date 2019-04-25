#! /usr/bin/python


import requests
import json


# disables console warning when calling an insecured URL
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

class Cluster:

    def __init__(self,name,ip_address,username=None,password=None):
        self.name = name
        self.cldbs = [ip_address]
        self.nodes = []
        self.username = username
        self.password = password
        self.port = 8443
        self.url = "https://{}:{}/rest".format(ip_address,self.port)


    def authenticate(self,username,password):
        """ defines authentication informations """
        self.username = username
        self.password = password

    def post(self,relative_url):
        response = requests.post(self.url + relative_url,verify=False,auth=(self.username,self.password))
        if response.status_code == 200:
            return json.loads(response.content)
        else:
            print(response)
            print(response.content)
            return "request failed" # TD : raise exception

    def initialize(self):
        """ query cluster to populate all values """
        self.nodes = self.get_nodes() 


    """ Cluster management """
    
    def get_nodes(self,service=None):
        """
        returns the list of the cluster nodes as a json dictionary
        service can be : cldb, fileserver, nfs, apiserver, data-access-gateway
        default returns all the nodes
        """
        url = "/node/list"
        response = self.post(url) 
        nodes = [{"hostname":node["hostname"],"ip":node["ip"],"service":node["service"]} \
                for node in response["data"] \
                if not "/nfsserver/{}".format(node["hostname"]) in node["racktopo"]]
        if service:
            nodes = [node for node in nodes if service in node["service"]]
        return nodes


    """ Stream management """

    def create_stream(self,path,permissions={"produceperm":"p","consumeperm":"p","topicperm":"p","copyperm":"p","adminperm":"p"}):
        """
        creates a stream at the given path
        returns True if success
        returns False if not
        """
        stream_permissions = ["produceperm","consumeperm","topicperm","copyperm","adminperm"]
        for permission in stream_permissions:
            try:
                permissions[permission]
            except:
                permissions[permission]="p"

        url = "/stream/create?path={}&produceperm={}&consumeperm={}&topicperm={}&copyperm={}&adminperm={}".format(path,
                                      permissions["produceperm"],
                                      permissions["consumeperm"],
                                      permissions["topicperm"],
                                      permissions["copyperm"],
                                      permissions["adminperm"]
                                      )
        response = self.post(url)
        if response["status"] == "OK":
            return True
        else:
            print(response) #TD : raise exception
            return False

    def is_stream(self,path):
        """
        check if path points on a stream
        True if it's a stream
        False if not
        """
        url = "/stream/info?path={}".format(path)
        response = self.post(url)
        if response["status"] == "OK":
            return True
        else:
            # print(response) #TD : raise exception
            return False

    def delete_stream(self,path):
        """
        deletes a stream at the given path
        returns True if success
        returns False if failure
        """
        if self.is_stream(path):
            url = "/stream/delete?path={}".format(path)
            response = self.post(url)
            if response["status"] == "OK":
                return True
        return False

    def replicate_stream(self,path,replica,synchronous=False):
        if synchronous:
            synchronous = "true"
        else:
            synchronous = "false"

        if self.is_stream(path):
            url = "/stream/replica/autosetup?path={}&replica={}&synchronous={}".format(path,replica,synchronous)
            response = self.post(url)
            if response["status"] == "OK":
                return True
            print(response)
        return False

if __name__ == '__main__':
    c = Cluster("demo.mapr.com","10.0.0.11","mapr","mapr")
    print("Get nodes")
    print(c.get_nodes())
    print("Get CLDBs")
    print(c.get_nodes(service="cldb"))
    print("Create test stream : {}".format(c.create_stream("/test_stream")))
    print("does test stream exists ? should be True : {}".format(c.is_stream("/test_stream")))
    print("Create replica stream : {}".format(c.replicate_stream("/test_stream","/test_replica")))
    print("does replica exists ? should be True : {}".format(c.is_stream("/test_replica")))
    print("Delete test stream : {}".format(c.delete_stream("/test_stream")))
    print("does test stream exists ? should be False : {}".format(c.is_stream("/test_stream")))
    print("does replica exists ? should be True : {}".format(c.is_stream("/test_replica")))
    print("Delete replica stream : {}".format(c.delete_stream("/test_replica")))
    print("does replica exists ? should be False : {}".format(c.is_stream("/test_replica")))
