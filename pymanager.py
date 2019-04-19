#! /usr/bin/python

"""
Library of functions to manage a cluster

"""

import requests
import json

# disables console warning when calling an insecured URL
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

class Cluster:

    def __init__(self,name,ip_address,username=None,password=None):
        self.name = name
        self.mcs = ip_address
        self.cldbs = [ip_address]
        self.nodes = []
        self.username = username
        self.password = password
        self.url = "https://{}:8443/rest".format(self.mcs)


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

    def create_stream(self,path):
        """
        creates a stream at the given path
        returns True if success
        returns """
        url = "/stream/create?path={}".format(path)
        response = self.post(url)
        if response["status"] == "OK":
            return True
        else:
            print(response) #TD : raise exception
            return False

    


if __name__ == '__main__':
    c = Cluster("demo.mapr.com","10.0.0.11","mapr","mapr")
    c.create_stream("/test2")
