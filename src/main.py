
import argparse
from datetime import datetime
import time
import random
import os
import asyncio
from astarte.astarte import device


class App:
    def __init__(self, args) -> None:
        print ("Initialzing the object..")
        
        # Obtaining command line arguments
        self.device_id          = args.device_id
        self.secret             = args.device_secret
        self.realm_name         = args.realm_name
        self.pairing_url        = args.astarte_pairing_url
        self.limit              = (int(args.limit)>0 and int(args.limit) or None)
        print("limit: {}".format(self.limit))

        self.persistency_dir    = "astarte_persistency.d"
        self.persistency_path   = os.path.curdir+'/'+self.persistency_dir

        self.astarte_loop       = asyncio.get_event_loop ()
        
        # Checking existence of persistence directory
        if not os.path.exists(self.persistency_path) :
            print ("Directory at path "+self.persistency_path+" does not exists.\nCreating it...")
            os.mkdir (self.persistency_path)
        elif not os.path.isdir (self.persistency_path) :
            error_message   = "File at path "+self.persistency_path+" is not a directory"
            print (error_message)
            raise Exception (error_message)

        print ("Connecting to {}@{} as {} ({})".format (self.pairing_url, self.realm_name,
                                                        self.device_id, self.secret))

        # Creating a Device object
        self.astarte_device = device.Device(self.device_id, self.realm_name,
                                                    self.secret, self.pairing_url,
                                                    self.persistency_path,
                                                    loop=self.astarte_loop)

        # Setting up callbacks
        self.astarte_device.on_connected                = self.astarte_conection_cb
        self.astarte_device.on_disconnected             = self.astarte_disconnection_cb
        self.astarte_device.on_data_received            = self.astarte_data_cb
        self.astarte_device.on_aggregate_data_received  = self.astarte_aggr_data_cb

        # Defining and adding the interface
        self.interface_name         = "com.astarte.Tester"
        self.interface_descriptor   = { 
            "interface_name": "com.astarte.Tester", 
            "version_major": 0,
            "version_minor": 1, 
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [ 
            {
                "endpoint": "/timestamp",
                "type": "integer",
                "reliability":"unique",
                "retention":"volatile",
                "expiry":60,
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 28800
            },
            {
                "endpoint": "/counter",
                "type": "integer",
                "reliability":"unique",
                "retention":"volatile",
                "expiry":60,
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 28800
            },
            {
                "endpoint": "/random",
                "type": "integer",
                "reliability":"unique",
                "retention":"volatile",
                "expiry":60,
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 28800
            }
            ]
        }
        self.astarte_device.add_interface (self.interface_descriptor)
    
    def run (self) :
        self.astarte_device.connect()
        self.astarte_loop.run_forever()

    def astarte_conection_cb (self, d) :
        print ('\n================\nDevice connected\n================\n\n')
        counter = 0
        
        while self.limit == None or self.limit>0:
            counter     += 1
            timestamp   = int(datetime.timestamp(datetime.now()))
            payload     = {
                "timestamp" : timestamp,
                "counter"   : counter,
                "random"    : random.randint(0,1000)
            }
            if not self.astarte_device.is_connected() :
                print ("Astarte connection absent! Cannot publish data :(")
            else :
                self.astarte_device.send_aggregate (self.interface_name, '/',
                                                    payload, timestamp)
            print ("Sent {}".format(counter))

            if self.limit != None :
                self.limit  = self.limit-1
            time.sleep (1)
            
        self.astarte_loop.stop()


    def astarte_disconnection_cb (self, d) :
        print ('\n===================\nDevice disconnected\n\n')


    def astarte_data_cb (self, data) :
        print ('\n=====================\nReceived new message!\n\n')


    def astarte_aggr_data_cb (self, data) :
        print ('\n=====================\nReceived new message!\n\n')




# ========================================
# ========================================

parser  = argparse.ArgumentParser ()
parser.add_argument ("-i", "--device-id", required=True)
parser.add_argument ("-s", "--device-secret", required=True)
parser.add_argument ("-u", "--astarte-pairing-url", required=True)
parser.add_argument ("-n", "--realm-name", required=True)
parser.add_argument ("-l", "--limit", default=-1)
args    = parser.parse_args()


app     = App (args)

try :
    app.run ()
except BaseException as e:
    print ('\nCatched this exception:\n{}\n'.format(e))