#!/usr/bin/env python
__author__ = 'Riccardo Pozza, r.pozza@surrey.ac.uk'

import requests
import csv
import time
import datetime
from periodictimer import PeriodicTimer

# definition of variables TODO optparser
hostname = "192.168.0.1"
port = 80
logging_duration = 30
collection_interval = 1
max_number_attemps = 5

# objects definition
ipso_temperature = 3303
ipso_humidity = 3304
ipso_loudness = 3324
ipso_concentration = 3325
ipso_distance = 3330
ipso_multistate = 3348

# instance definition
instance_number = 0  # all use cases just one instance

# resources definition
ipso_sensorvalue = 5700
ipso_multistateinput = 5547

def main():

    #order for collection of data
    objectsorder = [ipso_temperature, ipso_humidity, ipso_loudness, ipso_concentration, ipso_distance, ipso_multistate]
    instanceorder = [instance_number] * 6
    resourceorder = [ipso_sensorvalue] * 5 + [ipso_multistateinput]

    fullclientlist = getclientlist(hostname,port)

    for clientsnames in fullclientlist:
        synchronizetime(hostname, port, clientsnames)

    #
    # staticinformation
    #
    sfl,swl = getcswriters(fullclientlist, "static_info.txt")

    #device
    objl = [3] * 6
    insl = [0] * 6
    resl = [17,0,1,18,3,2]
    writerowscsv(hostname, port, objl, insl, resl, swl, fullclientlist)

    #connectivity
    objl = [4] * 3
    insl = [0] * 3
    resl = [4,5,2]
    writerowscsv(hostname, port, objl, insl, resl, swl, fullclientlist)

    #location
    objl = [6] * 3
    insl = [0] * 3
    resl = [0,1,2]
    writerowscsv(hostname, port, objl, insl, resl, swl, fullclientlist)

    for f in sfl:
        f.close()

    #
    # dynamic information
    #
    dfl,dwl = getcswriters(fullclientlist, "time_series.csv")

    logging = PeriodicTimer(collection_interval,writerowscsv,hostname,port,objectsorder,instanceorder,resourceorder,dwl,fullclientlist)

    logging.start()
    print ("Logging Started")
    while True:
        raw_input("Press Enter to quit: ")
        break

    print ("Logging stopped, Closing up!")
    #time.sleep(logging_duration)
    logging.stop()

    time.sleep(3)
    for f in dfl:
        f.close()


def synchronizetime(fqdn, port, client):
    header_json = {'Content-Type': 'application/json'}
    payload_now = {'id': 13, 'value': datetime.datetime.now().isoformat()[:-7] + "Z"}
    objecturl = "/" + str(3) + "/" + str(0) + "/" + str(13) # time
    fullurl = "http://" + fqdn + ":" + str(port) + "/api/clients/" + client + objecturl
    r = requests.put(fullurl,headers=header_json, json=payload_now)
    return

def getclientlist(fqdn, port):
    attempts = 0
    clientslist = []
    fullurl = "http://" + fqdn + ":" + str(port) + "/api/clients?"
    while attempts < max_number_attemps:
        r = requests.get(fullurl)
        attempts += 1
        if r.status_code == requests.codes.ok:
            for clients in r.json():
                clientslist.append(str(clients['endpoint']))
            return clientslist
    print "Error! cannot retrieve clients list!"
    raise SystemExit

def getsensorvalue(fqdn, port, client, object, instance, resource):
    attempts = 0
    objecturl = "/" + str(object) + "/" + str(instance) + "/" + str(resource)
    fullurl = "http://" + fqdn + ":" + str(port) + "/api/clients/" + client + objecturl
    while attempts < max_number_attemps:
        r = requests.get(fullurl)
        attempts += 1
        if r.status_code == requests.codes.ok:
            try:
                value = r.json()['content']['value']
            except KeyError:
                value = r.json()['content']['values']['0'] + "-" + r.json()['content']['values']['1']
            return value

    print "Missed value!!"
    return "N/A"

def getrowdata(fqdn, port, clientname, objectslist, instancelist, resourcelist):
    datarow = [datetime.datetime.now().isoformat()[:-7] + "Z"]
    for object, instance, resource in zip(objectslist, instancelist, resourcelist):
        datarow.append(getsensorvalue(fqdn, port, clientname, object, instance, resource))

    return datarow

def getcswriters(clientslist,purpose):
    flist = []
    wlist = []
    for clientname in clientslist:
        f = open(clientname + "-" + purpose, 'a')
        flist.append(f)
        wlist.append(csv.writer(f))
    return flist,wlist

def writerowscsv(fqdn, port, objectslist, instancelist, resourcelist, wlist, clist):
    for client, writer in zip(clist,wlist):
        writer.writerow(getrowdata(fqdn, port, client, objectslist, instancelist, resourcelist))

if __name__ == '__main__':
    main()
