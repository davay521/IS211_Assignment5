#David Vayman
#IS211_Assignment5
#URL = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
import urllib2
import argparse
import csv


class Queue (object):

    def __init__(self):
        
        self.items = []             # make an empty list

    def is_empty(self):
        return self.items == []     # True if list is empty

    def enqueue(self, item):
        self.items.insert(0, item)  # inserts into [0]

    def dequeue(self):
        return self.items.pop()     # pops last item

    def size(self):
        return len(self.items)      # length

class Server(object):

    def __init__(self, name):
        self.name = name
        self.currentRequest = None
        self.remainingTime = 0

    def tick(self):
        """
        decrements internal timer, sets server to idle if task complete
        """
        if self.currentRequest != None:
            self.remainingTime -= 1

            if self.remainingTime <= 0:
                self.currentRequest = None

    def busy(self):
        """
        returns true if busy (current task not None), false if not
        """
        if self.currentRequest != None:
            return True
        else:
            return False

    def start_next(self, newRequest):
        """
        starts next request (Request object) adjusts time remaining which affects the tick method
        """

        self.currentRequest = newRequest
        self.remainingTime = newRequest.get_time()     # gets processing time from Request obj


class Request(object):

    def __init__(self, name, time, processingTime):   # input the current time on initialization (left column)
        self.name = name
        self.timestamp = time
        self.processingTime = processingTime    # replace with right column

    def get_stamp(self):
        """ returns timestamp """
        return self.timestamp

    def get_time(self):        # "get_pages" should return processing time (right column)
        """returns pages"""
        return self.processingTime

    def wait_time(self, current_second):
        """returns the wait time"""
        return current_second - self.timestamp


def download_data(url):
    response = urllib2.urlopen(url)
    return response


def get_max(fileIn):
    """
    gets the max timestamp value to use in order to construct simulation dictionary
    """

    numberList = []
    for i in fileIn:
        numberList.append(i[0])

    return max(numberList)


def url_csv_read(fileName):
        """
        takes in csv data from url and returns dict
        """

        fileIn = csv.reader(fileName)

        parsedFile = []

        for row in fileIn:         # parses incoming csv into a list
            row[0] = int(row[0])
            parsedFile.append(row)

        timestampMax = get_max(parsedFile)
        dictionary = {x: [] for x in range(1, timestampMax + 1)}  # builds dictionary for simulation

        for row in parsedFile:
            dictionary[row[0]].append(row)

        return dictionary


def simulateOneServer(request_file):

    server = Server(name=0)
    serverQ = Queue()

    waiting_times = []
    req_num = 1

    for current_second, bulk_request in request_file.iteritems():

        if bulk_request:     # if there is a task this second...


            for req in bulk_request:   # drill into
                timestamp = int(req[0])
                process_time = int(req[2])

                request = Request(req_num, timestamp, process_time)
                serverQ.enqueue(request)
                req_num += 1

        if (not server.busy()) and (not serverQ.is_empty()):   # check if server idle and itms in queue
            next_request = serverQ.dequeue()
            waiting_times.append(next_request.wait_time(current_second))
            server.start_next(next_request)

        server.tick()
    average_wait = float(sum(waiting_times)) / len(waiting_times)
    print "Average Wait %6.2f secs %3d tasks remaining." % (average_wait, serverQ.size())


def simulateManyServers(request_file, size):

    server_list = [Server(name=x) for x in range(0, size)]       # creates a list of servers specified by size
    server_queue = Queue()

    waiting_times = []
    req_num = 1

    for current_second, bulk_request in request_file.iteritems():

        for req in bulk_request:   # drill into
            timestamp = int(req[0])
            process_time = int(req[2])

            request = Request(req_num, timestamp, process_time)
            server_queue.enqueue(request)
            req_num += 1

        for server in server_list:
            if (not server.busy()) and (not server_queue.is_empty()):
                next_request = server_queue.dequeue()
                waiting_times.append(next_request.wait_time(current_second))
                server.start_next(next_request)

            server.tick()

    average_wait = float(sum(waiting_times)) / len(waiting_times)
    print "Average Wait %6.2f secs %3d tasks remaining." % (average_wait, server_queue.size())

# main function -- argparse file and server file flags

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="url of csv file")
    parser.add_argument("--servers", help="number of servers (integer)")
    args = parser.parse_args()

    if args.file:
        try:
            raw_server_data = download_data(args.file)
            if not raw_server_data:                     # returns false, fails to connect
                print "Cannot connect to server, try again"

            else:
                csvFile = url_csv_read(raw_server_data)

                if args.servers:
                    simulateManyServers(csvFile, int(args.servers))

                else:
                    simulateOneServer(csvFile)

        except ValueError:
            print 'url is not valid'
    else:
        print "Please add url next to --file argument"

if __name__ == "__main__":
    main()
