#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# %%
import sys
from datetime import datetime
from datetime import timedelta
from copy import deepcopy
import heapq as hq


# %%
class LogItem:
    def __init__(self, log_line):

        log_list = log_line.split(' ')
        self.hostname = log_list[0]
        self.time = datetime.strptime(log_list[3][1:], '%d/%b/%Y:%X')
        self.resource = log_list[6]
        self.code = log_list[-2]
        self.size = log_list[-1].strip('\n')
        self.whole = log_line.strip('\n')
        if self.size == '-':
            self.size = 0
        else:
            self.size = long(self.size)


# %%
class LogDataBase:
    def __init__(self):
        # for Feature 1
        self.hostname_count = {}
        # for Feature 2
        self.resource_count = {}
        # for Feature 3
        self.time_count = {}
        self.time_period_count = {}
        # for Feature 4
        self.wall_time = datetime.min
        self.block_list = set()
        self.block_list_time = {}
        self.login_failure_hostname = {}
        self.login_failure_time = {}
        self.wall_log = []
        # addition
        # self.data_dict = {}
        # self.total_nums = 0

    # database add new entry
    def update(self, log_item):
        # save each log to dict
        # self.data_dict[self.total_nums] = log_item
        # self.total_nums += 1
        # for Feature 1
        self.__add_count__(log_item.hostname, self.hostname_count)
        # for Feature 2
        self.__add_count__((log_item.resource, log_item.size), self.resource_count)
        # for Feature 3
        self.__add_count__(log_item.time, self.time_count)
        # for Feature 4
        self.__login_wall__(log_item)

    def __add_count__(self, log_attr, db_attr):
        if log_attr not in db_attr:
            db_attr[log_attr] = 1
        else:
            db_attr[log_attr] += 1

    # Feature 1
    def hostname_top10(self, output_file):
        # hostname_result = sorted(self.hostname_count.iteritems(), key = lambda x: x[1], reverse = True)[0:10]
        # use heap to get nlargest
        hostname_result = hq.nlargest(10, self.hostname_count.iteritems(), key=lambda x: x[1])
        fd = open(output_file, 'w')
        for result_line in hostname_result:
            fd.write(result_line[0] + ',' + str(result_line[1]) + '\n')
        fd.close()

    # Feature 2
    def resource_top10(self, output_file):
        # resource_result = sorted(self.resource_count.iteritems(), key=lambda x: x[0][1] * x[1], reverse = True)[0:10]
        # use heap to get nlargest
        resource_result = hq.nlargest(10, self.resource_count.iteritems(), key=lambda x: x[0][1] * x[1])
        fd = open(output_file, 'w')
        for result_line in resource_result:
            fd.write(result_line[0][0] + '\n')
        fd.close()

    # Feature 3
    def time_period_top10(self, output_file):
        sorted_keys = sorted(self.time_count.keys())
        # use time window to calculate sum in each time period
        time_window = []
        count_sum = 0
        time_period_result = []
        time_delta = timedelta(hours=1)
        # count in each time period
        for key in sorted_keys:
            count_sum += self.time_count[key]
            time_window.append(key)
            start_time = key - time_delta
            while time_window[0] <= start_time:
                self.time_period_count[time_window[0]] = count_sum - self.time_count[key]
                count_sum -= self.time_count[time_window[0]]
                time_window.pop(0)

        time_period_count = deepcopy(self.time_period_count)
        # find top10
        for i in range(1, 11):
            # use heap to get nlargest
            count_max = hq.nlargest(1, time_period_count.items(), key=lambda x: x[1])
            time_period_result.append(count_max[0])
            count_max_time = count_max[0][0]
            '''
            00:00:01 - 01:00:00
            01:00:01 - 02:00:00
            02:00:01 - 03:00:00
            '''
            period_start = count_max_time - time_delta
            period_end = count_max_time + time_delta
            # delete common time period
            time_period_count_keys = time_period_count.keys()
            for key in time_period_count_keys:
                if period_start < key < period_end:
                    del time_period_count[key]
        # write to file
        fd = open(output_file, 'w')
        for result_line in time_period_result:
            fd.write(result_line[0].strftime('%d/%b/%Y:%X') + ' -0400,' + str(result_line[1]) + '\n')
        fd.close()

    # Feature 4
    def blocked_log(self, output_file):
        fd = open(output_file, 'w')
        for result_line in self.wall_log:
            fd.write(result_line + '\n')
        fd.close()

    def __login_wall__(self, log_item):
        # wall_time update
        if self.wall_time < log_item.time:
            self.wall_time = log_item.time
            self.__wall_time_update__(self.wall_time)
        # check if already blocked
        # connection blocked
        if log_item.hostname in self.block_list:
            self.wall_log.append(log_item.whole)
            return
        # login failed
        if log_item.code == '401' and log_item.resource == '/login':
            # the value of login_failure_hostname is minimum heap
            try:
                hq.heappush(self.login_failure_hostname[log_item.hostname], log_item.time)
            except KeyError:
                self.login_failure_hostname[log_item.hostname] = [log_item.time]
            try:
                # set login time of this hostname
                self.login_failure_time[self.login_failure_hostname[log_item.hostname][0]].add(log_item.hostname)
            except KeyError:
                self.login_failure_time[self.login_failure_hostname[log_item.hostname][0]] = set([log_item.hostname])
            if len(self.login_failure_hostname[log_item.hostname]) == 3:
                self.block_list.add(log_item.hostname)
                try:
                    self.block_list_time[log_item.time + timedelta(minutes=5)].add(log_item.hostname)
                except KeyError:
                    self.block_list_time[log_item.time + timedelta(minutes=5)] = set([log_item.hostname])
            return

        # login succeed
        if log_item.code == '200' and log_item.resource == '/login':
            try:
                for login_time in self.login_failure_hostname[log_item.hostname]:
                    self.login_failure_time[login_time].discard(log_item.hostname)
            except KeyError:
                pass
            try:
                del self.login_failure_hostname[log_item.hostname]
            except KeyError:
                pass
            return

    '''
    self.login_failure_time is a dictionary which stores the login time and 'login-failure' hostnames at that moment.
    self.login_failure_hostname is a dictionary whose keys is hostname, and values is a minimum heap of hostname's login time.

    If keys(login time) in self.login_failure_time are overdue, find the hostnames under this key, and pop the minimum value from heap, which is same as key.
    Because the start_time_key is from smallest one.
    Finally just delete this key-val pair(login time, hostnames) in login_failure_time.
    '''

    def __wall_time_update__(self, wall_time):
        # delete all overdue from block list
        for end_time_key in self.block_list_time.keys():
            if end_time_key < wall_time:
                for block_hostname in self.block_list_time[end_time_key]:
                    self.block_list.discard(block_hostname)
                del self.block_list_time[end_time_key]
        # delete from login_failure
        for start_time_key in sorted(self.login_failure_time.keys()):
            end_time_key = start_time_key + timedelta(seconds=20)
            if end_time_key < wall_time:
                for failure_hostname in self.login_failure_time[start_time_key]:
                    self.login_failure_hostname[failure_hostname].pop(0)
                    if len(self.login_failure_hostname[failure_hostname]) == 0:
                        del self.login_failure_hostname[failure_hostname]
                del self.login_failure_time[start_time_key]
        return


# %%
def main():
    argv = sys.argv

    log_file = open(argv[1], 'r')
    logdb = LogDataBase()
    eachline = log_file.readline()
    logdb.update(LogItem(eachline))
    while eachline:
        logdb.update(LogItem(eachline))
        eachline = log_file.readline()
    logdb.hostname_top10(argv[2])
    logdb.resource_top10(argv[4])
    logdb.time_period_top10(argv[3])
    logdb.blocked_log(argv[5])


if __name__ == '__main__':
    main()
