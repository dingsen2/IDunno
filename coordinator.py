import socket
from glob import *
import sdfs
import sys
import json
import threading
import pprint
import math
import collections
import time
import re
from datetime import datetime
import logging
import random

# receive input from client
ANIMAL = 'animal'
NUMBER = 'number'


class Coordinator:
    def __init__(self, host) -> None:
        self.host = host
        self.alive_mids = set()
        self.image_sent_mids = set()
        self.job_to_cnt = {}
        self.job_to_image_to_preds = {}
        self.vm_to_unsent_image_types = collections.defaultdict(list) # work assigned to vms, this dict doesn't pop, only for assignment purpose
        self.vm_to_assignment = collections.defaultdict(set) # add a (image, job_type) tuple when assigning, delete a tuple when this tuple's job accomplished, this records
                                                             # the work that have been assigned to this vm but haven't been accomplished
        self.vm_assign_lock = threading.Lock()
        self.client_addr = ''
        self.job_to_images = collections.defaultdict(list) # job to image_list (raw job list from client)
        self.send_number_job_cnt = 0
        self.send_animal_job_cnt = 0
        self.vm_assignment_cnt = collections.defaultdict(int)
        self.vm_accomplish_cnt = collections.defaultdict(int)
        self.number_processed_images = 0 # count number images that have been processed in the last period
        self.animal_processed_images = 0 # count animal images that have been processed in the last period
        self.total_number_images_processed = 0 # number of number images that have been processed thus far
        self.total_animal_images_processed = 0 # number of animal images that have been processed thus far
        self.batch_size = 10 # default batch size is 10
        self.last_check_time = datetime.now()
        self.animal_rate = 0
        self.number_rate = 0
        self.failed_mids = set() # set containing all the already failed mid
        # self.sdfs_server = sdfs.Server(self.host, DEFAULT_PORT_SDFS)

    def get_host_from_id(self, id):
        '''
        helper to get host from id
        '''
        return 'fa22-cs425-32%02d.cs.illinois.edu' % int(id)

    def update_processed_cnt(self, job_type):
        '''
        helper function to update the images that have been processed
        '''
        if job_type == ANIMAL:
            self.animal_processed_images += 1
            self.total_animal_images_processed += 1
            
        else:
            self.number_processed_images += 1
            self.total_number_images_processed += 1
    
    def update_send_list(self, curr_images, pop_cnt, send_list, job_type):
        '''
        add {pop_cnt} amount of (images, job_type) to {send_list}
        '''
        for _ in range(pop_cnt):
            send_list.append((curr_images.pop(), job_type))

    def update_job_distribution(self):
        '''
        update the job distribution according to the number_processed_images 
        and animal_processed_images
        '''
        if self.number_processed_images > self.animal_processed_images:
            if self.animal_processed_images == 0 or (self.number_processed_images - self.animal_processed_images) / self.animal_processed_images > 0.2:
                self.send_number_job_cnt = min(self.send_number_job_cnt - 1, len(self.job_to_images[NUMBER]))
                self.send_animal_job_cnt = min(self.send_animal_job_cnt + 1, len(self.job_to_images[ANIMAL]))
        elif self.number_processed_images <= self.animal_processed_images:
            if self.number_processed_images == 0 or (self.animal_processed_images - self.number_processed_images) / self.number_processed_images > 0.2:
                self.send_animal_job_cnt = min(self.send_animal_job_cnt - 1, len(self.job_to_images[ANIMAL]))
                self.send_number_job_cnt = min(self.send_number_job_cnt + 1, len(self.job_to_images[NUMBER]))

        # make sure that the job_cnt variables are within valid range
        self.send_animal_job_cnt = max(0, self.send_animal_job_cnt)
        self.send_animal_job_cnt = min(len(self.job_to_images[ANIMAL]), self.send_animal_job_cnt)
        self.send_number_job_cnt = max(0, self.send_number_job_cnt)
        self.send_number_job_cnt = min(len(self.job_to_images[NUMBER]), self.send_number_job_cnt)

    def assign_workers(self, cv_assign):
        '''
        Function to send assign worker messages.
        Require a condition variable.
        '''
        # FIXME: I use NUMBER and 'animal as the key'
        # 每次发出10个
        while True:
            send_list = []
            while not self.job_to_images[NUMBER] and not self.job_to_images[ANIMAL]:
                self.send_number_job_cnt = 0
                self.send_animal_job_cnt = 0
                print("acquire condition varible until we have some images for number/animal")
                with cv_assign:
                    cv_assign.wait()
            if not self.job_to_images[NUMBER] or not self.job_to_images[ANIMAL]:
                if self.job_to_images[NUMBER]:
                    # need to set the send_animal_job_cnt to 0
                    self.send_animal_job_cnt = 0
                    # currently we only have number jobs 
                    number_images = self.job_to_images[NUMBER]
                    #FIXME: I didn't use the len(alive_mids) to set the send_number_job_cnt
                    self.send_number_job_cnt = min(len(number_images), self.batch_size)
                    self.update_send_list(number_images, self.send_number_job_cnt, send_list, NUMBER)
                elif self.job_to_images[ANIMAL]:
                    # need to set the send_number_job_cnt to 0
                    self.send_number_job_cnt = 0
                    # currently we only have animal jobs 
                    animal_images = self.job_to_images[ANIMAL]
                    self.send_animal_job_cnt = min(len(animal_images), self.batch_size)
                    self.update_send_list(animal_images, self.send_animal_job_cnt, send_list, ANIMAL)
                
            else:
                # now we have two jobs(number and animal) that need to be dealt with
                if self.send_animal_job_cnt == 0 and self.send_number_job_cnt == 0:
                    order = random.randint(0,1)
                    if order == 0:
                        self.send_animal_job_cnt = 10
                    else:
                        self.send_number_job_cnt = 10
                cur_time = datetime.now()
                print("we might need to rebalance!")
                self.update_job_distribution()
                self.last_check_time = cur_time
                self.update_send_list(self.job_to_images[ANIMAL], self.send_animal_job_cnt,send_list, ANIMAL)
                self.update_send_list(self.job_to_images[NUMBER], self.send_number_job_cnt,send_list, NUMBER)
            if len(self.alive_mids) == 0:
                continue
            if self.send_animal_job_cnt == 0 and self.send_number_job_cnt == 0:
                # if last period send_animal_job_cnt and send_number_job_cnt are both 0; continue
                continue
            machine_assignment_count = math.ceil(len(send_list) / len(self.alive_mids))
            mid_list = list(self.alive_mids)
            # pprint.pprint(mid_list)
            # pprint.pprint(send_list)

            # assign work should be locked
            self.vm_assign_lock.acquire()
            for i, image_type_tuple in enumerate(send_list):
                # only add to the vm_to_unsent_image_types, don't send at this point
                # FIXME: assignment method right? My method: append image_type_tuple to the id's list
                target_host_id = mid_list[i // machine_assignment_count]
                self.vm_to_unsent_image_types[target_host_id].append(image_type_tuple)
                self.vm_to_assignment[target_host_id].add(image_type_tuple)
                self.vm_assignment_cnt[target_host_id] += 1
            self.vm_assign_lock.release()
            self.number_processed_images = 0
            self.animal_processed_images = 0
            # wait 10 seconds for sending the images, processing the image and its results
            time.sleep(10)
            # count the number of images processed in last 10 seconds and calculate the rate
            self.update_processing_rate()

    def update_processing_rate(self):
        '''
        function to update the processing rate of animal images and number images
        '''
        self.animal_rate = self.animal_processed_images / 10
        self.number_rate = self.number_processed_images / 10
        logging.info('animal rate: %s', self.animal_rate)
        logging.info('number rate: %s', self.number_rate)

    def send_assign_msg(self):
        '''
        function to send message out, always on
        '''
        while True:
            self.vm_assign_lock.acquire()
            for vm_id, image_type_tuples in self.vm_to_unsent_image_types.items():
                while image_type_tuples:
                    image_type_tuple = image_type_tuples.pop()
                    image_name, job_type = image_type_tuple[0], image_type_tuple[1]
                    assignment_msg = {
                        'type' : 'assign',
                        'job_type':job_type,
                        'file_name':image_name,
                        'src_coord_host':self.host
                    }
                    target_host = self.get_host_from_id(vm_id)
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        s.sendto(json.dumps(assignment_msg).encode('utf-8'), (target_host, DEFAULT_PORT_COORDINATOR_INPUT))
            self.vm_assign_lock.release()
            
    def receiver_client(self, cv_assign):
        '''
        receive information from client
        :cv_assign: condition variable to notify there are jobs coming
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_PORT_CLIENT_INPUT))
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']

                    # if message is from client, distribute the work to other workers
                    if msg_type == 'from_client':
                        job_type = msg['job_type']
                        self.client_addr = msg['client_addr']
                        image_file_names = list(msg['images'])
                        if job_type not in self.job_to_cnt:
                            print(job_type)
                            self.job_to_cnt[job_type] = 0
                            self.job_to_images[job_type] = []
                        self.job_to_cnt[f'{job_type}'] += len(image_file_names)
                        self.job_to_images[job_type].extend(image_file_names)
                        print("self.job_to_cnt" + str(self.job_to_cnt[job_type]))
                        print("self.job_to_images[job_type]" + str(len(self.job_to_images[job_type])))
                        print(f"add {job_type} to job_to_images")
                        with cv_assign:
                            cv_assign.notify_all()
                            print("we have something to assign, unlock the cv!")


    def add_failed_assignment(self, failed_unsent_image_types:set, cv_assign):
        '''
        helper function to add the failed vm's assignment back to self.job_to_images
        '''
        for image_name, job_type in failed_unsent_image_types:
            if job_type == ANIMAL:
                self.job_to_images[ANIMAL].append(image_name)
            elif job_type == NUMBER:
                self.job_to_images[NUMBER].append(image_name)
        with cv_assign:
            cv_assign.notify_all()
            print("failed machine's assignment readded, have something to assign, unlock the cv!")


    def receiver_worker(self, cv_assign):
        '''
        receive messages sent by worker
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_PORT_WORKER_INPUT))
            
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']
                    if msg_type == 'image_sent':
                        src_vm_id = msg['vm_id']
                        self.image_sent_mids.add(src_vm_id)
                        if len(self.image_sent_mids) == len(self.alive_mids):
                            print("INFO: Loading done, you can start inference!")
                    elif msg_type == 'worker_start':
                        src_vm_id = msg['vm_id']
                        self.alive_mids.add(src_vm_id)
                    elif msg_type == 'm_failed':
                        failed_mid = msg['failed_mid']
                        if failed_mid in self.failed_mids:
                            continue
                        self.failed_mids.add(failed_mid)
                        self.alive_mids.remove(failed_mid)
                        if NUMBER not in self.job_to_cnt and ANIMAL not in self.job_to_cnt:
                            continue
                        failed_unsent_image_types = self.vm_to_assignment[failed_mid]
                        pprint.pprint(f'machine: {failed_mid} failed, rejoin its work:{failed_unsent_image_types}')
                        del self.vm_to_assignment[failed_mid]
                        self.vm_assign_lock.acquire()
                        del self.vm_to_unsent_image_types[failed_mid]
                        self.vm_assign_lock.release()
                        # add the failed_unsent_image_types back to the self.job_to_images
                        self.add_failed_assignment(failed_unsent_image_types, cv_assign)
                    elif msg_type == 'pred':
                        
                        # this is a prediction message containing the image to its pred class
                        # pprint.pprint(msg)
                        job_type = msg['job_type']
                        image_name = msg['image_name']
                        result = msg['result']
                        src_vm_id = msg['vm_id']
                        pprint.pprint(msg)
                        # when accomplish a job, we delete that job from the vm assignment 
                        self.vm_to_assignment[src_vm_id].remove((image_name, job_type))
                        self.vm_accomplish_cnt[src_vm_id] += 1

                        self.update_processed_cnt(job_type)
                        # insert our image predictions to job_to_image_to_preds
                        unique_job_id = f'{job_type}'
                        if unique_job_id not in self.job_to_image_to_preds:
                            self.job_to_image_to_preds[unique_job_id] = {}
                        self.job_to_image_to_preds[unique_job_id][image_name] = result
                        # we have done this current job, write the result to a txt file
                        if len(self.job_to_image_to_preds[unique_job_id]) == self.job_to_cnt[unique_job_id]:
                            # also set the vm assigned to this job to be 0
                            if job_type == ANIMAL:
                                self.send_animal_job_cnt = 0
                            elif job_type == NUMBER:
                                self.send_number_job_cnt = 0

                            current_job_pred = self.job_to_image_to_preds[unique_job_id]
                            # store the prediction result
                            result_file_path = f'{unique_job_id}-result.txt'
                            with open(result_file_path, 'a') as result_file:
                                for key, value in current_job_pred.items():
                                    result_file.write(f'{key}:{value}\n')
                            store_file_msg = {
                                'type' : 'store',
                                'filename':result_file_path
                            }
                            s.sendto(json.dumps(store_file_msg).encode('utf-8'), (self.host, DEFAULT_PORT_COORDINATOR_INPUT))

                            # send the prediction result to client
                            job_complete_msg = {
                                'type':'accomplish',
                                'pred': current_job_pred,
                                'job_type':job_type,
                            }
                            self.number_processed_images = 0
                            self.animal_processed_images = 0
                            del self.job_to_image_to_preds[unique_job_id] # once this job finishes, delete it from the result dict
                            s.sendto(json.dumps(job_complete_msg).encode('utf-8'), (self.client_addr, DEFAULT_PORT_COORDINATOR_INPUT))
                            pprint.pprint(self.job_to_cnt)
                            pprint.pprint(self.job_to_image_to_preds)
                            pprint.pprint(job_complete_msg)


    def receiver_standby(self):
        '''
        receiver for standby coordinator
        once receiving standby's PING message, should ack back
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_PORT_STANDBY_INPUT))
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    # pprint.pprint(msg['type'] + ":" + datetime.now().strftime(TIME_FORMAT_STRING))
                    msg_type = msg['type']
                    if msg_type == 'standby_ping':
                        ack_msg = {
                            'type':'main_ack'
                        }
                        s.sendto(json.dumps(ack_msg).encode('utf-8'), ('fa22-cs425-3202.cs.illinois.edu', DEFAULT_PORT_MAIN_COORD_INPUT))

    def monitor(self):
        '''
        debug function to print stuff
        '''
        helper = '''
        ======  Command List  ======
        - lives
        - total_processed
        - last_processed
        - rate
        - distribution
        - cur_vm_work
        - pred_result
        - last_check_time
        - batch [number]
        - image_sent_vm
        - waiting_assignment
        - unaccomplished_assignment
        - unsent_assignment
        ============================
        '''
        print(helper)
        while True:
            arg = input('-->')
            args = arg.split(' ')
            if arg == '?' or arg == 'help':
                print(helper)
            elif arg == 'lives':
                print(self.alive_mids)
            elif arg == 'total_processed':
                print(f'total processed number: {self.total_number_images_processed}; total animal: {self.total_animal_images_processed}')    
            elif arg == 'last_processed':
                print(f'last period processed number: {self.number_processed_images}; last period animal: {self.animal_processed_images}')    
            elif arg == 'rate':
                print(f'number rate: {self.number_rate}; animal rate: {self.animal_rate}')
            elif arg == 'distribution':
                print(f'number vm: {self.send_number_job_cnt}; animal vm: {self.send_animal_job_cnt}')
            elif arg == 'unaccomplished_assignment':
                pprint.pprint(f'each vm work assignment: {self.vm_to_assignment}')
            elif arg == 'unsent_assignment':
                pprint.pprint(f'each vm to unsent distribution: {self.vm_to_unsent_image_types}')
            elif arg == 'pred_result':
                pprint.pprint(f'current result: {self.job_to_image_to_preds}')
            elif arg == 'last_check_time':
                pprint.pprint(self.last_check_time)
                pprint.pprint(datetime.now())
            elif arg == 'waiting_assignment':
                pprint.pprint(self.job_to_images)
            elif arg.startswith('batch'):
                if len(args) != 2:
                    print('[ERROR] batch [number]')
                print(f'set batch size to be {args[1]}')
                self.batch_size = int(args[1])
            elif arg == 'image_sent_vm':
                print(self.image_sent_mids)
            else:
                print('ERROR: command not found')


    
    def run(self):
        '''
        run the coordinator
        '''
        # self.sdfs_server.run()
        # init default SDFS path
        cv_assign = threading.Condition()
        t_receiver_client = threading.Thread(target=self.receiver_client, args=(cv_assign,))
        t_receiver_worker = threading.Thread(target=self.receiver_worker, args=(cv_assign,))
        # t_receiver_standby = threading.Thread(target=self.receiver_standby)
        t_assigner = threading.Thread(target=self.assign_workers, args=(cv_assign,))
        t_sender = threading.Thread(target=self.send_assign_msg)
        t_monitor = threading.Thread(target=self.monitor)
        # t_receiver_standby.start()
        t_monitor.start()
        t_sender.start()
        t_assigner.start()
        t_receiver_worker.start()
        t_receiver_client.start()
        # t_receiver_standby.join()
        t_monitor.join()
        t_sender.join()
        t_assigner.join()
        t_receiver_worker.join()
        t_receiver_client.join()
        


def main():
    logging.basicConfig(filename='rate.log', level=logging.DEBUG)
    logging.info('start at %s', {datetime.now().strftime(TIME_FORMAT_STRING)})
    c = Coordinator(host=socket.gethostname())
    c.run()


if __name__ == '__main__':
    main()