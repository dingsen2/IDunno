from datetime import datetime
import socket
from glob import *
import json
import os
import threading

class Client:
    '''
    Client class to receive input
    and incoming messages from coordinator
    '''
    def __init__(self) -> None:
        self.host = socket.gethostname()
        self.port = DEFAULT_PORT_COORDINATOR_INPUT
        self.addr = (self.host, self.port)
        self.job_to_testres = {}
    
    def store_all_result(self, image_folder_path_list:list):
        '''
        store all images and its classes to the job_to_testres
        '''
        for image_folder_path in image_folder_path_list:
            print(os.listdir(image_folder_path))
            self.store_all_result_in_current_path(image_folder_path, self.job_to_testres)

    def store_all_result_in_current_path(self, image_folder_path:str, job_to_testres):
        '''
        helper to store all test set result into a class dict
        '''
        for child in os.listdir(image_folder_path):
            path = os.path.join(image_folder_path, child)
            print(path)
            if os.path.isdir(path):
                self.store_all_result_in_current_path(path, job_to_testres)
            elif path.endswith('.png') or path.endswith('.jpg'):
                job_to_testres[child] = os.path.basename(os.path.dirname(path))


    def monitor(self):
        '''
        monitor stdin input
        '''
        helper = '''
        ======  Command List  ======
        - [job type][file_list.txt]
        e.g. animal image_list.txt
             number image_list.txt
        ============================
        '''
        print(helper)
        while True:
            arg = input('-->')
            args = arg.split(' ')
            if len(args) != 2:
                print("2 variables!")
                continue

            elif not args[1].endswith('.txt') :
                print("second parameter must be a .txt file!")
                continue

            elif args[0] == "animal" or args[0] == "number":
                # handle animal
                self.input_handler(args[0], args[1])
            else:
                print("Unknown commands")

    def input_handler(self, job_type:str, file_list_path:str):
        input_image_file = open(file_list_path, 'r')
        lines = input_image_file.readlines()
        image_list = []
        for line in lines:
            image_list.append(line.strip())
        # 

        # send the image set and job type to coordinator
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            client_input_msg = {
                'type':'from_client',
                'job_type': job_type,
                'job_id':datetime.now().strftime("%m-%d-%Y-%H-%M-%S"),
                'images':image_list,
                'client_addr':self.host
            }
            # send message to both standby and current coordinator, 
            # if current doesn't die, standby do nothing
            for host in CANDIDATE_COORDINATORS:
                s.sendto(json.dumps(client_input_msg).encode('utf-8'), (host, DEFAULT_PORT_CLIENT_INPUT))
    
    def receiver(self):
        '''
        receive message from coordinator
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']

                    # if message is from client, distribute the work to other workers
                    if msg_type == 'accomplish':
                        job_type = msg['job_type']
                        image_to_pred = msg['pred']


    def run(self):
        self.store_all_result(['/Users/dingsen2/Desktop/uiuc-mcs/425/mp/mp4-other-version/mnist_testing', '/Users/dingsen2/Desktop/uiuc-mcs/425/mp/mp4-other-version/cat_dog_testing_set'])
        t_receiver = threading.Thread(target=self.receiver)
        t_monitor = threading.Thread(target=self.monitor)
        t_receiver.start()
        t_monitor.start()
        t_receiver.join()
        t_monitor.join()

def main():
    client = Client()
    client.run()

if __name__ == "__main__":
    main()