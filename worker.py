import sdfs
from glob import *
import json
import os
import numpy as np
from tensorflow import keras
import tensorflow as tf
import imageio
import socket
import shutil
import threading
import pprint

resnet_model = tf.keras.models.load_model('resnet_model')
mnist_model = tf.keras.models.load_model('lenet_model')

class Worker:
    """
    worker nodes to predict the images
    """
    def __init__(self, host) -> None:
        self.host = host
        self.sdfs_server = sdfs.Server(host=self.host, port=DEFAULT_PORT_SDFS)

    def reshape_input(self, image_path:str):
        """
        reshape the image, util function
        image_path:str
        """
        img = imageio.imread(image_path).astype(np.float)
        pad_image = tf.pad(img, [[2,2], [2,2]])/255
        ret = tf.expand_dims(pad_image, axis=0, name=None)
        ret = tf.expand_dims(ret, axis=3, name=None)
        return ret

    def send_amount_images(self, amount, image_folder_path:str):
        '''
        helper function to send amount images in target folder(flat file structure)
        '''
        sent_count = 0
        for child in os.listdir(image_folder_path):
            if sent_count >= int(amount):
                break
            path = os.path.join(image_folder_path, child)
            print(path + " " + child)
            self.sdfs_server.put_file(path, child)
            sent_count += 1
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            image_sent_msg = {
                'type':'image_sent',
                'vm_id':str(self.sdfs_server.get_id_from_host(socket.gethostname()))
            }
            for host in CANDIDATE_COORDINATORS:
                s.sendto(json.dumps(image_sent_msg).encode('utf-8'), (host, DEFAULT_PORT_WORKER_INPUT))
        
        print(f"sent {sent_count} images to sdfs")

        
    def get_images(self, image_name_list, image_dir_path):
        """
        get the images from sdfs to local
        image_name_list:list list of image names we want to get
        image_dir_path: the folder we want to place our newly gotten images in
        """
        for image_name in image_name_list:
            self.sdfs_server.get_file(image_name, os.path.join(image_dir_path,image_name))
        
    def predict_image(self, image_path, job_type):
        """
        predict the results of images in image_folder_path
        image_folder_path:str the target image folder path
        job_type:str 'animal' or 'number'
        """
        result = ''
        if job_type == 'animal':
            load_image = keras.preprocessing.image.load_img(
                image_path,
                target_size=(224, 224)
            )
            load_image = np.array(load_image)
            load_image = np.expand_dims(load_image, axis=0)
            load_image = keras.applications.resnet50.preprocess_input(load_image)
            prob = resnet_model.predict(load_image)[0][0]
            if prob < 0.5:
                result = 'cats'
            elif prob >= 0.5:
                result = 'dogs'
        elif job_type == 'number':
            load_image = self.reshape_input(image_path)
            prob_list = mnist_model.predict(load_image)
            pred_class = np.argmax(prob_list, axis=1)
            result = str(pred_class[0])
        return result
    
    def receiver(self):
        """
        receive messages from coordinator
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_PORT_COORDINATOR_INPUT))
            while True:
                print("start receiving")
                data, server = s.recvfrom(4096)
                
                if data:
                    
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']
                    # message to assign work to worker
                    # worker need to process the data and return results back
                    if msg_type == 'assign':
                        # pprint.pprint(msg)
                        job_type = msg['job_type']
                        file_name = msg['file_name']
                        src_coord_host = msg['src_coord_host']
                        self.sdfs_server.get_file(file_name, file_name)
                        # return back one image's prediction
                        pred_result = self.predict_image(file_name, job_type)
                        os.remove(file_name)
                        pred_msg = {
                            'type':'pred',
                            'vm_id' : str(self.sdfs_server.get_id_from_host(self.host)),
                            'job_type':job_type,
                            'image_name':file_name,
                            'result':pred_result
                        }
                        #TODO: send the pred_msg back to the coordinator with designated address
                        s.sendto(json.dumps(pred_msg).encode('utf-8'), (src_coord_host, DEFAULT_PORT_WORKER_INPUT))
                
                    elif msg_type == 'store':
                        # store the designated prediction file into sdfs and delete it afterwards
                        filename = msg['filename']
                        if filename not in self.sdfs_server.ft.fm:
                            self.sdfs_server.put_file(filename, filename)
                        os.remove(filename)
    
    def send_worker_start_msg(self):
        '''
        helper function to notify coordinator that this worker is on
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            worker_start_msg = {
                'type':'worker_start',
                'vm_id':str(self.sdfs_server.get_id_from_host(socket.gethostname()))
            }
            for host in CANDIDATE_COORDINATORS:
                s.sendto(json.dumps(worker_start_msg).encode('utf-8'), (host, DEFAULT_PORT_WORKER_INPUT))
        
    def run(self):
        self.sdfs_server.run()
        self.send_worker_start_msg()
        t_receiver = threading.Thread(target=self.receiver)
        t_monitor = threading.Thread(target=self.monitor)
        t_monitor.start()
        t_receiver.start()
        t_monitor.join()
        t_receiver.join()

    def monitor(self):
        helper = '''
        ======  Command List  ======
        - get [sdfs_file_name] [local_file_name]
        - get-versions [sdfs_file_name] [num-versions] [local_file_name]
        - put [local_file_name] [sdfs_file_name]
        - putfolder [amount] [folder_path]
        - delete [sdfs_file_name]
        - ls [sdfs_file_name]
        - store
        - ml
        - join
        - leave
        - lives
        ============================
        '''
        print(helper)
        while True:
            arg = input('-->')
            args = arg.split(' ')
            if arg == '?' or arg == 'help':
                print(helper)
            elif arg.startswith('get-versions'):
                if len(args) != 4:
                    print('[ERROR FORMAT] get-versions sdfs_file_name num-versions local_file_name')
                    continue
                self.sdfs_server.get_file(args[1], args[3], num_version=int(args[2]))
            elif arg.startswith('get'):
                if len(args) != 3:
                    print('[ERROR FORMAT] get sdfs_file_name local_file_name')
                    continue
                self.sdfs_server.get_file(args[1], args[2])
            elif arg.startswith('putfolder'):
                if len(args) != 3:
                    print('[ERROR FORMAT] putfolder amount folder_path')
                    continue
                self.send_amount_images(args[1], args[2])
            elif arg.startswith('put'):
                if len(args) != 3:
                    print('[ERROR FORMAT] put local_file_name sdfs_file_name')
                    continue
                self.sdfs_server.put_file(args[1], args[2])
            elif arg.startswith('delete'):
                if len(args) != 2:
                    print('[ERROR FORMAT] delete sdfs_file_name')
                    continue
                self.sdfs_server.delete_file(args[1])
            elif arg.startswith('ls'):
                if len(args) != 2:
                    print('[ERROR FORMAT] ls sdfs_file_name')
                    continue
                self.sdfs_server.list_sdfs_file(args[1])
            elif arg.startswith('store'):
                self.sdfs_server.show_store()
            elif arg == 'fm':
                pprint.pprint(self.sdfs_server.ft.fm)
            elif arg == 'idm':
                pprint.pprint(self.sdfs_server.ft.idm)
            elif arg == 'join':
                self.sdfs_server.failure_detector.join()
            elif arg == 'leave':
                self.sdfs_server.failure_detector.leave()
            elif arg == 'ml':
                self.sdfs_server.failure_detector.print_ml()
            elif arg == 'lives':
                print(self.sdfs_server.lives)
            else:
                print(f'[ERROR] Invalid input arg {arg}')



def main():
    #FIXME: add a store print statement for debugging
    w = Worker(host=socket.gethostname())
    w.run()


if __name__ == '__main__':
    main()

# putfolder amount images
