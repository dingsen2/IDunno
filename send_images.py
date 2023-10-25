import os
import socket
import argparse
from glob import *
import math
import subprocess

IMAGES_PATH = '/public/mp4-other-version/images'

def get_host_from_id(host_id):
        """
        e.g. 1 -> fa22-cs425-3201.cs.illinois.edu
        :param host_id: int id
        :return: host str
        """
        return 'fa22-cs425-32%02d.cs.illinois.edu' % host_id

def add_image_path_to_list(image_folder_path, image_list):
    for child in os.listdir(image_folder_path):
        path = os.path.join(image_folder_path, child)
        if os.path.isdir(path):
            add_image_path_to_list(path, image_list)
        else:
            # put the image path into our image_list
            image_list.append(path)

def send_image_handler(image_folder_path:str, last_machine_idx:int):
    # first 2 vms are coordinator and standby coordinator
    print("alive machine should be started from 3 to last_machine_idx + 1")
    if last_machine_idx <= 3:
        print("ERROR: last machine should be at least 3")
    image_list = []
    add_image_path_to_list(image_folder_path, image_list)
    machine_assignment_count = math.ceil(len(image_list) / (last_machine_idx - 2))
    # split image_list into 10 list
    sub_image_list = [[] for _ in range(last_machine_idx - 2)]
    
    for x in range(last_machine_idx - 2):
        sub_image_list[x] = image_list[x*machine_assignment_count:x*machine_assignment_count + machine_assignment_count]

    for i in range(3,1 + last_machine_idx):
        prefix = 'dingsen2' + '@' + get_host_from_id(i)
        
        for j in range(len(sub_image_list[i - 3])):
            image_file_name = os.path.basename(sub_image_list[i - 3][j])
            images_path = os.path.join(IMAGES_PATH, image_file_name)
            p = subprocess.Popen(['sshpass', '-p', 'Yyyy19491001','scp',
                sub_image_list[i - 3][j],
                prefix + ':' + images_path])
            os.waitpid(p.pid, 0)
        print(f'sent for vm{i}')
    print(f"send all files in {image_folder_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                description=__doc__,
                formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--image_folder_path',
                        metavar='<image_folder_path>',
                        help='the folder path containing the images',
                        required=True)
    parser.add_argument('--last_mid',
                        metavar='<last_mid>',
                        help='last machines to send images on',
                        required=True)
    args = parser.parse_args()
    send_image_handler(args.image_folder_path, int(args.last_mid))