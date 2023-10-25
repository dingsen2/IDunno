import logging
from glob import *
import socket
import json
import os
from pathlib import Path

# def store_all_result(image_folder_path_list:list):
#     '''
#     store all images and its classes to the job_to_testres
#     '''
#     job_to_testres = {}
#     for image_folder_path in image_folder_path_list:
#         print(os.listdir(image_folder_path))
#         store_all_result_in_current_path(image_folder_path, job_to_testres)
#     print(job_to_testres)

# def store_all_result_in_current_path(image_folder_path:str, job_to_testres):
#     for child in os.listdir(image_folder_path):
#         path = os.path.join(image_folder_path, child)
#         print(path)
#         if os.path.isdir(path):
#             store_all_result_in_current_path(path, job_to_testres)
#         elif path.endswith('.png') or path.endswith('.jpg'):
#             job_to_testres[child] = os.path.basename(os.path.dirname(path))
def print_stuff():
    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('And this, too')
    logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

# print(store_all_result(['/public/mp4-other-version/cat_dog_testing_set', '/public/mp4-other-version/mnist_testing']))

if __name__ == '__main__':
    while True:
        arg = input('-->')
        x = str(arg)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            client_input_msg = {
                'type' : 'processed_cnt',
                'animal_processed':str(200),
            }
            s.sendto(json.dumps(client_input_msg).encode('utf-8'), ('wirelessprv-10-193-156-87.near.illinois.edu', TRIAL_PORT))
