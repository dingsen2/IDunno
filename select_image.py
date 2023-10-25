# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 12:28:33 2022

"""

import os
import shutil
import argparse
import random



def add_image_path_to_list(image_folder_path, image_list):
    for child in os.listdir(image_folder_path):
        path = os.path.join(image_folder_path, child)
        if os.path.isdir(path):
            add_image_path_to_list(path, image_list)
        elif path.endswith('png') or path.endswith('jpg'):
            # put the image path into our image_list
            image_list.append(path)
            
            
def select_images(image_folder_path, target_folder_path, selected_num):
    
    image_list = []  
    add_image_path_to_list(image_folder_path, image_list)
    image_list = random.sample(image_list, min(selected_num, len(image_list)))
    for i in image_list:
        shutil.copyfile(i, f'{target_folder_path}/{os.path.basename(i)}')
    return image_list
            
            
def basename2txt(job_type, image_list):          
    with open(f'{job_type}.txt', 'w') as f:
        for i in image_list:
            i_basename = os.path.basename(i)
            f.write(i_basename + '\n')
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                description=__doc__,
                formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--src',
                        metavar='<src_path>',
                        help='the folder path containing the images',
                        required=True)
    parser.add_argument('--target_path',
                        metavar='<image_folder_path>',
                        help='the folder path to store those images',
                        required=True)
    parser.add_argument('--amount',
                        metavar='<amount>',
                        help='the amount of images we want',
                        required=True)
    parser.add_argument('--job',
                        metavar='<job>',
                        help='the job_type',
                        required=True)
    args = parser.parse_args()
    src = args.src
    target_path = args.target_path
    selected_img_num = int(args.amount)
    job_type = args.job
    if os.path.exists(target_path) is False:
        os.mkdir(target_path)
    for f in os.listdir(target_path):
        os.remove(os.path.join(target_path, f))
    image_list = select_images(src, target_path, selected_img_num)
    basename2txt(job_type,image_list)

    

    

# python3 select_image.py --src cat_dog_testing_set --target_path animal_images --amount 100 --job animal
# python3 select_image.py --src mnist_testing --target_path number_images --amount 100 --job animal