# IDunno: A distributed inference framework
## Usage

### Run Server

Similar to failure detector, we only need to run a single script to start both failure
detecor and SDFS srever (in different ports).

```bash
$ python3 sdfs.py

```

Then it will continuously print membership list for the node with default status
`LEAVED`. To join the group, use the following interface:

You are able to input directly to the terminal as command including

- `join`: join the node to group
- `leave`: leave the group
- `ml`: print current node's membership list

### Simple Distributed File System Commands

Except for commands from failure detector (`join`, `leave` and `ml`), the
following commands are used to control SDFS:

- `get [sdfs_file_name] [local_file_name]`: get a (latest version) file from SDFS
- `get-versions [sdfs_file_name] [num-versions] [local_file_name]`: get versions file from SDFS
- `put [local_file_name] [sdfs_file_name]`: put a local to SDFS
- `delete [sdfs_file_name]`: delete a file in SDFS
- `ls [sdfs_file_name]`: list all replicas for a file stored in SDFS
- `store`: list all files store in SDFS and their replicas
- `lives`: check all current available replicas

### Steps
- ssh into each vm and cd to the folder  
- run the select image script to get 'amount' of images  
```
python3 select_image.py --src cat_dog_testing_set --target_path animal_images --amount [amount] --job animal
python3 select_image.py --src mnist_testing --target_path number_images --amount [amount] --job number
```
- copy the number.txt and animal.txt files generated in the last step to the client machine (running on the client folder)
```
sshpass -p 'Yyyy19491001' scp 'fa22-cs425-3201.cs.illinois.edu':/public/mp4-other-version/animal.txt animal.txt
sshpass -p 'Yyyy19491001' scp 'fa22-cs425-3201.cs.illinois.edu':/public/mp4-other-version/number.txt number.txt
```

- run the 'send_images folder' script to send those images to 'images' folder on each vm
```
python3 send_images.py --image_folder_path animal_images --last_mid 10
python3 send_images.py --image_folder_path number_images --last_mid 10
```

- run the coordinator and the standby coordinator on VM1 and VM2
```
python3 coordinator.py -vm1
python3 coordinator_standby.py -vm2
```

- run the worker on each VM
```
python3 worker_local.py

```

- join the worker on each VM
```
join

```

- run 'putfolder amount images' on each VM
```
putfolder 1000 images

```

- run the 'job_type file_list.txt' command in the client side(start infer)
```
animal animal.txt
number number.txt
```
