# time stamp format in this program
TIME_FORMAT_STRING = '%H:%M:%S'

# static introducer addr
INTRODUCER_HOST = 'fa22-cs425-3201.cs.illinois.edu'

# made for introducer
ALL_HOSTS = [
    'fa22-cs425-3201.cs.illinois.edu',  # 01
    'fa22-cs425-3202.cs.illinois.edu',  # 02
    'fa22-cs425-3203.cs.illinois.edu',  # 03
    'fa22-cs425-3204.cs.illinois.edu',  # 04
    'fa22-cs425-3205.cs.illinois.edu',  # 05
    'fa22-cs425-3206.cs.illinois.edu',  # 06
    'fa22-cs425-3207.cs.illinois.edu',  # 07
    'fa22-cs425-3208.cs.illinois.edu',  # 08
    'fa22-cs425-3209.cs.illinois.edu',  # 09
    'fa22-cs425-3210.cs.illinois.edu',  # 10
]

# the designated coordinator and the hot standy-by coordinator
CANDIDATE_COORDINATORS = [
    'fa22-cs425-3201.cs.illinois.edu',  # 01
    'fa22-cs425-3202.cs.illinois.edu'  # 02
]

# the image folder we want to send to sdfs in the first place
IMAGE_FOLDER_PATH_LIST = ['image_test']

# default socket port
DEFAULT_PORT_FD = 52333
DEFAULT_PORT_SDFS = 53222
DEFAULT_PORT_CLIENT_INPUT = 54222
DEFAULT_PORT_COORDINATOR_INPUT = 55222
DEFAULT_PORT_WORKER_INPUT = 56222
DEFAULT_PORT_STANDBY_INPUT = 57222
DEFAULT_PORT_MAIN_COORD_INPUT = 58222
TRIAL_PORT = 5050

# topology of 10 VMs
# {source: [dest_1, dest_2, ...]}
CONNECTIONS = {
    'fa22-cs425-3201.cs.illinois.edu': [
        'fa22-cs425-3209.cs.illinois.edu',
        'fa22-cs425-3210.cs.illinois.edu',
        'fa22-cs425-3202.cs.illinois.edu',
        'fa22-cs425-3203.cs.illinois.edu',
    ],
    'fa22-cs425-3202.cs.illinois.edu': [
        'fa22-cs425-3203.cs.illinois.edu',
        'fa22-cs425-3204.cs.illinois.edu',
        'fa22-cs425-3201.cs.illinois.edu',
        'fa22-cs425-3210.cs.illinois.edu',
    ],
    'fa22-cs425-3203.cs.illinois.edu': [
        'fa22-cs425-3204.cs.illinois.edu',
        'fa22-cs425-3205.cs.illinois.edu',
        'fa22-cs425-3201.cs.illinois.edu',
        'fa22-cs425-3202.cs.illinois.edu',
    ],
    'fa22-cs425-3204.cs.illinois.edu': [
        'fa22-cs425-3205.cs.illinois.edu',
        'fa22-cs425-3206.cs.illinois.edu',
        'fa22-cs425-3202.cs.illinois.edu',
        'fa22-cs425-3203.cs.illinois.edu',
    ],
    'fa22-cs425-3205.cs.illinois.edu': [
        'fa22-cs425-3206.cs.illinois.edu',
        'fa22-cs425-3207.cs.illinois.edu',
        'fa22-cs425-3203.cs.illinois.edu',
        'fa22-cs425-3204.cs.illinois.edu',
    ],
    'fa22-cs425-3206.cs.illinois.edu': [
        'fa22-cs425-3207.cs.illinois.edu',
        'fa22-cs425-3208.cs.illinois.edu',
        'fa22-cs425-3204.cs.illinois.edu',
        'fa22-cs425-3205.cs.illinois.edu',
    ],
    'fa22-cs425-3207.cs.illinois.edu': [
        'fa22-cs425-3208.cs.illinois.edu',
        'fa22-cs425-3209.cs.illinois.edu',
        'fa22-cs425-3205.cs.illinois.edu',
        'fa22-cs425-3206.cs.illinois.edu',
    ],
    'fa22-cs425-3208.cs.illinois.edu': [
        'fa22-cs425-3209.cs.illinois.edu',
        'fa22-cs425-3210.cs.illinois.edu',
        'fa22-cs425-3206.cs.illinois.edu',
        'fa22-cs425-3207.cs.illinois.edu',
    ],
    'fa22-cs425-3209.cs.illinois.edu': [
        'fa22-cs425-3210.cs.illinois.edu',
        'fa22-cs425-3201.cs.illinois.edu',
        'fa22-cs425-3207.cs.illinois.edu',
        'fa22-cs425-3208.cs.illinois.edu',
    ],
    'fa22-cs425-3210.cs.illinois.edu': [
        'fa22-cs425-3201.cs.illinois.edu',
        'fa22-cs425-3202.cs.illinois.edu',
        'fa22-cs425-3208.cs.illinois.edu',
        'fa22-cs425-3209.cs.illinois.edu',
    ]
}
