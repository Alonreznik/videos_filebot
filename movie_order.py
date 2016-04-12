from ConfigParser import ConfigParser

import time
from guessit import guessit
import os

PATH_FORMAT = "{r}/{types}/{title}/Season {season}/{title}_S{season}E{episode}_{format}.{container}"
TYPES_DIR = {"episode": "TV Series",
             "movie": "Movies"}

ini_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
conf = ConfigParser()
conf.read(ini_path)
downloaded_dir = conf.get('PATHS','downloaded_path')
root = conf.get('PATHS', 'root_path')


def distinguish_by_port(port_num):
    """
    prot binding - the func will be distinguish each process by its port
    for preventing duplicate running instances
    """
    def func_wrapper(func):
        def wrapper(*args, **kwargs):
            port = port_num
            if not bind(int(port)):
                raise Exception('port {port} in use'.format(port=port))
            return func(*args, **kwargs)
        return wrapper
    return func_wrapper


def bind(port):
    """
    Binds to specified port and returns socket
    If port can't be bound - returns None
    This can be used to run single instance of a process
    """
    global socketHold
    try:
        import socket
        socketHold = socket.socket()
        host = socket.gethostname()
        socketHold.bind((host, port))
        return True
    except:
        return False


def get_filename_info(path_):
    videos_info = []
    if os.path.isdir(path_):
        for root, dirs, files in os.walk(path_):
            videos_info += [{'video_info': dict(guessit(f)), 'file_path': os.path.join(root, f)}
                            for f in files]
    elif os.path.isfile(path_):
        videos_info.append(guessit(os.path.basename(path_)))

    print videos_info
    if videos_info:
        videos_info = filter(lambda md: 'video/' in md.get('video_info', {}).get('mimetype', []),
                             videos_info)
    return videos_info


def make_dir_pattern(root_, videos_info):
    new_paths = {}
    for vid in videos_info:
        info = dict(vid.get('video_info'))
        new_paths.update({
            vid.get('file_path'): PATH_FORMAT.format(
                r=root_,
                types=TYPES_DIR[info.get('type')], **info)})
    return new_paths


def make_rename_and_move(new_paths):
    for fp, nfp in new_paths.iteritems():
        if not os.path.exists(os.path.dirname(nfp)):
            os.makedirs(os.path.abspath(nfp))

        os.rename(fp, nfp)


@distinguish_by_port(10000)
def run():
    while True:
        videos_info = get_filename_info(downloaded_dir)
        new_paths = make_dir_pattern(root, videos_info)
        make_rename_and_move(new_paths)

        time.sleep(60)


if __name__ == '__main__':
    run()