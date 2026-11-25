import threading
import time

debug = True

# the application and the protocol thread print at the same time
lock = threading.Lock()


# print a whole message without mixing it with the other thread's
def show(message):
    with lock:
        print(message, flush=True)


def debug_log(message):
    if debug:
        show(message)

# format a time.time_ns() value in local time with microseconds
def format_time(val):
    f_time = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(val // 1000000000))
    us = (val // 1000) % 1000000
    return f_time + f".{us:06d}"
