import os


def startup_message():
    msg = "Program is starting. Working directory: \n" + os.getcwd()
    print(msg)
