import os


if __name__ == "__main__":
    command = "python3 streaming.py & python3 dataToHdfs.py"
    os.system(command)