import os

def directory_exists(directory):
    return os.path.isdir(directory)

# usage
print(directory_exists("//Users//zebra//Desktop//google-cloud-sdk"))  # replace with your directory