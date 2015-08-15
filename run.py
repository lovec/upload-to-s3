from boto.s3.connection import S3Connection
from os.path import isfile, join, isdir
from os import listdir
import yaml, re, os, sys

# Hack for S3 Frankfurt region
os.environ['S3_USE_SIGV4'] = 'True'

ignore_files = ['.DS_Store']

print('File importer')


def validate_arguments():
    if len(sys.argv) < 2:
        print('Error: Please add path to directory or file as first argument')
        exit()
    if not isdir(sys.argv[1]) and not isfile(sys.argv[1]):
        print('Error: %s does not exists' % sys.argv[1])
        exit()


validate_arguments()


def load_config():
    with open('config.yaml', 'r') as config_file:
        return yaml.load(config_file)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

config = load_config()
connection = S3Connection(config['s3']['key_id'], config['s3']['access_key'], host=config['s3']['host'])

bucket = connection.get_bucket(config['s3']['bucket'])


def should_upload_file(filepath):
    # filename containing w123 which stands for width and number of pixels
    if re.search('w\d{3}', filepath) or filepath in ignore_files:
        return False
    else:
        return True


def upload_file_to_s3(path, file):
    print('Uploading file', file)
    key = bucket.new_key(file)
    key.set_contents_from_filename(path + file, cb=percent_cb, num_cb=10)


def upload_file(path, file):
    if should_upload_file(file):
        upload_file_to_s3(path, file)
    else:
        print('Not uploading: ', file)


def scan_dir(basepath, filepath):
    for file in listdir(basepath + filepath):
        if isfile(join(basepath + filepath, file)):
            upload_file(basepath, filepath + '/' +file)
        else:
            print("Continue into directory", file)
            scan_dir(basepath, filepath + '/' + file)


scan_dir(sys.argv[1], '')

print('Finito!')