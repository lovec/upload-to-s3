from boto.s3.connection import S3Connection, S3ResponseError
import yaml, re, os, sys, settings
import MySQLdb as mdb

# Hack for S3 Frankfurt region
os.environ['S3_USE_SIGV4'] = 'True'

print('Email importer')

def load_config():
    with open('config.yaml', 'r') as config_file:
        return yaml.load(config_file)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

config = load_config()

connection = S3Connection(config['s3']['key_id'], config['s3']['access_key'], host=config['s3']['host'])
bucket = connection.get_bucket(config['s3']['buckets']['emails'])

mysql_connection = settings.connect_to_mysql(config)


def save_to_s3(content, filepath):
    try:
        print('Saving file', filepath)

        key = bucket.new_key(filepath)
        key.set_contents_from_string(content)
    except S3ResponseError:
        print('Unexpected error:', sys.exc_info()[0])


def key_exists(key):
    key = bucket.get_key(key)
    if not key:
        return False
    return True


def create_path(id):
    return re.sub(r"(.)", r"/\1", str(id))


def load_emails(limit):
    cursor = mysql_connection.cursor()
    cursor.execute("SELECT id, email_body FROM email_message where email_body IS NOT NULL LIMIT %d" % limit)

    return cursor


def remove_content_from_db(id):
    cursor = mysql_connection.cursor()
    cursor.execute("UPDATE email_message SET email_body = NULL WHERE id = %d" % id)
    mysql_connection.commit()


def migrate_emails():
    emails = load_emails(1)

    for (id, email_body) in emails:
        s3_filepath = create_path(id) + "/content.html"

        save_to_s3(email_body, s3_filepath)

        remove_content_from_db(id)


migrate_emails()

print('Finito!')