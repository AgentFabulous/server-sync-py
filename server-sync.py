#!/usr/bin/python3

import asyncio
import hashlib
import json
import os
import shutil
import socket
import time
from pathlib import Path

import requests
from google.cloud import firestore
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

MASTER = os.environ.get('MASTER_NODE').lower() == 'true'
DIRECTORY_TO_WATCH = "/raid/h5ai"
MASTER_WORKING_DIR = "/extra_ssd"
API_AUTH_KEY = os.environ.get('API_AUTH_KEY')
SSH_USER = os.environ.get('SSH_USER')
SF_KEY = "__internal__/sfkey" if MASTER else "/raid/secrets/sfkey"
FB_KEY = "__internal__/credentials.json" if MASTER else "/raid/secrets/credentials.json"
SF_KEY_EXTRA = os.environ.get('SF_KEY_EXTRA')
FB_KEY_EXTRA = os.environ.get('FB_KEY_EXTRA')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = FB_KEY
file_list = []
id_list = []
doc_id_list = None
master_job_list = {}


# noinspection PyDictCreation
def retrieve_secrets():
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    headers['Extra'] = SF_KEY_EXTRA
    print('--> GET sfkey')
    response = requests.get('https://posp-server-sync.web.app/api/v1/file/sfkey', headers=headers)
    print('<-- Response:', response.status_code)
    if response.status_code == 200:
        with open(SF_KEY, 'wb') as handle:
            handle.write(response.content)
            handle.write(b'\n')
        os.chmod(SF_KEY, 0o600)
        print('[.] Wrote sfkey')
    headers['Extra'] = FB_KEY_EXTRA
    print('--> GET fbkey')
    response = requests.get('https://posp-server-sync.web.app/api/v1/file/fbkey', headers=headers)
    print('<-- Response:', response.status_code)
    if response.status_code == 200:
        with open(FB_KEY, 'wb') as handle:
            handle.write(response.content)
        print('[.] Wrote fbkey')


def fire_and_forget(f):
    def wrapped(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_in_executor(None, f, *args, *kwargs)

    return wrapped


def get_hostname():
    return socket.gethostname().lower()


def make_checked_dirs(name):
    if not os.path.exists(name):
        os.makedirs(name)


def sha1_internal(fname):
    hash_sha1 = hashlib.sha1()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha1.update(chunk)
    return hash_sha1.hexdigest()


def sha1(fname):
    hash_sha1 = hashlib.sha1((sha1_internal(fname) + ':' + fname).encode('utf-8'))
    return hash_sha1.hexdigest()


def add_file(name, id):
    mirrors = [get_hostname()]
    body = {
        'name': os.path.relpath(name, DIRECTORY_TO_WATCH),
        'id': id,
        'mirrors': mirrors,
    }
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    print('--> POST file {}:{}@{}'.format(name, id, mirrors))
    response = requests.post('https://posp-server-sync.web.app/api/v1/file', data=json.dumps(body), headers=headers)
    print('<-- Response:', response)


def delete_file(id):
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    print('--> DELETE file {}'.format(id))
    response = requests.delete('https://posp-server-sync.web.app/api/v1/file/' + id, headers=headers)
    print('<-- Response:', response)


def add_mirror(id):
    mirrors = [get_hostname()]
    body = {
        'mirrors': mirrors,
    }
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    print('--> POST mirror {}@{}'.format(id, mirrors))
    response = requests.post('https://posp-server-sync.web.app/api/v1/file/mirror/' + id, data=json.dumps(body),
                             headers=headers)
    print('<-- Response:', response)


def delete_mirror(id):
    mirrors = [get_hostname()]
    body = {
        'mirrors': mirrors,
    }
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    print('--> DELETE mirror {}@{}'.format(id, mirrors))
    response = requests.delete('https://posp-server-sync.web.app/api/v1/file/mirror/' + id, data=json.dumps(body),
                               headers=headers)
    print('<-- Response:', response)


def set_sfupload(id):
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    print('--> POST sfupload {}@'.format(id))
    response = requests.post('https://posp-server-sync.web.app/api/v1/file/sfupload/' + id, headers=headers)
    print('<-- Response:', response)


def unset_sfupload(id):
    headers = {
        'Authorization': API_AUTH_KEY,
        'Content-Type': 'application/json'
    }
    print('--> DELETE sfupload {}'.format(id))
    response = requests.delete('https://posp-server-sync.web.app/api/v1/file/sfupload/' + id, headers=headers)
    print('<-- Response:', response)


def get_url(data):
    eligible_mirrors = [x for x in data['mirrors'] if x is not get_hostname()]
    if len(eligible_mirrors) == 0:
        print('[!] Failed to retrieve {}. Deleting entry!'.format(data['id']))
        delete_file(data['id'])
        return None
    return 'http://{}-mirror.potatoproject.co/{}'.format(eligible_mirrors[0], data['name'])


@fire_and_forget
def process_snapshot(data):
    make_checked_dirs('__internal__/{}'.format(data['id']))
    with open('__internal__/{}/{}.json'.format(data['id'], data['id']), 'w') as handle:
        handle.write(json.dumps(data, indent=4, sort_keys=True))
        url = get_url(data)
        if url is not None:
            lockfile = '__internal__/{}/{}.lock'.format(data['id'], data['id'])
            if os.path.exists(lockfile):
                print('[!] Lock present for {}! Ignoring request.'.format(data['id']))
                return
            if os.path.exists(data['name']):
                print('[!] File present for {}! Ignoring request and cleaning up.'.format(data['id']))
                shutil.rmtree('__internal__/{}'.format(data['id']))
                return
            with open(lockfile, 'w'):
                print('[*] Acquiring lock for {}'.format(data['id']))
                pass
            print("--- Downloading {}".format(data['id']))
            ret = os.system("wget -P {} {}".format('__internal__/{}/ > /dev/null 2>&1'.format(data['id']), url))
            if ret == 0:
                if os.path.dirname(data['name']).strip() != '':
                    make_checked_dirs(os.path.dirname(data['name']))
                fname = '__internal__/{}/{}'.format(data['id'], os.path.basename(data['name']))
                shutil.move(fname, data['name'])
                add_mirror(data['id'])
                print("-*- Completed {}! Cleaning up".format(data['id']))
            else:
                print("-!- Failed {}! Cleaning up".format(data['id']))
            shutil.rmtree('__internal__/{}'.format(data['id']))


# noinspection PyUnusedLocal
def listener_executor(doc_snapshot, changes, file_time):
    global doc_id_list
    new_doc_id_list = []
    for doc in doc_snapshot:
        data = doc.to_dict()
        new_doc_id_list.append(data['id'])
        if get_hostname() in data['mirrors'] and \
                os.path.join(DIRECTORY_TO_WATCH, data['name']) not in file_list and not os.path.exists(data['name']):
            delete_mirror(data['id'])
        if get_hostname() not in data['mirrors'] and (
                os.path.join(DIRECTORY_TO_WATCH, data['name']) in file_list or os.path.exists(data['name'])):
            add_mirror(data['id'])
        if os.path.join(DIRECTORY_TO_WATCH, data['name']) not in file_list:
            process_snapshot(data)
    must_update = doc_id_list is None or (len(new_doc_id_list) == 0 and len(id_list) != 0) or not all(
        x in new_doc_id_list for x in id_list)
    doc_id_list = new_doc_id_list
    if must_update:
        print('[*] Firebase update! Running FS triggers')
        on_fs_event()


def on_fs_event(first_run=False):
    global file_list
    global id_list
    global doc_id_list
    new_file_list = sorted([x.absolute().as_posix() for x in list(Path('.').rglob("*")) if x.is_file()])
    file_list = sorted(
        [x for x in new_file_list if '__internal__' not in x and '__private__' not in x and '_h5ai' not in x])
    new_id_list = [sha1(x) for x in file_list]
    if not first_run:
        if doc_id_list is None:
            print('[!] Firebase ID List is None! Waiting.')
        while doc_id_list is None:
            time.sleep(1)
        for i in range(len(file_list)):
            if new_id_list[i] not in id_list or new_id_list[i] not in doc_id_list:
                add_file(file_list[i], new_id_list[i])
        for i in range(len(id_list)):
            if id_list[i] not in new_id_list:
                delete_mirror(id_list[i])
    id_list = new_id_list


class Watcher:
    def __init__(self):
        self.observer = Observer()

    # noinspection PyBroadException
    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, '.', recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except Exception as e:
            self.observer.stop()
            print('Exiting.', e)

        self.observer.join()


class Handler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory or '__internal__' in event.src_path or \
                '__private__' in event.src_path or '_h5ai' in event.src_path:
            return None
        on_fs_event()


def sf_add_upload(data):
    global master_job_list
    if data['id'] not in master_job_list.keys():
        print('[*] Queueing upload {}'.format(data['id']))
        master_job_list[data['id']] = data


def sf_job_proc():
    global master_job_list
    if len(master_job_list.keys()) == 0:
        return
    current_job_list = master_job_list
    jobs_done = []
    for data in current_job_list.values():
        ret = os.system("wget -P {} {}".format('__internal__/{}/ > /dev/null 2>&1'.format(data['id']), get_url(data)))
        if ret == 0:
            if os.path.dirname(data['name']).strip() != '':
                make_checked_dirs(os.path.dirname(data['name']))
            fname = '__internal__/{}/{}'.format(data['id'], os.path.basename(data['name']))
            shutil.move(fname, data['name'])
            jobs_done.append(data)
            print('-*- Downloaded {}. Starting SF upload'.format(data['id']))
            ret_rsync = os.system(
                "rsync -R -e \"ssh -o StrictHostKeyChecking=no -i {}\" {} {}".format(SF_KEY, data['name'], SSH_USER) +
                "@frs.sourceforge.net:/home/frs/project/posp/")
            if ret_rsync == 0:
                set_sfupload(data['id'])
                print('-*- Uploaded {} to SF! Cleaning up'.format(data['id']))
            else:
                print('-!- Upload {} to SF failed! Cleaning up'.format(data['id']))
            if os.path.dirname(data['name']).strip() != '':
                shutil.rmtree(os.path.dirname(data['name']))
            else:
                os.remove(data['name'])
        else:
            print('-!- Download failed for {}! Cleaning and continuing'.format(data['id']))
        shutil.rmtree('__internal__/{}'.format(data['id']))
    for data in jobs_done:
        del master_job_list[data['id']]


# noinspection PyUnusedLocal
def listener_master(doc_snapshot, changes, file_time):
    for doc in doc_snapshot:
        data = doc.to_dict()
        if not data['sfupload']:
            sf_add_upload(data)


if __name__ == '__main__':
    if MASTER:
        os.chdir(MASTER_WORKING_DIR)
        make_checked_dirs('__internal__')
        retrieve_secrets()
        db = firestore.Client()
        print('[*] Starting Cloud Firestore listener')
        db.collection(u'Files').on_snapshot(listener_master)
        while True:
            sf_job_proc()
            time.sleep(5)
        pass
    else:
        os.chdir(DIRECTORY_TO_WATCH)
        make_checked_dirs('__internal__')
        make_checked_dirs('__private__')
        retrieve_secrets()
        db = firestore.Client()
        print('[*] Creating data lists')
        on_fs_event(first_run=True)
        print('[.] Done!')
        print('[-] file_list:', file_list)
        print('[-] id_list:', id_list)
        print('[*] Starting Cloud Firestore listener')
        db.collection(u'Files').on_snapshot(listener_executor)
        print('[*] Waiting to finish populating IDs from Cloud Firestore')
        while doc_id_list is None:
            time.sleep(1)
        print('[.] Done!')
        print('[*] Starting FS watcher')
        w = Watcher()
        w.run()
