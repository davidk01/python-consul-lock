import requests
import threading
import json
import time


class ConsulLock(object):


    class LockParameterException(Exception):
        pass


    class LockAcquireException(Exception):
        pass


    # The session operation endpoints
    _session_endpoints = {'create': 'v1/session/create', 'destroy': 'v1/session/destroy', 
            'renew': 'v1/session/renew', 'info': 'v1/session/info'}


    # K/V operation endpoints
    _kv_endpoints = {'kv': 'v1/kv', 'txn': 'v1/txn'}


    # What we send to Consul to create the session ID
    _session_create_payload = {"Name": "lock", "Behavior": "delete", "TTL": "15s"}


    # Duration in seconds we sleep between session heartbeat/renewal requests
    _sleep_duration = 2


    # Duration in seconds we wait for the heartbeat thread to stop
    _join_timeout = 2 * _sleep_duration
    

    # Timeout for requests
    _requests_timeout = 1


    def __init__(self, consul_host='http://localhost', consul_port=8500, 
            lock_key=None, lock_value=None):
        self._consul_base_path = "{}:{}".format(consul_host, consul_port)
        self._session_id = None
        self._semaphore = threading.Semaphore()
        self._heartbeating = False
        self._heartbeat_thread = None
        if lock_key is None or lock_value is None:
            raise self.LockParameterException('lock_key and lock_value are required parameters')
        self._lock_key = lock_key
        self._lock_value = lock_value


    def _endpoint(self, *segments):
        """
        Generate the full HTTP endpoint with the given path segments
        """
        return '/'.join([self._consul_base_path] + list(segments))


    def _acquire_session(self):
        """
        Grab a session ID if there isn't one already or if there is one and it expired. Note
        that this method is not thread safe if used without a semaphore
        """
        if self._session_id is None:
            endpoint = self._endpoint(self._session_endpoints['create'])
            response = requests.put(endpoint, 
                    data=json.dumps(self._session_create_payload),
                    timeout=self._requests_timeout)
            self._session_id = response.json()['ID']
            self._start_heartbeat()
            return self._session_id
        else:
            endpoint = self._endpoint(self._session_endpoints['info'], self._session_id)
            sessions = requests.get(endpoint, timeout=self._requests_timeout)
            if sessions.status_code != 200:
                self._session_id = None
                return self._acquire_session()
            for session in sessions.json():
                if session['ID'] == self._session_id:
                    return self._session_id
            self._session_id = None
            return self._acquire_session()


    def session_is_active(self):
        """
        Any long running task should periodically verify the session is still active
        by calling this method. If for whatever reason the session is no longer active
        then the task should abort or try to acquire the lock again
        """
        if self._session_id is None:
            return False
        endpoint = self._endpoint(self._session_endpoints['info'], self._session_id)
        sessions = requests.get(endpoint, timeout=self._requests_timeout)
        if sessions.status_code != 200:
            return False
        for session in sessions.json():
            if session['ID'] == self._session_id:
                return True
        return False


    def _release_session(self):
        """
        Delete/release the session. Similar to above it is not thread safe if used without
        a semaphore
        """
        endpoint = self._endpoint(self._session_endpoints['destroy'], self._session_id)
        response = requests.put(endpoint, timeout=self._requests_timeout)
        self._session_id = None
        self._stop_heartbeat()
        return self._session_id


    def _heartbeat(self):
        """
        Make sure to keep the session alive because losing the session means we lose
        any locked K/V pairs in Consul which kinda defeats the purpose of locking
        things
        """
        endpoint = self._endpoint(self._session_endpoints['renew'], self._session_id)
        while self._heartbeating:
            response = requests.put(endpoint, timeout=self._requests_timeout)
            time.sleep(self._sleep_duration)


    def _start_heartbeat(self):
        """
        Once we have grabbed the session and successfully acquired a key to use as
        a lock we start the heartbeat to keep renewing the session to maintain the
        the key we have acquired for this session as a lock. Not thread safe if used
        without acquiring a semaphore
        """
        self._stop_heartbeat()
        self._heartbeating = True
        self._heartbeat_thread = threading.Thread(target=self._heartbeat)
        self._heartbeat_thread.start()


    def _stop_heartbeat(self):
        """
        Stop the heartbeating thread. Presumably we have released the session at this point and
        no longer need to renew it. Not thread safe if used without acquiring a semaphore.
        """
        self._heartbeating = False
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(self._join_timeout)
            self._heartbeat_thread = None


    def _lock(self):
        """
        At this point we should have a session and heartbeating working so we can
        acquire a key to act as the lock for the session. If we fail to acquire the key
        then throw an exception.
        """
        lock_failure_response = 'false'
        key_success_status_code = 200
        # Create the lock
        endpoint = self._endpoint(self._kv_endpoints['kv'], self._lock_key)
        params = {'acquire': self._session_id}
        acquire_response = requests.put(endpoint, params=params, 
                data=self._lock_value, timeout=self._requests_timeout)
        # We could not get the key with our session ID so clean up and raise exception
        if acquire_response.content == lock_failure_response:
            self._release_session()
            raise self.LockAcquireException('Failed to acquire lock')
        # Verify we actually got the lock by querying the session ID for the key
        response = requests.get(endpoint, timeout=self._requests_timeout)
        if response.status_code == key_success_status_code:
            response_list = response.json()
            for lock in response_list:
                if lock['Session'] != self._session_id:
                    self._release_session()
                    raise self.LockAcquireException('Failed to acquire lock')
        else:
            self._release_session()
            raise self.LockAcquireException('Could not get the lock key')
        return self


    def __enter__(self):
        """
        Performs the necessary actions to acquire a Consul lock and start the session heartbeat
        """
        with self._semaphore:
            self._acquire_session()
            self._lock()


    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        """
        Cleans up any state we had set up in Consul for acquiring the Consul lock
        """
        with self._semaphore:
            self._release_session()
