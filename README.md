# Introduction

Basic context manager for using consul as a distributed lock for mutual exclusion. Uses ephemeral keys
and session heartbeating to maintain the lock while the context block is running.

# Usage

```
import consul.lock

locker = consul.lock.ConsulLock(consul_host='http://localhost',
        consul_port=8500, lock_key='key', lock_value='value')

with locker:
  import time
  print 'Doing some stuff'
  time.sleep(5)
  print 'Finished doing some stuff'
```
