// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	
  pthread_mutex_lock(&mutex);
  if (manager.find(lid) == manager.end()) {
    manager.insert(std::pair<lock_protocol::lockid_t, pthread_cond_t>(lid, PTHREAD_COND_INITIALIZER));
    recorder.insert(std::pair<lock_protocol::lockid_t, bool>(lid, false));
  } else {
    while (!recorder[lid])
      pthread_cond_wait(&(manager.find(lid)->second), &mutex);
    recorder[lid] = false;
  }

  pthread_mutex_unlock(&mutex);
  r = ret;

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	
  pthread_mutex_lock(&mutex);
  recorder[lid] = true;
  pthread_cond_signal(&(manager.find(lid)->second));
  pthread_mutex_unlock(&mutex);
  r = ret;

  return ret;
}
