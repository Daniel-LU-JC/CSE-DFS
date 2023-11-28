// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

/*
 * checkpoint.bin logic:
 * 1. operate an instruction: append log entry and push back to the vector
 * 2. detect logdata.bin size: at CMD_BEGIN, calculate the total size of current transaction
 * 3. if the total size stays below the threshold, finish the entire transaction
 * 4. if the total size is gonna exceed MAX_LOG_SZ, checkpoint
 * 5. how to do checkpoint: remove all the executable instructions from logdata.bin and vector,
 *    record them in checkpoint.bin, and then start the transaction waiting in line
 * 6. how to restore data: before dealing with log entries, execute all the instructions stored
 *    in checkpoint.bin which are bound to happen
 */

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  _persister->restore_logdata();  // store the entries into vector

  // restore instructions in checkpoint.bin and execute them
  _persister->restore_checkpoint();
  std::vector<chfs_command>::iterator check;
  for (check = _persister->checkpoint_entries.begin(); check != _persister->checkpoint_entries.end(); check++) {
    switch ((*check).type) {
      case chfs_command::CMD_CREATE: {
        inode_t * ino = (inode_t *) malloc(sizeof(inode_t));
        bzero(ino, sizeof(inode_t));
        ino->type = (*check).file_type;
        ino->atime = (unsigned int) time(NULL);
        ino->mtime = (unsigned int) time(NULL);
        ino->ctime = (unsigned int) time(NULL);
        im->put_inode((*check).inum_id, ino);
        free(ino);
        break;
      }
      case chfs_command::CMD_PUT: {
        im->write_file((*check).inum_id, (*check).content.c_str(), (*check).content_size);
        break;
      }
      case chfs_command::CMD_REMOVE: {
        im->remove_file((*check).inum_id);
        break;
      }
    }
    // Don't forget to update the transaction ID
    txid_max = (*check).id;
  }

  // push all of the executable transactions into the vector
  // Also, recover the 'txid_max' to continue logging service
  std::vector<chfs_command::txid_t> executable;
  std::vector<chfs_command>::iterator iter;
  for (iter = _persister->log_entries.begin(); iter != _persister->log_entries.end(); iter++) {
    if ((*iter).type == chfs_command::CMD_BEGIN) {
      txid_max++;
      printf("TXID: updated transaction ID: %d\n", txid_max);
      if (txid_max != (*iter).id) {
        printf("ERROR: txid_max error!\n");
        exit(1);
      }  // assert that the updated txid_max equals (*iter).id
    }
    if ((*iter).type == chfs_command::CMD_COMMIT) {
      executable.push_back((*iter).id);
      printf("EXE: executable transaction ID: %d\n", (*iter).id);
    }
  }

  std::vector<chfs_command::txid_t>::iterator exe = executable.begin();
  for (iter = _persister->log_entries.begin(); iter != _persister->log_entries.end(); iter ++) {
    // if current transaction ID is not in executable, then 'continue'
    if ((*iter).id != (*exe)) continue;
    switch ((*iter).type) {
      case chfs_command::CMD_BEGIN: break;
      case chfs_command::CMD_COMMIT: {
        printf("RECOVER: recover transaction ID: %d\n", *exe);
        exe++;  // move on to the next executable transaction
        break;
      }
      case chfs_command::CMD_CREATE: {
        inode_t * ino = (inode_t *) malloc(sizeof(inode_t));
        bzero(ino, sizeof(inode_t));
        ino->type = (*iter).file_type;
        ino->atime = (unsigned int) time(NULL);
        ino->mtime = (unsigned int) time(NULL);
        ino->ctime = (unsigned int) time(NULL);
        im->put_inode((*iter).inum_id, ino);
        free(ino);
        break;
      }
      case chfs_command::CMD_PUT: {
        im->write_file((*iter).inum_id, (*iter).content.c_str(), (*iter).content_size);
        break;
      }
      case chfs_command::CMD_REMOVE: {
        im->remove_file((*iter).inum_id);
        break;
      }
    }
  }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  chfs_command cmd_2(chfs_command::CMD_CREATE, txid_max);
  cmd_2.file_type = type;
  cmd_2.inum_id = id;
  _persister->append_log(cmd_2);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  chfs_command cmd_3(chfs_command::CMD_PUT, txid_max);
  cmd_3.inum_id = id;
  cmd_3.content_size = buf.size();
  cmd_3.content = buf;
  _persister->append_log(cmd_3);
  
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  im->remove_file(id);

  chfs_command cmd_3(chfs_command::CMD_REMOVE, txid_max);
  cmd_3.inum_id = id;
  _persister->append_log(cmd_3);
 
  return extent_protocol::OK;
}
