#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE,
        CMD_INVALID
    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    uint32_t file_type = 0;
    unsigned long long inum_id = 0;
    std::string content = "";
    unsigned long long content_size = 0;

    chfs_command(cmd_type t, txid_t i) {
        type = t;
        id = i;
    }
};

template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    void append_log(const command& log);
    void snapshot(unsigned char blocks[]);

    // restore data from solid binary file
    void restore_logdata();
    void restore_snapshot(unsigned char blocks[]);

    // restored log data
    std::vector<chfs_command> log_entries;

    // restore checkpoint data
    std::vector<chfs_command> checkpoint_entries;

    // calculate the current size of logdata.bin
    int logdata_size();

    // do checkpoint and restore data from checkpoint.bin
    void do_checkpoint();
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
};

template<typename command>
persister<command>::persister(const std::string& dir){
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {}

template<typename command>
void persister<command>::append_log(const command& log) {
    std::fstream foi_log(file_path_logfile, std::ios::app|std::ios::binary);
    chfs_command cmd = log;
    switch (cmd.type) {
        case chfs_command::CMD_BEGIN: {
            foi_log.write((char *) & cmd.id, sizeof(chfs_command::txid_t));
            foi_log.write((char *) & cmd.type, sizeof(chfs_command::cmd_type));
            break;
        }
        case chfs_command::CMD_COMMIT: {
            foi_log.write((char *) & cmd.id, sizeof(chfs_command::txid_t));
            foi_log.write((char *) & cmd.type, sizeof(chfs_command::cmd_type));
            break;
        }
        case chfs_command::CMD_CREATE: {
            foi_log.write((char *) & cmd.id, sizeof(chfs_command::txid_t));
            foi_log.write((char *) & cmd.type, sizeof(chfs_command::cmd_type));
            foi_log.write((char *) & cmd.file_type, sizeof(uint32_t));
            foi_log.write((char *) & cmd.inum_id, sizeof(unsigned long long));
            break;
        }
        case chfs_command::CMD_PUT: {
            foi_log.write((char *) & cmd.id, sizeof(chfs_command::txid_t));
            foi_log.write((char *) & cmd.type, sizeof(chfs_command::cmd_type));
            foi_log.write((char *) & cmd.inum_id, sizeof(unsigned long long));
            foi_log.write((char *) & cmd.content_size, sizeof(unsigned long long));
            foi_log.write(cmd.content.c_str(), cmd.content_size);
            break;
        }
        case chfs_command::CMD_REMOVE: {
            foi_log.write((char *) & cmd.id, sizeof(chfs_command::txid_t));
            foi_log.write((char *) & cmd.type, sizeof(chfs_command::cmd_type));
            foi_log.write((char *) & cmd.inum_id, sizeof(unsigned long long));
            break;
        }
    }  // append an entry into logdata.bin

    // push the instruction into vector to synchronize
    log_entries.push_back(cmd);

    foi_log.close();
}

// CMD_BEGIN: txid_t cmd_type
// CMD_COMMIT: txid_t cmd_type
// CMD_CREATE: txid_t cmd_type file_type inum_id
// CMD_PUT: txid_t cmd_type inum_id size content
// CMD_REMOVE: txid_t cmd_type inum_id
template<typename command>
void persister<command>::restore_logdata() {
    std::fstream foi_log(file_path_logfile, std::ios::in|std::ios::binary);
    if (!foi_log) return;
    while (!foi_log.eof()) {
        chfs_command cmd(chfs_command::CMD_INVALID, 0);  // parse entries stored inside the log file
        // step 01: read txid_t and cmd_type
        foi_log.read((char *) & cmd.id, sizeof(chfs_command::txid_t));
        foi_log.read((char *) & cmd.type, sizeof(chfs_command::cmd_type));
        switch (cmd.type) {
            case chfs_command::CMD_BEGIN: break;
            case chfs_command::CMD_COMMIT: break;
            case chfs_command::CMD_CREATE: {
                foi_log.read((char *) & cmd.file_type, sizeof(uint32_t));
                foi_log.read((char *) & cmd.inum_id, sizeof(unsigned long long));
                break;
            }
            case chfs_command::CMD_PUT: {
                foi_log.read((char *) & cmd.inum_id, sizeof(unsigned long long));
                foi_log.read((char *) & cmd.content_size, sizeof(unsigned long long));
                char * buffer = (char *) malloc(cmd.content_size);
                foi_log.read(buffer, cmd.content_size);
                cmd.content.resize(cmd.content_size);  // resize to the length of the string
                for (int i = 0; i < cmd.content_size; ++i)
                    cmd.content[i] = buffer[i];
                free(buffer);
                break;
            }
            case chfs_command::CMD_REMOVE: {
                foi_log.read((char *) & cmd.inum_id, sizeof(unsigned long long));
                break;
            }
        }
        if (cmd.type != chfs_command::CMD_INVALID)
            log_entries.push_back(cmd);
    }  // recover the operation entries into the vector
    foi_log.close();
};

template<typename command>
int persister<command>::logdata_size() {
    std::fstream cal_bytes(file_path_logfile, std::ios::in|std::ios::binary);
    int head = 0, tail = 0;
    head = cal_bytes.tellg();
    cal_bytes.seekg(0, std::ios::end);
    tail = cal_bytes.tellg();
    cal_bytes.close();
    return (tail - head);  // return the size in bytes of logdata.bin
}

template<typename command>
void persister<command>::snapshot(unsigned char blocks[]) {
    // take a snapshot of the system and store the data into checkpoint.bin
    std::fstream snapshot(file_path_checkpoint, std::ios::out|std::ios::binary);
    snapshot.write((char *) blocks, 1024 * 1024 * 16);
    snapshot.close();
}

template<typename command>
void persister<command>::restore_snapshot(unsigned char blocks[]) {
    // restore the blocks data from checkpoint.bin
    std::fstream snapshot(file_path_checkpoint, std::ios::in|std::ios::binary);
    snapshot.read((char *) blocks, 1024 * 1024 * 16);
    snapshot.close();
};

template<typename command>
void persister<command>::do_checkpoint() {
    // step 01: empty the logdata.bin file
    std::fstream empty(file_path_logfile, std::ios::out|std::ios::trunc);
    empty.close();

    // step 02: check the executable instructions stored in vector
    std::vector<chfs_command::txid_t> executable;
    std::vector<chfs_command>::iterator iter;
    for (iter = log_entries.begin(); iter != log_entries.end(); iter++) {
        if ((*iter).type == chfs_command::CMD_COMMIT)
            executable.push_back((*iter).id);
    }

    // step 03: write the entries to checkpoint.bin
    std::fstream foi_log(file_path_checkpoint, std::ios::app|std::ios::binary);
    std::vector<chfs_command::txid_t>::iterator exe = executable.begin();
    for (iter = log_entries.begin(); iter != log_entries.end(); iter++) {
        if ((*iter).id != (*exe)) continue;
        switch ((*iter).type) {
            case chfs_command::CMD_BEGIN: break;
            case chfs_command::CMD_COMMIT: {
                exe++;
                break;
            }
            case chfs_command::CMD_CREATE: {
                foi_log.write((char *) & (*iter).id, sizeof(chfs_command::txid_t));
                foi_log.write((char *) & (*iter).type, sizeof(chfs_command::cmd_type));
                foi_log.write((char *) & (*iter).file_type, sizeof(uint32_t));
                foi_log.write((char *) & (*iter).inum_id, sizeof(unsigned long long));
                break;
            }
            case chfs_command::CMD_PUT: {
                foi_log.write((char *) & (*iter).id, sizeof(chfs_command::txid_t));
                foi_log.write((char *) & (*iter).type, sizeof(chfs_command::cmd_type));
                foi_log.write((char *) & (*iter).inum_id, sizeof(unsigned long long));
                foi_log.write((char *) & (*iter).content_size, sizeof(unsigned long long));
                foi_log.write((*iter).content.c_str(), (*iter).content_size);
                break;
            }
            case chfs_command::CMD_REMOVE: {
                foi_log.write((char *) & (*iter).id, sizeof(chfs_command::txid_t));
                foi_log.write((char *) & (*iter).type, sizeof(chfs_command::cmd_type));
                foi_log.write((char *) & (*iter).inum_id, sizeof(unsigned long long));
                break;
            }
        }
    }
    foi_log.close();

    // step 04: clear the log_entries vector
    std::vector<chfs_command>().swap(log_entries);
}

template<typename command>
void persister<command>::restore_checkpoint() {
    std::fstream foi_log(file_path_checkpoint, std::ios::in|std::ios::binary);
    if (!foi_log) return;
    while (!foi_log.eof()) {
        chfs_command cmd(chfs_command::CMD_INVALID, 0);
        // step 01: read txid_t and cmd_type
        foi_log.read((char *) & cmd.id, sizeof(chfs_command::txid_t));
        foi_log.read((char *) & cmd.type, sizeof(chfs_command::cmd_type));
        switch (cmd.type) {
            case chfs_command::CMD_CREATE: {
                foi_log.read((char *) & cmd.file_type, sizeof(uint32_t));
                foi_log.read((char *) & cmd.inum_id, sizeof(unsigned long long));
                break;
            }
            case chfs_command::CMD_PUT: {
                foi_log.read((char *) & cmd.inum_id, sizeof(unsigned long long));
                foi_log.read((char *) & cmd.content_size, sizeof(unsigned long long));
                char * buffer = (char *) malloc(cmd.content_size);
                foi_log.read(buffer, cmd.content_size);
                cmd.content.resize(cmd.content_size);  // resize to the length of the string
                for (int i = 0; i < cmd.content_size; ++i)
                    cmd.content[i] = buffer[i];
                free(buffer);
                break;
            }
            case chfs_command::CMD_REMOVE: {
                foi_log.read((char *) & cmd.inum_id, sizeof(unsigned long long));
                break;
            }
        }
        if (cmd.type != chfs_command::CMD_INVALID)
            checkpoint_entries.push_back(cmd);
    }  // recover the operation entries into the vector
    foi_log.close();
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h