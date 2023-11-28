#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    return 0;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    std::unique_lock<std::mutex> lock(chfs_cmd.res->mtx);
    chfs_cmd.res->start = std::chrono::system_clock::now();
    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::CMD_NONE: break;
        case chfs_command_raft::CMD_CRT: {
            extent_protocol::extentid_t id;
            es.create(chfs_cmd.type, id);
            mtx.lock();
            chfs_cmd.res->id = id;
            mtx.unlock();
            break;
        }
        case chfs_command_raft::CMD_PUT: {
            int tmp = 0;
            es.put(chfs_cmd.id, chfs_cmd.buf, tmp);
            break;
        }
        case chfs_command_raft::CMD_GET: {
            std::string buf = "";
            es.get(chfs_cmd.id, buf);
            mtx.lock();
            chfs_cmd.res->buf = buf;
            mtx.unlock();
            break;
        }
        case chfs_command_raft::CMD_GETA: {
            extent_protocol::attr attr;
            es.getattr(chfs_cmd.id, attr);
            mtx.lock();
            chfs_cmd.res->attr = attr;
            mtx.unlock();
            break;
        }
        case chfs_command_raft::CMD_RMV: {
            int tmp = 0;
            es.remove(chfs_cmd.id, tmp);
            break;
        }
        default: break;
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}


