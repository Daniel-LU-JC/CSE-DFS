#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    res = std::make_shared<result>();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) { }
chfs_command_raft::~chfs_command_raft() { }

int chfs_command_raft::size() const {
    return sizeof(chfs_command_raft::command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t)
         + sizeof(int) + buf.size() + 1;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    * ((command_type *) buf_out) = cmd_tp;
    * ((uint32_t *)(buf_out + sizeof(command_type))) = type;
    * ((extent_protocol::extentid_t *)(buf_out + sizeof(command_type) + sizeof(uint32_t))) = id;
    * ((int *)(buf_out + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t))) = buf.size();
    strcpy(buf_out + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int), buf.data());
    // buf_out[this->size()] = '\0';
    // memcpy(buf_out + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int), buf.data(), buf.size());
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    cmd_tp = *((command_type *) buf_in);
    type = *((uint32_t *) (buf_in + sizeof(command_type)));
    id = *((extent_protocol::extentid_t *) (buf_in + sizeof(command_type) + sizeof(uint32_t)));
    int buf_size = *((int *) (buf_in + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t)));
    // buf = std::string(buf_in + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int), buf_size);
    buf.assign(buf_in + sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int));
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    m << (int) cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    int type;
    u >> type >> cmd.type >> cmd.id >> cmd.buf;
    cmd.cmd_tp = (chfs_command_raft::command_type) type;
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
