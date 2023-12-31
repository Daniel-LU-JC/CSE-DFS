#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;

    request_vote_args() {}
    request_vote_args(int _term, int _candidate_id, int _last_log_index, int _last_log_term):
        term(_term), candidate_id(_candidate_id), last_log_index(_last_log_index), last_log_term(_last_log_term) {}
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    int term;
    bool vote_granted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry {
public:
    int term;
    command cmd;

    log_entry(): term(0) {}
    log_entry(int _term, command _cmd): term(_term), cmd(_cmd) {}
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    m << entry.term << entry.cmd;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    u >> entry.term >> entry.cmd;
    return u;
}

template <typename command>
class append_entries_args {
public:
    int term;
    int leader_id;
    int prev_log_idx;
    int prev_log_term;
    int leader_commit;
    std::vector<log_entry<command>> entries;

    append_entries_args() {}
    append_entries_args(int _term, int _leader_id, int _prev_log_idx, int _prev_log_term, int _leader_commit):
        term(_term),
        leader_id(_leader_id),
        prev_log_idx(_prev_log_idx),
        prev_log_term(_prev_log_term),
        leader_commit(_leader_commit) {}
    
    append_entries_args(int _term, int _leader_id, int _prev_log_idx, int _prev_log_term,
        int _leader_commit, std::vector<log_entry<command>> &_entries):
        term(_term),
        leader_id(_leader_id),
        prev_log_idx(_prev_log_idx),
        prev_log_term(_prev_log_term),
        leader_commit(_leader_commit),
        entries(_entries) {}
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    m << args.term << args.leader_id << args.prev_log_idx
        << args.prev_log_term << args.leader_commit << args.entries;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    u >> args.term >> args.leader_id >> args.prev_log_idx
        >> args.prev_log_term >> args.leader_commit >> args.entries;
    return u;
}

class append_entries_reply {
public:
    int term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    int term;
    int leader_id;
    int last_included_idx;
    int last_included_term;
    std::vector<char> data;

    install_snapshot_args() {}
    install_snapshot_args(int _term, int _leader_id, int _last_included_idx,
        int _last_included_term, const std::vector<char> &_data):
        term(_term), leader_id(_leader_id), last_included_idx(_last_included_idx),
        last_included_term(_last_included_term), data(_data) {}
    
    install_snapshot_args(int _term, int _leader_id, int _last_included_idx, int _last_included_term):
        term(_term), leader_id(_leader_id), last_included_idx(_last_included_idx),
        last_included_term(_last_included_term) {}
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    int term;

    install_snapshot_reply() {}
    install_snapshot_reply(int _term): term(_term) {}
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h