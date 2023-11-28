#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

int
chfs_client::symlink(inum parent, const char * name, inum & ino_out, const char * link) {
    int r = OK;

    // step 01: check if the link file name has existed
    bool found = false;
    inum tmp;
    lookup(parent, name, found, tmp);
    if (found) return EXIST;

    // step 02: create the link file
    ec->create(extent_protocol::T_SYMLINK, ino_out);
    ec->put(ino_out, std::string(link));

    // step 03: add an entry to the parent directory
    std::string buf;
    ec->get(parent, buf);
    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    ec->put(parent, buf);

    return r;
}

int
chfs_client::readlink(inum ino, std::string &data) {
    ec->get(ino, data);  // get the content of link file
    return OK;
}

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) return false;
    if (a.type == extent_protocol::T_DIR) return true;
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

int
chfs_client::setattr(inum ino, size_t size)
{
    std::string buf;
    ec->get(ino, buf);
    buf.resize(size);
    ec->put(ino, buf);
    return OK;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    bool found = false;  // check if the file has already existed
    inum tmp;
    lookup(parent, name, found, tmp);
    if (found) return EXIST;

    ec->create(extent_protocol::T_FILE, ino_out);  // allocate an inode for current file
    std::string buf;
    ec->get(parent, buf);
    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    ec->put(parent, buf);  // append the new entry to the parent folder

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    inum tmp;
    bool found = false;
    lookup(parent, name, found, tmp);  // check if the directory name has existed
    if (found) return EXIST;

    ec->create(extent_protocol::T_DIR, ino_out);  // create an inode for the directory
    std::string buf;
    ec->get(parent, buf);  // get the original content of parent and add an entry
    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    ec->put(parent, buf);

    return r;
}

/*
 * parent: the inum of the current directory
 * name: the name of the file we intend to search
 * found: the return value of checking the directory
 * ino_out: the return value of the inum of the target file
 */
int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> file_list;
    readdir(parent, file_list);  // find all of the files in the directory
    if (file_list.empty()) {
        found = false;
        return r;
    } else {
        for (std::list<dirent>::iterator it = file_list.begin(); it != file_list.end(); it++) {
            if (!it->name.compare(name)) {  // find the target file
                found = true;
                ino_out = it->inum;
                return r;
            }
        }
    }
    found = false;  // there is no file with 'name' in the current directory

    return r;
}

/*
 * parse the directory content and get the list of files in directory
 * format: "name:inum/"
 * dir: the inode number of the current directory
 * list: reference to the list of files in the directory
 */
int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    std::string buf;
    ec->get(dir, buf);  // get the content of the directory

    int name_start = 0;
    int name_end = buf.find(':');  // get the initial index of char
    while (name_end != std::string::npos) {
        std::string file_name = buf.substr(name_start, name_end - name_start);
        int inum_start = name_end + 1;
        int inum_end = buf.find('/', inum_start);
        std::string inum_val = buf.substr(inum_start, inum_end - inum_start);
        struct dirent entry;
        entry.name = file_name;
        entry.inum = n2i(inum_val);  // get the entry of one file in the directory
        list.push_back(entry);
        name_start = inum_end + 1;
        name_end = buf.find(':', name_start);
    }

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    std::string buf;
    ec->get(ino, buf);  // read the whole file
    if (off > buf.size()) {  // offset is greater than the file size
        data = "";
        return r;
    } else if (off + size > buf.size()) {  // 'off + size' overflows
        data = buf.substr(off);
        return r;
    } else {
        data = buf.substr(off, size);
    }

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    std::string buf;
    ec->get(ino, buf);  // get the original content in the file
    if (size + off > buf.size())
        buf.resize(off + size);
    for (int i=off; i<off+size; i++) {
        buf[i] = data[i-off];
    }
    bytes_written = size;
    ec->put(ino, buf);

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    inum tmp;
    bool found = false;
    lookup(parent, name, found, tmp);
    ec->remove(tmp);

    std::string buf;
    ec->get(parent, buf);  // remove the entry from the parent file
    int start = buf.find(name);
    int end = buf.find('/', start);
    buf.erase(start, end - start + 1);
    ec->put(parent, buf);

    return r;
}
