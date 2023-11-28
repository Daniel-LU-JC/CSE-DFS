#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

blockid_t
block_manager::alloc_block()
{
  int min = IBLOCK(INODE_NUM, sb.nblocks) + 1;
  for (int i=min; i<BLOCK_NUM; ++i) {
    if (using_blocks[i] == 0) {  // allocate the corresponding block
      using_blocks[i] = 1;
      return i;
    }
  }

  printf("ERROR: no more blocks\n");
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  using_blocks[id] = 0;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  for (uint32_t i=0; i<IBLOCK(INODE_NUM, sb.nblocks); i++)
    using_blocks[i] = 1;
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  inumber = 0;
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  // static int inum = 0;
  for (int i=0; i<INODE_NUM; ++i) {
    inumber = (inumber + 1) % INODE_NUM;
    inode_t * ino = get_inode(inumber);
    if (ino == NULL) {
      ino = (inode_t *) malloc(sizeof(inode_t));
      bzero(ino, sizeof(inode_t));
      ino->type = type;
      ino->atime = (unsigned int) time(NULL);
      ino->mtime = (unsigned int) time(NULL);
      ino->ctime = (unsigned int) time(NULL);
      put_inode(inumber, ino);
      free(ino);
      break;
    }
    free(ino);  // current inode has already been occupied
  }
  return inumber;
}

void
inode_manager::free_inode(uint32_t inum)
{
  inode_t * ino = get_inode(inum);
  if (ino) {
    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
  }
}

struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  char buf[BLOCK_SIZE];
  if (inum < 0 || inum >= INODE_NUM) {
    printf("ERROR: inum out of range\n");
    return NULL;
  }
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  struct inode *ino_disk = (struct inode *) buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("ERROR: inode doesn't exist\n");
    return NULL;
  }
  ino = (struct inode *) malloc(sizeof(struct inode));
  * ino = * ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

blockid_t
inode_manager::get_nth_blockid(inode_t *ino, uint32_t num) {
  blockid_t ret = 0;
  if (num < NDIRECT)
    ret = ino->blocks[num];
  else {
    char buf[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], buf);
    ret = ((blockid_t *) buf)[num - NDIRECT];
  }
  return ret;
}

void
inode_manager::alloc_nth_block(inode_t *ino, uint32_t num) {
  if (num < NDIRECT) {
    ino->blocks[num] = bm->alloc_block();
  } else {
    if (!ino->blocks[NDIRECT]) {
      ino->blocks[NDIRECT] = bm->alloc_block();
    }
    char tmp[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], tmp);
    ((blockid_t *)tmp)[num - NDIRECT] = bm->alloc_block();
    bm->write_block(ino->blocks[NDIRECT], tmp);
  }
}

void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  inode_t * ino = get_inode(inum);
  char buf[BLOCK_SIZE];
  int i;
  if (ino) {
    *size = ino->size;
    *buf_out = (char *) malloc(ino->size);
    int block_num = ino->size / BLOCK_SIZE;
    int remain_num = ino->size % BLOCK_SIZE;
    for (i=0; i<block_num; ++i) {
      bm->read_block(get_nth_blockid(ino, i), buf);
      memcpy(*buf_out + i*BLOCK_SIZE, buf, BLOCK_SIZE);
    }
    if (remain_num != 0) {
      bm->read_block(get_nth_blockid(ino, i), buf);
      memcpy(*buf_out + i*BLOCK_SIZE, buf, remain_num);
    }
    free(ino);
  }
}

void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  inode_t * ino = get_inode(inum);
  int i;
  if (ino) {
    int old_num = 0, new_num = 0;
    if (ino->size != 0)
      old_num = (ino->size - 1) / BLOCK_SIZE + 1;
    if (size != 0)
      new_num = (size - 1) / BLOCK_SIZE + 1;
    if (old_num < new_num) {
      for (int j=old_num; j<new_num; ++j)
        alloc_nth_block(ino, j);
    } else if (old_num > new_num) {
      for (int j=new_num; j<old_num; ++j)
        bm->free_block(get_nth_blockid(ino, j));
    }
    int block_num = size / BLOCK_SIZE;
    int remain_num = size % BLOCK_SIZE;
    for (i=0; i<block_num; ++i)
      bm->write_block(get_nth_blockid(ino, i), buf + i*BLOCK_SIZE);
    if (remain_num != 0) {
      char tmp[BLOCK_SIZE];
      memcpy(tmp, buf + i*BLOCK_SIZE, remain_num);
      bm->write_block(get_nth_blockid(ino, i), tmp);
    }
    ino->size = size;
    ino->atime = (unsigned int) time(NULL);
    ino->mtime = (unsigned int) time(NULL);
    ino->ctime = (unsigned int) time(NULL);
    put_inode(inum, ino);
    free(ino);
  }
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  inode_t * ino = get_inode(inum);
  if (ino == NULL) return;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.type = ino->type;
  a.size = ino->size;
  free(ino);
}

void
inode_manager::remove_file(uint32_t inum)
{
  inode_t * ino = get_inode(inum);
  int block_num = 0;
  if (ino->size != 0)
    block_num = (ino->size - 1) / BLOCK_SIZE + 1;
  for (int i=0; i<block_num; ++i)
    bm->free_block(get_nth_blockid(ino, i));
  if (block_num > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);

  free_inode(inum);
  free(ino);
}
