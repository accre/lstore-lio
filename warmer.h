/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

//***********************************************************************
// Warmer helper definitions
//***********************************************************************


#ifndef _WARMER_H_
#define _WARMER_H_

#ifdef __cplusplus
extern "C" {
#endif

#define WFE_SUCCESS   1
#define WFE_FAIL      2
#define WFE_WRITE_ERR 4

typedef struct {
    int  state;
    int  nfailed;
    char name[1];
} __attribute__((packed)) db_inode_entry_t;

typedef struct {
    ex_id_t inode;
    int  state;
} __attribute__((packed)) db_rid_entry_t;

int warm_put_inode(leveldb_t *db, ex_id_t inode, int status, int nfailed, char *name);
int warm_put_rid(leveldb_t *db, char *rid, ex_id_t inode, int state);
void create_warm_db(char *db_base, leveldb_t **inode, leveldb_t **rid);
int open_warm_db(char *db_base, leveldb_t **inode_db, leveldb_t **rid_db);
void close_warm_db(leveldb_t *inode, leveldb_t *rid);

#ifdef __cplusplus
}
#endif

#endif