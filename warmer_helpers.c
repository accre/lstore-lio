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

#define _log_module_index 207

#include <assert.h>
#include <leveldb/c.h>
#include "assert_result.h"
#include <apr_pools.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "ds_ibp_priv.h"
#include "ibp.h"
#include "string_token.h"
#include "warmer.h"

//*************************************************************************
// warm_put_inode - Puts an entry in the inode DB
//*************************************************************************

int warm_put_inode(leveldb_t *db, ex_id_t inode, int state, int nfailed, char *name)
{
    leveldb_writeoptions_t *wopt;
    db_inode_entry_t *r;
    char *errstr = NULL;
    int n;

    n = strlen(name) + sizeof(db_inode_entry_t);
    r = malloc(n);
    r->state = state;
    r->nfailed = nfailed;
    strcpy(r->name, name);

    wopt = leveldb_writeoptions_create();
    leveldb_put(db, wopt, (const char *)&inode, sizeof(ex_id_t), (const char *)r, n, &errstr);
    leveldb_writeoptions_destroy(wopt);

    if (errstr != NULL) {
        log_printf(0, "ERROR: %s\n", errstr);
        free(errstr);
    }

    free(r);

    return((errstr == NULL) ? 0 : 1);
}

//*************************************************************************
// warm_put_rid - Puts an entry in the RID DB
//*************************************************************************

int warm_put_rid(leveldb_t *db, char *rid, ex_id_t inode, int state)
{
    leveldb_writeoptions_t *wopt;
    db_rid_entry_t r;
    char *errstr = NULL;
    char *key;
    int n;

    n = strlen(rid) + 1 + 20 + 1;
    type_malloc(key, char, n);
    n = sprintf(key, "%s|" XIDT, rid, inode) + 1;

    r.inode = inode;
    r.state = state;

    wopt = leveldb_writeoptions_create();
    leveldb_put(db, wopt, key, n, (const char *)&r, sizeof(db_rid_entry_t), &errstr);
    leveldb_writeoptions_destroy(wopt);

    free(key);

    if (errstr != NULL) {
        log_printf(0, "ERROR: %s\n", errstr);
        free(errstr);
    }

    return((errstr == NULL) ? 0 : 1);
}

//*************************************************************************
// create_a_db - Creates  a LevelDB database using the given path
//*************************************************************************

leveldb_t *create_a_db(char *db_path)
{
    leveldb_t *db;
    leveldb_options_t *opt_exists, *opt_create, *opt_none;
    char *errstr = NULL;

    opt_exists = leveldb_options_create(); leveldb_options_set_error_if_exists(opt_exists, 1);
    opt_create = leveldb_options_create(); leveldb_options_set_create_if_missing(opt_create, 1);
    opt_none = leveldb_options_create();

    db = leveldb_open(opt_exists, db_path, &errstr);
    if (errstr != NULL) {  //** It already exists so need to remove it first
        free(errstr);
        errstr = NULL;

        //** Remove it
        leveldb_destroy_db(opt_none, db_path, &errstr);
        if (errstr != NULL) {  //** Got an error so just kick out
            fprintf(stderr, "ERROR: Failed removing %s for fresh DB. ERROR:%s\n", db_path, errstr);
            exit(1);
        }

        //** Try opening it again
        db = leveldb_open(opt_create, db_path, &errstr);
        if (errstr != NULL) {  //** An ERror occured
            fprintf(stderr, "ERROR: Failed creating %s. ERROR:%s\n", db_path, errstr);
            exit(1);
        }
    }

    leveldb_options_destroy(opt_none);
    leveldb_options_destroy(opt_exists);
    leveldb_options_destroy(opt_create);

    return(db);
}

//*************************************************************************
//  create_warm_db - Creates the DBs using the given base directory
//*************************************************************************

void create_warm_db(char *db_base, leveldb_t **inode_db, leveldb_t **rid_db)
{
    char *db_path;

    type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); *inode_db = create_a_db(db_path);
    sprintf(db_path, "%s/rid", db_base); *rid_db = create_a_db(db_path);
    free(db_path);
}

//*************************************************************************
// open_a_db - Opens a LevelDB database
//*************************************************************************

leveldb_t *open_a_db(char *db_path)
{
    leveldb_t *db;
    leveldb_options_t *opt;
    char *errstr = NULL;

    opt = leveldb_options_create();

    db = leveldb_open(opt, db_path, &errstr);
    if (errstr != NULL) {  //** It already exists so need to remove it first
        fprintf(stderr, "ERROR: Failed creating %s. ERROR:%s\n", db_path, errstr);
        free(errstr);
        errstr = NULL;
    }

    leveldb_options_destroy(opt);

    return(db);
}

//*************************************************************************
//  open_warm_db - Opens the warmer DBs
//*************************************************************************

int open_warm_db(char *db_base, leveldb_t **inode_db, leveldb_t **rid_db)
{
    char *db_path;

    type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); *inode_db = open_a_db(db_path);
    if (inode_db == NULL) { free(db_path); return(1); }

    sprintf(db_path, "%s/rid", db_base); *rid_db = open_a_db(db_path);
    if (rid_db == NULL) { free(db_path); return(2); }

    free(db_path);
    return(0);
}

//*************************************************************************
//  close_warm_db - Closes the DBs
//*************************************************************************

void close_warm_db(leveldb_t *inode, leveldb_t *rid)
{
    leveldb_close(inode);
    leveldb_close(rid);
}

