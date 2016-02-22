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

#define _log_module_index 197

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"

char **argv_list = NULL;
int start_index = -1;
int final_index = -1;
int current_index = -1;
int from_stdin = 0;

//*************************************************************************
//  next_path - Returns the next path from either argv or stdin
//*************************************************************************

char *next_path()
{
    char *p, *p2;

    if (from_stdin == 0) {
        if (current_index == -1) current_index = start_index;
        if (current_index > final_index) return(NULL);

        p = strdup(argv_list[current_index]);
        current_index++;
    } else {
        type_malloc(p2, char, 8192);
        p = fgets(p2, 8192, stdin);
        if (p) {
            p2[strlen(p)-1] = 0;  //** Truncate the \n
        } else {
            free(p2);	
        }
    }

    log_printf(5, "from_stdin=%d path=%s\n", from_stdin, p);
    return(p);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, err, rg_mode, start_option, loop;
    opque_t *q;
    op_generic_t *gop;
    char *path;
    op_status_t status;
    os_regex_table_t **rpath;
    lio_path_tuple_t *flist, tuple;
    char **path_list;
    os_regex_table_t *rp_single, *ro_single;
    int recurse_depth = 0;
    int obj_types = OS_OBJECT_ANY;

    if (argc < 2) {
        printf("\n");
        printf("lio_rm LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [LIO_PATH_OPTIONS | -]\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d.\n", recurse_depth);
        printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
        printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
        return(1);
    }

    lio_init(&argc, &argv);
    argv_list = argv;


    //*** Parse the args
    rp_single = ro_single = NULL;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
            i++;
            obj_types = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-") == 0) { //** Take files from stdin
            i++;
            from_stdin = 1;
        }

    } while ((start_option < i) && (i<argc));
    start_index = i;
    final_index = argc - 1;

    if (rg_mode == 1) {  //** Got an explicit R/G path set
        err = gop_sync_exec(gop_lio_remove_regex_object(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, recurse_depth, lio_parallel_task_count));
        if (err != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "Error occured with remove\n");

        if (rp_single != NULL) os_regex_table_destroy(rp_single);
        if (ro_single != NULL) os_regex_table_destroy(ro_single);
        lio_path_release(&tuple);
    }

    //** Check if we have more things to remove
    if ((i>=argc) && (from_stdin == 0)) {
        if (rp_single == NULL) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }

        goto finished;
    }


    //** Spawn the tasks
    type_malloc(flist, lio_path_tuple_t, lio_parallel_task_count);
    type_malloc(rpath, os_regex_table_t *, lio_parallel_task_count);
    type_malloc(path_list, char *, lio_parallel_task_count);

    q = new_opque();
    opque_start_execution(q);
    i = 0;
    loop = 0;
    while ((path = next_path()) != NULL) {
        loop++;
        path_list[i] = path;
        flist[i] = lio_path_resolve(lio_gc->auto_translate, path_list[i]);
        rpath[i] = os_path_glob2regex(flist[i].path);
        gop = gop_lio_remove_regex_object(flist[i].lc, flist[i].creds, rpath[i], NULL, obj_types, recurse_depth, lio_parallel_task_count);
        gop_set_myid(gop, i);
        log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].path);
        opque_add(q, gop);
        i++;

        if (loop >= lio_parallel_task_count) {
            gop = opque_waitany(q);
            i = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "ERROR with %s\n", path_list[i]);
            lio_path_release(&(flist[i]));
            os_regex_table_destroy(rpath[i]);
            free(path_list[i]);
            gop_free(gop, OP_DESTROY);
        }
    }

    err = opque_waitall(q);
    while ((gop = opque_waitany(q)) != NULL) {
        i = gop_get_myid(gop);
        status = gop_get_status(gop);
        if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "ERROR with %s\n", path_list[i]);
        lio_path_release(&(flist[i]));
        os_regex_table_destroy(rpath[i]);
        free(path_list[i]);
        gop_free(gop, OP_DESTROY);
    }

    opque_free(q, OP_DESTROY);


    free(flist);
    free(rpath);
    free(path_list);

finished:
    lio_shutdown();

    return(0);
}


