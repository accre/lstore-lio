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

#define _log_module_index 189

#include "type_malloc.h"
#include "lio.h"
#include "log.h"
#include "string_token.h"

//***********************************************************************
// Misc Core LIO functionality
//***********************************************************************

//***********************************************************************
//  lio_parse_path - Parses a path ofthe form: user@service:/my/path
//        The user and service are optional
//
//  Returns 1 if @: are encountered and 0 otherwise
//***********************************************************************

int lio_parse_path(char *startpath, char **user, char **service, char **path)
{
    int i, j, found, n, ptype;

    *user = *service = *path = NULL;
    n = strlen(startpath);
    ptype = 0;
    found = -1;
    for (i=0; i<n; i++) {
        if (startpath[i] == '@') {
            found = i;
            ptype = 1;
            break;
        }
    }

    if (found == -1) {
        *path = strdup(startpath);
        return(ptype);
    }

    if (found > 0) { //** Got a valid user
        *user = strndup(startpath, found);
    }

    j = found+1;
    found = -1;
    for (i=j; i<n; i++) {
        if (startpath[i] == ':') {
            found = i;
            break;
        }
    }

    if (found == -1) {  //**No path.  Just a service
        if (j < n) {
            *service = strdup(&(startpath[j]));
        }
        return(ptype);
    }

    i = found - j;
    *service = (i == 0) ? NULL : strndup(&(startpath[j]), i);

    //** Everything else is the path
    j = found + 1;
    if (found < n) {
        *path = strdup(&(startpath[j]));
    }

    return(ptype);
}

//***********************************************************************
// lio_set_timestamp - Sets the timestamp val/size for a attr put
//***********************************************************************

void lio_set_timestamp(char *id, char **val, int *v_size)
{
    *val = id;
    *v_size = (id == NULL) ? 0 : strlen(id);
    return;
}

//***********************************************************************
// lio_get_timestamp - Splits the timestamp ts/id field
//***********************************************************************

void lio_get_timestamp(char *val, int *timestamp, char **id)
{
    char *bstate;
    int fin;

    *timestamp = 0;
    sscanf(string_token(val, "|", &bstate, &fin), "%d", timestamp);
    if (id != NULL) *id = string_token(NULL, "|", &bstate, &fin);
    return;
}

//-------------------------------------------------------------------------
//------- Universal Object Iterators
//-------------------------------------------------------------------------


//*************************************************************************
//  unified_create_object_iter - Create an ls object iterator
//*************************************************************************

unified_object_iter_t *unified_create_object_iter(lio_path_tuple_t tuple, os_regex_table_t *path_regex, os_regex_table_t *obj_regex, int obj_types, int rd)
{
    unified_object_iter_t *it;

    type_malloc_clear(it, unified_object_iter_t, 1);

    it->tuple = tuple;
    if (tuple.is_lio == 1) {
        it->oit = os_create_object_iter(tuple.lc->os, tuple.creds, path_regex, obj_regex, obj_types, NULL, rd, NULL, 0);
    } else {
        it->lit = create_local_object_iter(path_regex, obj_regex, obj_types, rd);
    }

    return(it);
}

//*************************************************************************
//  unified_destroy_object_iter - Destroys an ls object iterator
//*************************************************************************

void unified_destroy_object_iter(unified_object_iter_t *it)
{

    if (it->tuple.is_lio == 1) {
        os_destroy_object_iter(it->tuple.lc->os, it->oit);
    } else {
        destroy_local_object_iter(it->lit);
    }

    free(it);
}

//*************************************************************************
//  unified_next_object - Returns the next object to work on
//*************************************************************************

int unified_next_object(unified_object_iter_t *it, char **fname, int *prefix_len)
{
    int err = 0;

    if (it->tuple.is_lio == 1) {
        err = os_next_object(it->tuple.lc->os, it->oit, fname, prefix_len);
    } else {
        err = local_next_object(it->lit, fname, prefix_len);
    }

    log_printf(15, "ftype=%d\n", err);
    return(err);
}

