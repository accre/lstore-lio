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

#define _log_module_index 201

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"

#define MAX_SET 1000


//*************************************************************************
// load_file - Loads a file from disk
//*************************************************************************

void load_file(char *fname, char **val, int *v_size)
{
  FILE *fd;
  int i;

  *v_size = 0;  *val = NULL;

  fd = fopen(fname, "r");
  if (fd == NULL) {
     info_printf(lio_ifd, 0, "ERROR opeing file=%s!  Exiting\n", fname);
     exit(1);
  }
  fseek(fd, 0, SEEK_END);

  i = ftell(fd);
  type_malloc(*val, char, i+1);
  (*val)[i] = 0;
  *v_size = i;

  fseek(fd, 0, SEEK_SET);
  if (fread(*val, i, 1, fd) != 1) { //**
     info_printf(lio_ifd, 0, "ERROR reading file=%s! Exiting\n", fname);
     exit(1);
  }
  fclose(fd);

//info_printf(lio_ifd, 0, "fname=%s size=%d val=%s\n", fname, i, *val);

}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, rg_mode, start_index, start_option, fin, ftype, prefix_len;
  lio_path_tuple_t tuple;
  os_regex_table_t *rp_single, *ro_single, *attr_regex;
  os_object_iter_t *it;
  os_attr_iter_t *ait;
  char *bstate, *fname;
  char *path;
  char *key[MAX_SET];
  char *val[MAX_SET];
  int v_size[MAX_SET];
  int n_keys;
  int max_attr = 1024*1024;
  char *new_obj_fmt = "object=%s\\n";
  char *end_obj_fmt = "\\n";
  char *attr_fmt = "   %s=%s\\n";
  char *attr_sep = "";

  int recurse_depth = 10000;
  int obj_types = OS_OBJECT_FILE;

//estr = argv2format(argv[1]);
//printf("argv[1]=!%s! str=!%s!\n", argv[1], estr);
  if (argc < 2) {
     printf("\n");
     printf("lio_getattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-vmax max_val] PRINT_OPTIONS [-ga attr_glob | -ra attr_regex | -al key1,key2,...keyN] path\n");
     printf("lio_getattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-vmax max_val] PRINT_OPTIONS [-ga attr_glob | -ra attr_regex | -al key1,key2,...keyN] LIO_PATH_OPTIONS\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("    PRINT_OPTIONS: [-new_obj new_obj_fmt] [-end_obj end_obj_fmt] [-attr_fmt attr_fmt] [-attr_sep attr_sep]\n");
     printf("       -new_obj new_obj_fmt  - New object C format string.  Default is \"%s\"\n", new_obj_fmt);
     printf("       -end_obj end_obj_fmt  - End object C format string.  Default is \"%s\"\n", end_obj_fmt);
     printf("       -attr_fmt attr_fmt    - Attribute C format string.  Default is \"%s\"\n", attr_fmt);
     printf("       -attr_sep attr_sep    - Attribute separator C format string.  Default is \"%s\"\n", attr_sep);
     printf("\n");
     printf("       The format strings are string together according to the following code snippet:\n");
     printf("           printf(new_obj_fmt, object_name);\n");
     printf("           printf(attr_fmt, key[0], val[0]);\n");
     printf("           for (i=1; i<nattr; i++) {\n");
     printf("             printf(attr_sep);\n");
     printf("             printf(attr_fmt, key[i], val[i]);\n");
     printf("           }\n");
     printf("           printf(end_obj_fmt, object_name);\n");
     printf("\n");
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
     printf("    -vmax max_val      - Max size of attribute value allowed.  Default value is %d\n", max_attr);
     printf("    -ga attr_glob      - Attribute glob string\n");
     printf("    -ra attr_regex     - Attribute regex string\n");
     printf("    -al key1,key2,...  - Comma separated list of keys to retrieve\n");
     printf("    path               - Path glob to scan\n");
     return(1);
  }

  lio_init(&argc, &argv);


  //*** Parse the args
  rg_mode = 0;
  rp_single = ro_single = attr_regex = NULL;
  n_keys = 0;
  path = NULL;

  rg_mode = lio_parse_path_options(&argc, argv, &tuple, &rp_single, &ro_single);

  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
        i++;
        obj_types = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-vmax") == 0) {  //** MAx attribute size
        i++;
        max_attr = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-new_obj") == 0) {  //**  New object format
        i++;
        new_obj_fmt = argv[i]; i++;
     } else if (strcmp(argv[i], "-end_obj") == 0) {  //**  End object format
        i++;
        end_obj_fmt = argv[i]; i++;
     } else if (strcmp(argv[i], "-attr_fmt") == 0) {  //**  Attribute format
        i++;
        attr_fmt = argv[i]; i++;
     } else if (strcmp(argv[i], "-attr_sep") == 0) {  //**  Attribute separator
        i++;
        attr_sep = argv[i]; i++;
     } else if (strcmp(argv[i], "-ga") == 0) {  //**  Attribute glob
        i++;
        attr_regex = os_path_glob2regex(argv[i]); i++;
     } else if (strcmp(argv[i], "-ra") == 0) {  //**  Attribute regex
        i++;
        attr_regex = os_regex2table(argv[i]); i++;
     } else if (strcmp(argv[i], "-al") == 0) {  //**  Attribute list
        i++;
        key[0]=string_token(argv[i], ",", &bstate, &fin);
        n_keys = 0;
        do {
          n_keys++;
          key[n_keys] = string_token(NULL, ",", &bstate, &fin);
        } while (fin == 0);
        i++;
        //for(err=0; err<n_keys; err++) printf("key[%d]=%s\n", err, key[err]);
     }
  } while ((start_option < i) && (i<argc));
  start_index = i;

  //** Convert all the format strings
  new_obj_fmt = argv2format(new_obj_fmt);
  end_obj_fmt = argv2format(end_obj_fmt);
  attr_fmt = argv2format(attr_fmt);
  attr_sep = argv2format(attr_sep);

  //** MAke the path if needed
  if (rg_mode == 0) {
     if (i >= argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
     }
     path = argv[i];
log_printf(15, "path=%s\n", path);

     //** Create the simple path iterator
     tuple = lio_path_resolve(path);
     rp_single = os_path_glob2regex(tuple.path);
  }


  //** Make the iterator.  It depends on if we have a regex or a list
  if (n_keys > 0) {  //** Using a fixed list of keys
     for (i=0; i<n_keys; i++) { v_size[i] = -max_attr; val[i] = NULL; }
     it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, obj_types, recurse_depth, key, (void **)val, v_size, n_keys);
  } else {   //** Got a regex for attribute selection
     v_size[0] = -max_attr;
     it = os_create_object_iter(tuple.lc->os, tuple.creds, rp_single, ro_single, obj_types, attr_regex, recurse_depth, &ait, v_size[0]);
  }

  if (it == NULL) {
     info_printf(lio_ifd, 0, "ERROR creating iterator!\n");
  }

log_printf(15, "before main loop\n");
  //**Now iterate through the objects
  while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
     if (attr_regex != NULL) {  //** Got an attr regex so load all the attr
        n_keys = 0;
        v_size[n_keys] = -1*max_attr;
        val[n_keys] = NULL;
log_printf(15, "v_size = %d\n", v_size[n_keys]);
        while (os_next_attr(tuple.lc->os, ait, &(key[n_keys]), (void **)&(val[n_keys]), &(v_size[n_keys])) == 0) {
           n_keys++;
           v_size[n_keys] = -1*max_attr;
           val[n_keys] = NULL;
        }
     }

     //** Print the record
     info_printf(lio_ifd, 0, new_obj_fmt, fname);
     if (n_keys > 0) {
        info_printf(lio_ifd, 0, attr_fmt, key[0], val[0]);
        for (i=1; i<n_keys; i++) {
            info_printf(lio_ifd, 0, "%s", attr_sep);
            info_printf(lio_ifd, 0, attr_fmt, key[i], val[i]);
        }
     }
     info_printf(lio_ifd, 0, end_obj_fmt, fname);

     //** Free the space
     free(fname);
     for (i=0; i<n_keys; i++) {
       if (val[i] != NULL) free(val[i]);
       if (attr_regex != NULL) free(key[i]);
     }
  }

  os_destroy_object_iter(tuple.lc->os, it);

log_printf(15, "after main loop\n");

  lio_path_release(&tuple);
  if (ro_single != NULL) os_regex_table_destroy(ro_single);
  if (rp_single != NULL) os_regex_table_destroy(rp_single);
  if (attr_regex != NULL) os_regex_table_destroy(attr_regex);

  free(new_obj_fmt);
  free(end_obj_fmt);
  free(attr_fmt);
  free(attr_sep);

  lio_shutdown();

  return(0);
}

