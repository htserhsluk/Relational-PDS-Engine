#ifndef PDS4_H
#define PDS4_H
#include <stdio.h>
#include <stdbool.h>

#define SUCCESS 0
#define FAILURE 1
#define REC_NOT_FOUND 1
#define DB_OPEN 0
#define DB_CLOSE 1
#define DB_FULL 2
#define MAX 1000

struct RecNdx
{
    int key;
    int loc;
    bool is_deleted;
    int old_key;
};

struct tableInfo
{
    char tname[100];
    int rec_size;
    FILE *tfile;
    FILE *ndxfile;
    char ndxfile_name[100];
    struct RecNdx ndxArray[MAX];
    int rec_count;
};

struct relPair
{
    int prikey;
    int relkey;
    bool is_deleted;
};

struct relInfo
{
    char rname[200];
    char pritable[100];
    char reltable[100];
    FILE *rfile;
};

struct dbInfo
{
    char dbname[100];
    int num_table;
    int num_rel;
    int status;
    int rel_status;
    struct tableInfo **tinfo; // dynamic — unlimited tables
    struct relInfo **rinfo;   // dynamic — unlimited relationships
};

extern struct dbInfo db_info;

void db_init(char *dbname);
int db_create(char *dbname, char **tnames, int *rec_sizes, int num_tables);
int db_open(char *dbname);
int db_close();

int save_schema();
int load_schema();

// tables always open while db is open
struct tableInfo *get_tableInfo(char *table_name);
int restore_ndx(char *table_name, int rec_size);
int table_store(char *table_name, int key, void *rec);
int table_get(char *table_name, int key, void *coutput);
int table_update(char *table_name, int key, void *new_rec);
int table_delete(char *table_name, int key);
int table_undelete(char *table_name, int key);
int table_search_field(char *table_name, int field_offset, int field_size, void *field_value, void *coutput);

struct relInfo *get_relInfo(char *rel_name);
int create_rel(char *rel_name, char *tname1, char *tname2);
int open_rel(char *rel_name);
int store_rel(char *rel_name, int prikey, int relkey);
int get_rel(char *rel_name, void *related_rec, int prikey);
int delete_rel(char *rel_name, int key, bool is_primary);
int undelete_rel(char *rel_name, int key, bool is_primary);
int close_rel(char *rel_name);

#endif