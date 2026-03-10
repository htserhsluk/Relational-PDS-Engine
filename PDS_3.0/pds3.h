#ifndef PDS3_H
#define PDS3_H
#include <stdio.h>
#include <stdbool.h>

#define SUCCESS 0
#define FAILURE 1
#define REC_NOT_FOUND 1
#define DB_OPEN 0
#define DB_CLOSE 1
#define DB_FULL 2
#define MAX 1000

struct RecNdx{
	int key;
	int loc;
	bool is_deleted;
	int old_key;
};

/*struct pdsInfo{
	FILE *table_ptr_1;
	FILE *table_ptr_2;
	FILE *ndx_ptr_1;
	FILE *ndx_ptr_2;
};*/

struct tableInfo{
	char tname[100];
	int rec_size;
	FILE *tfile;
	FILE *ndxfile;
	char ndxfile_name[100];
	struct RecNdx ndxArray[MAX];
	int rec_count;
};

struct relInfo{
	char rname[200];
	char pritable[100];
	char reltable[100];
	FILE *rfile;
};

struct dbInfo{
	struct tableInfo tinfo[2];
	struct relInfo rel_info;
	int num_table;
	int status;
	int rel_status;
};

struct relPair{
	int prikey;
	int relkey;
	bool is_deleted;
};

extern struct dbInfo db_info;

void db_init();

struct tableInfo* get_tableInfo(char *table_name);

int table_create(char *table_name);

int table_open(char *table_name, int rec_size);

int table_store(char *table_name, int key, void *rec);

int table_get(char *table_name, int key, void *coutput);

int table_update(char *table_name, int key, void *new_rec);

int table_delete(char *table_name, int key);

int table_undelete(char *table_name, int key);

int table_close(char *table_name);

int db_create(char *tname1, char *tname2);

int db_open(char *tname1, int rec_size1, char *tname2, int rec_size2);

int db_close();

int create_rel(char *rel_name, char *tname1, char *tname2);

int open_rel(char *rel_name);

int store_rel(int prikey, int relkey);

int get_rel(void *related_rec, int prikey);

int delete_rel(char *table_name, int key);

int undelete_rel(char *table_name, int key);

int close_rel();

#endif
