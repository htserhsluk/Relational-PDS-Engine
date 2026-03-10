#ifndef PDS_H
#define PDS_H
#include <stdio.h>

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
	int is_deleted;
	int old_key;
};

struct dbInfo{
	FILE *dbfile;
	FILE *ndxfile;
	char dbname[50];
	char ndxname[50];
	int status;
	struct RecNdx ndxArray[MAX];
	int rec_count;
	int rec_size;
};

extern struct dbInfo db_info;

int create_db(char *dbname);

int open_db(char *dbname, int rec_size);

int store_db(int key, void *c);

int get_db(int key, void *coutput);

int close_db();

void db_init();

int update_db(int key, void *newcourse);

int delete_db(int key);

int undelete_db(int key);

#endif
