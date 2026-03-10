#include "pds.h"
#include <string.h>
#include <stdbool.h>

struct dbInfo db_info = { 
	.status = DB_CLOSE 
};

void db_init(){
	db_info.dbfile = NULL;
	db_info.ndxfile = NULL;
	strcpy(db_info.dbname,"");
	db_info.status = DB_CLOSE;
}

int create_db(char *dbname){
	strcpy(db_info.dbname,dbname);
	strcat(db_info.dbname,".dat");
	strcpy(db_info.ndxname,dbname);
	strcat(db_info.ndxname,".ndx");
	db_info.dbfile = fopen(db_info.dbname,"wb");
	db_info.ndxfile = fopen(db_info.ndxname,"wb");
	if(!db_info.dbfile || !db_info.ndxfile) return FAILURE;
	int total = 0;
	fwrite(&total,sizeof(int),1,db_info.ndxfile);
	db_info.status = DB_CLOSE;
	fclose(db_info.dbfile);
	fclose(db_info.ndxfile);
	return SUCCESS;
}

int open_db(char *dbname, int rec_size){
	if(db_info.status == DB_OPEN) return FAILURE;
	strcpy(db_info.dbname,dbname);
	strcat(db_info.dbname,".dat");
	strcpy(db_info.ndxname,dbname);
	strcat(db_info.ndxname,".ndx");
	db_info.rec_size = rec_size;
	db_info.dbfile = fopen(db_info.dbname,"rb+");
	db_info.ndxfile = fopen(db_info.ndxname,"rb+");
	if(!db_info.dbfile || !db_info.ndxfile) return FAILURE;
	int total;
	fread(&total,sizeof(int),1,db_info.ndxfile);
	db_info.rec_count = total;
	fread(db_info.ndxArray,sizeof(struct RecNdx),db_info.rec_count,db_info.ndxfile);
	db_info.status = DB_OPEN;
	fclose(db_info.ndxfile);
	db_info.ndxfile = NULL;
	return SUCCESS;
}

int store_db(int key, void *c){
	if(db_info.status != DB_OPEN) return FAILURE;
	if(db_info.rec_count >= MAX) return DB_FULL;
	fseek(db_info.dbfile,0,SEEK_END);
	int location = ftell(db_info.dbfile);
	db_info.ndxArray[db_info.rec_count].key = key;
	db_info.ndxArray[db_info.rec_count].loc = location;
	db_info.ndxArray[db_info.rec_count].is_deleted = false;
	db_info.ndxArray[db_info.rec_count].old_key = -1;
	db_info.rec_count++;
	fwrite(&key,sizeof(int),1,db_info.dbfile);
	fwrite(c,db_info.rec_size,1,db_info.dbfile);
	return SUCCESS;
}

int get_db(int key, void *coutput){
	if(db_info.status != DB_OPEN) return FAILURE;
	for(int i=0;i<db_info.rec_count;i++){
		if(db_info.ndxArray[i].key == key && !db_info.ndxArray[i].is_deleted){
			fseek(db_info.dbfile,db_info.ndxArray[i].loc + sizeof(int),SEEK_SET);
			fread(coutput,db_info.rec_size,1,db_info.dbfile);
			return SUCCESS;
		}
	}
	return REC_NOT_FOUND;
}

int close_db(){
	if(db_info.status != DB_OPEN) return FAILURE;
	db_info.ndxfile = fopen(db_info.ndxname,"wb");
	if(!db_info.ndxfile) return FAILURE;
	int total = db_info.rec_count;
	fwrite(&total,sizeof(int),1,db_info.ndxfile);
	fwrite(db_info.ndxArray,sizeof(struct RecNdx),db_info.rec_count,db_info.ndxfile);
	fclose(db_info.dbfile);
	fclose(db_info.ndxfile);
	db_info.dbfile = NULL;
	db_info.ndxfile = NULL;
	db_info.status = DB_CLOSE;
	return SUCCESS;
}

int update_db(int key, void *newcourse){
	if(db_info.status != DB_OPEN) return FAILURE;
	for(int i=0;i<db_info.rec_count;i++){
		if(db_info.ndxArray[i].key == key && !db_info.ndxArray[i].is_deleted){
			fseek(db_info.dbfile,db_info.ndxArray[i].loc + sizeof(int),SEEK_SET);
			fwrite(newcourse,db_info.rec_size,1,db_info.dbfile);
			return SUCCESS;
		}
	}
	return REC_NOT_FOUND;
}

int delete_db(int key){
	if(db_info.status != DB_OPEN) return FAILURE;
	for(int i=0;i<db_info.rec_count;i++){
		if(db_info.ndxArray[i].key == key && !db_info.ndxArray[i].is_deleted){
			db_info.ndxArray[i].old_key = key;
			db_info.ndxArray[i].key = -1;
			db_info.ndxArray[i].is_deleted = true;
			return SUCCESS;
		}
	}
	return FAILURE;
}

int undelete_db(int key){
	if(db_info.status != DB_OPEN) return FAILURE;
	for(int i=0;i<db_info.rec_count;i++){
		if(db_info.ndxArray[i].old_key == key && db_info.ndxArray[i].is_deleted){
			db_info.ndxArray[i].key = key;
			db_info.ndxArray[i].is_deleted = false;
			db_info.ndxArray[i].old_key = -1;
			return SUCCESS;
		}
	}
	return FAILURE;
}
