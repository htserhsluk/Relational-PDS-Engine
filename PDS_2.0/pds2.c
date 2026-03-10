#include "pds2.h"
#include <string.h>
#include <stdbool.h>
#define NO_OF_TABLES 2

struct dbInfo db_info = { 
	.status = DB_CLOSE, 
	.num_table = 0 
};

void db_init(){
	db_info.num_table = 0;
    	db_info.status = DB_CLOSE;
    	for(int i=0;i<NO_OF_TABLES;i++){
        	db_info.tinfo[i].tname[0] = '\0';
        	db_info.tinfo[i].ndxfile_name[0] = '\0';
        	db_info.tinfo[i].rec_count = 0;
        	db_info.tinfo[i].rec_size = 0;
        	db_info.tinfo[i].tfile = NULL;
        	db_info.tinfo[i].ndxfile = NULL;
        	for(int j=0;j<MAX;j++){
            		db_info.tinfo[i].ndxArray[j].key = -1;
            		db_info.tinfo[i].ndxArray[j].is_deleted = false;
        	}
    	}
}

struct tableInfo* get_tableInfo(char *table_name){
	for(int i=0;i<NO_OF_TABLES;i++){
		if(strcmp(db_info.tinfo[i].tname,table_name) == 0){
			return &db_info.tinfo[i];
		}
	}
	return NULL;
}

int table_create(char *table_name){
	char datfilename[100],ndxfilename[100];
	strcpy(datfilename,table_name);
	strcat(datfilename,".dat");
	strcpy(ndxfilename,table_name);
	strcat(ndxfilename,".ndx");
	FILE *df = fopen(datfilename,"wb");
	FILE *nf = fopen(ndxfilename,"wb");
	if(!df || !nf) return FAILURE;
	int total = 0;
	fwrite(&total,sizeof(int),1,nf);
	fclose(df);
	fclose(nf);
	return SUCCESS;
}

int table_open(char *table_name, int rec_size){
	int slot = -1;
    	for(int i=0;i<NO_OF_TABLES;i++){
        	if(db_info.tinfo[i].tname[0] == '\0' || strcmp(db_info.tinfo[i].tname,table_name) == 0){
            		slot = i;
            		break;
        	}
    	}
    	if(slot == -1) return FAILURE;
    	struct tableInfo *t = &db_info.tinfo[slot];
    	strcpy(t->tname,table_name);
    	strcpy(t->ndxfile_name,table_name);
    	t->rec_size = rec_size;
    	char datfilename[100],ndxfilename[100];
    	strcpy(datfilename,t->ndxfile_name);
    	strcat(datfilename,".dat");
    	strcpy(ndxfilename,t->ndxfile_name);
    	strcat(ndxfilename,".ndx");
    	t->tfile = fopen(datfilename,"rb+");
    	t->ndxfile = fopen(ndxfilename,"rb");
    	if(!t->tfile || !t->ndxfile) return FAILURE;
    	fread(&t->rec_count,sizeof(int),1,t->ndxfile);
    	fread(t->ndxArray,sizeof(struct RecNdx),t->rec_count,t->ndxfile);
    	fclose(t->ndxfile);
    	t->ndxfile = NULL;
    	db_info.num_table++;
    	return SUCCESS;
}

int table_store(char *table_name, int key, void *rec){
	struct tableInfo *t = get_tableInfo(table_name);
	if(t == NULL) return FAILURE;
	if(t->rec_count >= MAX) return FAILURE;
	fseek(t->tfile,0,SEEK_END);
	int loc = ftell(t->tfile);
	t->ndxArray[t->rec_count].key = key;
    	t->ndxArray[t->rec_count].loc = loc;
    	t->ndxArray[t->rec_count].is_deleted = false;
    	t->ndxArray[t->rec_count].old_key = -1;
    	fwrite(&key,sizeof(int),1,t->tfile);
    	fwrite(rec,t->rec_size,1,t->tfile);
    	t->rec_count++;
	return SUCCESS;
}

int table_get(char *table_name, int key, void *coutput){
	struct tableInfo *t = get_tableInfo(table_name);
	if(t == NULL) return FAILURE;
    	for(int i=0;i<t->rec_count;i++){
        	if(t->ndxArray[i].key == key && !t->ndxArray[i].is_deleted){
            		fseek(t->tfile,t->ndxArray[i].loc + sizeof(int),SEEK_SET);
            		fread(coutput,t->rec_size,1,t->tfile);
            		return SUCCESS;
        	}
    	}
    	return REC_NOT_FOUND;
}

int table_update(char *table_name, int key, void *new_rec){
	struct tableInfo *t = get_tableInfo(table_name);
	if(t == NULL) return FAILURE;
	for(int i=0;i<t->rec_count;i++){
        	if(t->ndxArray[i].key == key && !t->ndxArray[i].is_deleted){
            		fseek(t->tfile,t->ndxArray[i].loc + sizeof(int),SEEK_SET);
            		fwrite(new_rec,t->rec_size,1,t->tfile);
            		return SUCCESS;
        	}
    	}
    	return REC_NOT_FOUND;
}

int table_delete(char *table_name, int key){
	struct tableInfo *t = get_tableInfo(table_name);
	if(t == NULL) return FAILURE;
	for(int i=0;i<t->rec_count;i++){
		if(t->ndxArray[i].key == key && !t->ndxArray[i].is_deleted){
			t->ndxArray[i].old_key = key;
			t->ndxArray[i].key = -1;
			t->ndxArray[i].is_deleted = true;
			return SUCCESS;
		}
	}
	return FAILURE;
}

int table_undelete(char *table_name, int key){
	struct tableInfo *t = get_tableInfo(table_name);
	if(t == NULL) return FAILURE;
	for(int i=0;i<t->rec_count;i++){
		if(t->ndxArray[i].old_key == key && t->ndxArray[i].is_deleted){
			t->ndxArray[i].key = key;
			t->ndxArray[i].old_key = -1;
			t->ndxArray[i].is_deleted = false;
			return SUCCESS;
		}
	}
	return FAILURE;
}

int table_close(char *table_name){
	struct tableInfo *t = get_tableInfo(table_name);
    	if(t == NULL) return FAILURE;
    	char ndxfilename[100];
    	strcpy(ndxfilename,t->ndxfile_name);
    	strcat(ndxfilename,".ndx");
    	t->ndxfile = fopen(ndxfilename,"wb");
    	if(!t->ndxfile) return FAILURE;
    	fwrite(&t->rec_count,sizeof(int),1,t->ndxfile);
    	fwrite(t->ndxArray,sizeof(struct RecNdx),t->rec_count,t->ndxfile);
    	fclose(t->ndxfile);
    	fclose(t->tfile);
    	t->tname[0] = '\0'; 
    	db_info.num_table--; 
    	return SUCCESS;
}

int db_create(char *tname1, char *tname2){
	if(table_create(tname1) != SUCCESS) return FAILURE;
    	if(table_create(tname2) != SUCCESS) return FAILURE;
    	return SUCCESS;
}

int db_open(char *tname1, int rec_size1, char *tname2, int rec_size2){
	if(db_info.status == DB_OPEN) return FAILURE;
	db_info.num_table = 0;
    	if(table_open(tname1,rec_size1) != SUCCESS){
    		for(int i=0;i<NO_OF_TABLES;i++){ 
    			if(db_info.tinfo[i].tfile){ 
    				fclose(db_info.tinfo[i].tfile); 
    				db_info.tinfo[i].tfile=NULL; 
    			} 
    			db_info.tinfo[i].tname[0]='\0'; 
    		}
    		db_info.num_table = 0;
    		return FAILURE;
    	}
    	if(table_open(tname2,rec_size2) != SUCCESS){
    		for(int i=0;i<NO_OF_TABLES;i++){ 
    			if(db_info.tinfo[i].tfile){ 
    				fclose(db_info.tinfo[i].tfile); 
    				db_info.tinfo[i].tfile=NULL; 
    			} 
    			db_info.tinfo[i].tname[0]='\0'; 
    		}
    		db_info.num_table = 0;
    		return FAILURE;
    	}
    	db_info.status = DB_OPEN;
    	return SUCCESS;
}

int db_close(){
    	if(db_info.status == DB_CLOSE) return FAILURE;
    	for(int i=0;i<NO_OF_TABLES;i++){
        	if(db_info.tinfo[i].tname[0] != '\0'){
            		table_close(db_info.tinfo[i].tname);
        	}
    	}
    	db_info.status = DB_CLOSE;
    	return SUCCESS;
}
