#include "pds3.h"
#include <string.h>
#include <stdbool.h>
#define NO_OF_TABLES 2

struct dbInfo db_info = { 
	.status = DB_CLOSE, 
	.rel_status = DB_CLOSE, 
	.num_table = 0 
};

struct relInfo rel_info;

void db_init(){
	db_info.num_table = 0;
    	db_info.status = DB_CLOSE;
    	db_info.rel_status = DB_CLOSE;
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

int restore_ndx(char *table_name, int rec_size){
	struct tableInfo *t = get_tableInfo(table_name);
	if(t == NULL) return FAILURE;
	char ndxfilename[100];
	strcpy(ndxfilename, table_name);
	strcat(ndxfilename, ".ndx");
	t->rec_count = 0;
	fseek(t->tfile, 0, SEEK_SET);
	int key;
	while(fread(&key, sizeof(int), 1, t->tfile) == 1){
		int loc = ftell(t->tfile) - sizeof(int);
		if(fseek(t->tfile, rec_size, SEEK_CUR) != 0) break;
		t->ndxArray[t->rec_count].key = key;
		t->ndxArray[t->rec_count].loc = loc;
		t->ndxArray[t->rec_count].is_deleted = false;
		t->ndxArray[t->rec_count].old_key = -1;
		t->rec_count++;
		if(t->rec_count >= MAX) break;
	}
	FILE *nf = fopen(ndxfilename, "wb");
	if(!nf) return FAILURE;
	fwrite(&t->rec_count, sizeof(int), 1, nf);
	fwrite(t->ndxArray, sizeof(struct RecNdx), t->rec_count, nf);
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
    	if(!t->tfile) return FAILURE;
    	t->ndxfile = fopen(ndxfilename,"rb");
    	if(!t->ndxfile){
    		if(restore_ndx(table_name, rec_size) != SUCCESS) return FAILURE;
    	} 
    	else{
    		fread(&t->rec_count,sizeof(int),1,t->ndxfile);
    		fread(t->ndxArray,sizeof(struct RecNdx),t->rec_count,t->ndxfile);
    		fclose(t->ndxfile);
    		t->ndxfile = NULL;
    	}
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
			delete_rel(table_name,key);
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
    	if(db_info.rel_status != DB_CLOSE) close_rel();
    	return SUCCESS;
}

int create_rel(char *rel_name, char *tname1, char *tname2){
	char relfilename[200];
	strcpy(relfilename,rel_name);
	strcat(relfilename,".rel");
	FILE *rf = fopen(relfilename,"wb");
	if(!rf) return FAILURE;
	char table1[100],table2[100];
	strcpy(table1,tname1);
	strcpy(table2,tname2);
	fwrite(table1,100,1,rf);
	fwrite(table2,100,1,rf);
	fclose(rf);
	return SUCCESS;	
}

int open_rel(char *rel_name){
	if(db_info.status == DB_CLOSE) return FAILURE;
	if(db_info.rel_status == DB_OPEN) return FAILURE;
	char relfilename[200];
	strcpy(relfilename,rel_name);
	strcat(relfilename,".rel");
	FILE *rf = fopen(relfilename,"rb+");
	if(!rf) return FAILURE;
	char table1[100],table2[100];
	fread(table1,100,1,rf);
	fread(table2,100,1,rf);
	if(get_tableInfo(table1) == NULL || get_tableInfo(table2) == NULL){
		fclose(rf);
		return FAILURE;
	}
	strcpy(db_info.rel_info.rname,relfilename);
	db_info.rel_info.rfile = rf;
	strcpy(db_info.rel_info.pritable,table1);
	strcpy(db_info.rel_info.reltable,table2);
	db_info.rel_status = DB_OPEN;
	return SUCCESS;
}

int store_rel(int prikey, int relkey){
	if(db_info.rel_status == DB_CLOSE) return FAILURE;
	struct tableInfo *pt = get_tableInfo(db_info.rel_info.pritable);
	if(pt == NULL) return FAILURE;
	int found_pri = 0;
	for(int i=0;i<pt->rec_count;i++){
		if(pt->ndxArray[i].key == prikey && !pt->ndxArray[i].is_deleted){ 
			found_pri=1; 
			break; 
		}
	}
	if(!found_pri) return FAILURE;
	struct tableInfo *rt = get_tableInfo(db_info.rel_info.reltable);
	if(rt == NULL) return FAILURE;
	int found_rel = 0;
	for(int i=0;i<rt->rec_count;i++){
		if(rt->ndxArray[i].key == relkey && !rt->ndxArray[i].is_deleted){ 
			found_rel=1; 
			break; 
		}
	}
	if(!found_rel) return FAILURE;
	struct relPair rel_pair;
	rel_pair.prikey = prikey;
	rel_pair.relkey = relkey;
	rel_pair.is_deleted = false;
	fseek(db_info.rel_info.rfile,0,SEEK_END);
	fwrite(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile);
	return SUCCESS;
}

int get_rel(void *related_rec, int prikey){
	if(db_info.rel_status == DB_CLOSE) return FAILURE;
	int key_found = -1;
	struct relPair rel_pair;
	fseek(db_info.rel_info.rfile,200,SEEK_SET);
	while((fread(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile) == 1)){
		if(rel_pair.prikey == prikey && !rel_pair.is_deleted){
			key_found = rel_pair.relkey;
			break;
		}
	}
	if(key_found == -1) return REC_NOT_FOUND;
	return table_get(db_info.rel_info.reltable,key_found,related_rec);
}

int delete_rel(char *table_name, int key){
	if(db_info.rel_status == DB_CLOSE) return FAILURE;
	int key_found = -1;
	bool is_primary;
	if(strcmp(db_info.rel_info.pritable,table_name) == 0) is_primary = true;
	else if(strcmp(db_info.rel_info.reltable,table_name) == 0) is_primary = false;
	else return FAILURE;
	struct relPair rel_pair;
	fseek(db_info.rel_info.rfile,200,SEEK_SET);
	if(is_primary){
		while((fread(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile) == 1)){
			if(rel_pair.prikey == key && !rel_pair.is_deleted){
				rel_pair.is_deleted = true;
				fseek(db_info.rel_info.rfile,-sizeof(struct relPair),SEEK_CUR);
				fwrite(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile);
				return SUCCESS;
			}
		}
	}
	else{
		while((fread(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile) == 1)){
			if(rel_pair.relkey == key && !rel_pair.is_deleted){
				rel_pair.is_deleted = true;
				fseek(db_info.rel_info.rfile,-sizeof(struct relPair),SEEK_CUR);
				fwrite(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile);
				return SUCCESS;
			}
		}
	}
	return REC_NOT_FOUND;	
}

int undelete_rel(char *table_name, int key){
	if(db_info.rel_status == DB_CLOSE) return FAILURE;
	int key_found = -1;
	bool is_primary;
	if(strcmp(db_info.rel_info.pritable,table_name) == 0) is_primary = true;
	else if(strcmp(db_info.rel_info.reltable,table_name) == 0) is_primary = false;
	else return FAILURE;
	struct relPair rel_pair;
	fseek(db_info.rel_info.rfile,200,SEEK_SET);
	if(is_primary){
		while((fread(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile) == 1)){
			if(rel_pair.prikey == key && rel_pair.is_deleted){
				rel_pair.is_deleted = false;
				fseek(db_info.rel_info.rfile,-sizeof(struct relPair),SEEK_CUR);
				fwrite(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile);
				return SUCCESS;
			}
		}
	}
	else{
		while((fread(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile) == 1)){
			if(rel_pair.relkey == key && rel_pair.is_deleted){
				rel_pair.is_deleted = false;
				fseek(db_info.rel_info.rfile,-sizeof(struct relPair),SEEK_CUR);
				fwrite(&rel_pair,sizeof(struct relPair),1,db_info.rel_info.rfile);
				return SUCCESS;
			}
		}
	}
	return REC_NOT_FOUND;
}

int close_rel(){
	if(db_info.rel_status != DB_OPEN) return FAILURE;
	fclose(db_info.rel_info.rfile);
	db_info.rel_info.rfile = NULL;
	db_info.rel_status = DB_CLOSE;
	db_info.rel_info.rname[0] = '\0';
	return SUCCESS;
}
