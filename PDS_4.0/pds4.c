#include "pds4.h"
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

struct dbInfo db_info = {
    .status = DB_CLOSE,
    .rel_status = DB_CLOSE};

struct tableInfo *get_tableInfo(char *table_name)
{
    for (int i = 0; i < db_info.num_table; i++)
    {
        if (strcmp(db_info.tinfo[i]->tname, table_name) == 0)
        {
            return db_info.tinfo[i];
        }
    }
    return NULL;
}

struct relInfo *get_relInfo(char *rel_name)
{
    for (int i = 0; i < db_info.num_rel; i++)
    {
        if (strcmp(db_info.rinfo[i]->rname, rel_name) == 0)
        {
            return db_info.rinfo[i];
        }
    }
    return NULL;
}

int save_schema()
{
    char schname[110];
    strcpy(schname, db_info.dbname);
    strcat(schname, ".sch");
    FILE *sf = fopen(schname, "wb");
    if (!sf)
        return FAILURE;
    fwrite(&db_info.num_table, sizeof(int), 1, sf);
    for (int i = 0; i < db_info.num_table; i++)
    {
        struct tableInfo *t = db_info.tinfo[i];
        fwrite(t->tname, 100, 1, sf);
        fwrite(t->ndxfile_name, 100, 1, sf);
        fwrite(&t->rec_size, sizeof(int), 1, sf);
        fwrite(&t->rec_count, sizeof(int), 1, sf);
        fwrite(t->ndxArray, sizeof(struct RecNdx), t->rec_count, sf);
    }

    fwrite(&db_info.num_rel, sizeof(int), 1, sf);
    for (int i = 0; i < db_info.num_rel; i++)
    {
        struct relInfo *r = db_info.rinfo[i];
        fwrite(r->rname, 200, 1, sf);
        fwrite(r->pritable, 100, 1, sf);
        fwrite(r->reltable, 100, 1, sf);
    }

    fclose(sf);
    return SUCCESS;
}

int load_schema()
{
    char schname[110];
    strcpy(schname, db_info.dbname);
    strcat(schname, ".sch");
    FILE *sf = fopen(schname, "rb");
    if (!sf)
        return FAILURE;

    fread(&db_info.num_table, sizeof(int), 1, sf);
    db_info.tinfo = malloc(db_info.num_table * sizeof(struct tableInfo *));
    if (!db_info.tinfo)
    {
        fclose(sf);
        return FAILURE;
    }

    for (int i = 0; i < db_info.num_table; i++)
    {
        struct tableInfo *t = calloc(1, sizeof(struct tableInfo));
        if (!t)
        {
            fclose(sf);
            return FAILURE;
        }
        fread(t->tname, 100, 1, sf);
        fread(t->ndxfile_name, 100, 1, sf);
        fread(&t->rec_size, sizeof(int), 1, sf);
        fread(&t->rec_count, sizeof(int), 1, sf);
        fread(t->ndxArray, sizeof(struct RecNdx), t->rec_count, sf);
        t->tfile = NULL;
        t->ndxfile = NULL;
        db_info.tinfo[i] = t;
    }

    fread(&db_info.num_rel, sizeof(int), 1, sf);
    db_info.rinfo = (db_info.num_rel > 0) ? malloc(db_info.num_rel * sizeof(struct relInfo *)) : NULL;

    for (int i = 0; i < db_info.num_rel; i++)
    {
        struct relInfo *r = calloc(1, sizeof(struct relInfo));
        if (!r)
        {
            fclose(sf);
            return FAILURE;
        }
        fread(r->rname, 200, 1, sf);
        fread(r->pritable, 100, 1, sf);
        fread(r->reltable, 100, 1, sf);
        r->rfile = NULL;
        db_info.rinfo[i] = r;
    }

    fclose(sf);
    return SUCCESS;
}

void db_init(char *dbname)
{
    if (db_info.tinfo)
    {
        for (int i = 0; i < db_info.num_table; i++)
        {
            if (db_info.tinfo[i])
            {
                if (db_info.tinfo[i]->tfile)
                    fclose(db_info.tinfo[i]->tfile);
                if (db_info.tinfo[i]->ndxfile)
                    fclose(db_info.tinfo[i]->ndxfile);
                free(db_info.tinfo[i]);
            }
        }
        free(db_info.tinfo);
        db_info.tinfo = NULL;
    }
    if (db_info.rinfo)
    {
        for (int i = 0; i < db_info.num_rel; i++)
        {
            if (db_info.rinfo[i])
            {
                if (db_info.rinfo[i]->rfile)
                    fclose(db_info.rinfo[i]->rfile);
                free(db_info.rinfo[i]);
            }
        }
        free(db_info.rinfo);
        db_info.rinfo = NULL;
    }
    db_info.num_table = 0;
    db_info.num_rel = 0;
    db_info.status = DB_CLOSE;
    db_info.rel_status = DB_CLOSE;
    if (dbname)
        strcpy(db_info.dbname, dbname);
}

int db_create(char *dbname, char **tnames, int *rec_sizes, int num_tables)
{
    db_init(dbname);

    db_info.tinfo = malloc(num_tables * sizeof(struct tableInfo *));
    if (!db_info.tinfo)
        return FAILURE;

    for (int i = 0; i < num_tables; i++)
    {
        char datfile[110], ndxfile[110];
        strcpy(datfile, tnames[i]);
        strcat(datfile, ".dat");
        strcpy(ndxfile, tnames[i]);
        strcat(ndxfile, ".ndx");

        FILE *df = fopen(datfile, "wb");
        FILE *nf = fopen(ndxfile, "wb");
        if (!df || !nf)
            return FAILURE;
        int zero = 0;
        fwrite(&zero, sizeof(int), 1, nf);
        fclose(nf);
        fclose(df);

        struct tableInfo *t = calloc(1, sizeof(struct tableInfo));
        if (!t)
            return FAILURE;
        strcpy(t->tname, tnames[i]);
        strcpy(t->ndxfile_name, tnames[i]);
        t->rec_size = rec_sizes[i];
        t->rec_count = 0;
        t->ndxfile = NULL;
        t->tfile = NULL; // not open yet — call db_open to use
        db_info.tinfo[i] = t;
        db_info.num_table++;
    }

    db_info.num_rel = 0;
    db_info.rinfo = NULL;
    db_info.status = DB_CLOSE;

    return save_schema();
}

int db_open(char *dbname)
{
    if (db_info.status == DB_OPEN)
        return FAILURE;
    db_init(dbname);

    if (load_schema() != SUCCESS)
        return FAILURE;

    for (int i = 0; i < db_info.num_table; i++)
    {
        struct tableInfo *t = db_info.tinfo[i];
        char datfile[110], ndxfile[110];
        strcpy(datfile, t->ndxfile_name);
        strcat(datfile, ".dat");
        strcpy(ndxfile, t->ndxfile_name);
        strcat(ndxfile, ".ndx");

        t->tfile = fopen(datfile, "rb+");
        if (!t->tfile)
            return FAILURE;

        FILE *nf = fopen(ndxfile, "rb");
        if (!nf)
        {
            if (restore_ndx(t->tname, t->rec_size) != SUCCESS)
                return FAILURE;
        }
        else
        {
            fread(&t->rec_count, sizeof(int), 1, nf);
            fread(t->ndxArray, sizeof(struct RecNdx), t->rec_count, nf);
            fclose(nf);
            t->ndxfile = NULL;
        }
    }

    db_info.status = DB_OPEN;
    return SUCCESS;
}

int db_close()
{
    if (db_info.status == DB_CLOSE)
        return FAILURE;

    for (int i = 0; i < db_info.num_rel; i++)
    {
        if (db_info.rinfo[i]->rfile)
        {
            fclose(db_info.rinfo[i]->rfile);
            db_info.rinfo[i]->rfile = NULL;
        }
    }
    db_info.rel_status = DB_CLOSE;

    for (int i = 0; i < db_info.num_table; i++)
    {
        struct tableInfo *t = db_info.tinfo[i];
        if (!t || !t->tfile)
            continue;
        char ndxfile[110];
        strcpy(ndxfile, t->ndxfile_name);
        strcat(ndxfile, ".ndx");
        FILE *nf = fopen(ndxfile, "wb");
        if (nf)
        {
            fwrite(&t->rec_count, sizeof(int), 1, nf);
            fwrite(t->ndxArray, sizeof(struct RecNdx), t->rec_count, nf);
            fclose(nf);
        }
        fclose(t->tfile);
        t->tfile = NULL;
    }

    save_schema();
    db_info.status = DB_CLOSE;
    return SUCCESS;
}

int restore_ndx(char *table_name, int rec_size)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    char ndxfile[110];
    strcpy(ndxfile, table_name);
    strcat(ndxfile, ".ndx");

    t->rec_count = 0;
    fseek(t->tfile, 0, SEEK_SET);
    int key;
    while (fread(&key, sizeof(int), 1, t->tfile) == 1)
    {
        int loc = (int)ftell(t->tfile) - (int)sizeof(int);
        if (fseek(t->tfile, rec_size, SEEK_CUR) != 0)
            break;
        t->ndxArray[t->rec_count].key = key;
        t->ndxArray[t->rec_count].loc = loc;
        t->ndxArray[t->rec_count].is_deleted = false;
        t->ndxArray[t->rec_count].old_key = -1;
        t->rec_count++;
        if (t->rec_count >= MAX)
            break;
    }
    FILE *nf = fopen(ndxfile, "wb");
    if (!nf)
        return FAILURE;
    fwrite(&t->rec_count, sizeof(int), 1, nf);
    fwrite(t->ndxArray, sizeof(struct RecNdx), t->rec_count, nf);
    fclose(nf);
    return SUCCESS;
}

int table_store(char *table_name, int key, void *rec)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    if (t->rec_count >= MAX)
        return FAILURE;
    fseek(t->tfile, 0, SEEK_END);
    int loc = (int)ftell(t->tfile);
    t->ndxArray[t->rec_count].key = key;
    t->ndxArray[t->rec_count].loc = loc;
    t->ndxArray[t->rec_count].is_deleted = false;
    t->ndxArray[t->rec_count].old_key = -1;
    fwrite(&key, sizeof(int), 1, t->tfile);
    fwrite(rec, t->rec_size, 1, t->tfile);
    t->rec_count++;
    return SUCCESS;
}

int table_get(char *table_name, int key, void *coutput)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    for (int i = 0; i < t->rec_count; i++)
    {
        if (t->ndxArray[i].key == key && !t->ndxArray[i].is_deleted)
        {
            fseek(t->tfile, t->ndxArray[i].loc + sizeof(int), SEEK_SET);
            fread(coutput, t->rec_size, 1, t->tfile);
            return SUCCESS;
        }
    }
    return REC_NOT_FOUND;
}

int table_update(char *table_name, int key, void *new_rec)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    for (int i = 0; i < t->rec_count; i++)
    {
        if (t->ndxArray[i].key == key && !t->ndxArray[i].is_deleted)
        {
            fseek(t->tfile, t->ndxArray[i].loc + sizeof(int), SEEK_SET);
            fwrite(new_rec, t->rec_size, 1, t->tfile);
            return SUCCESS;
        }
    }
    return REC_NOT_FOUND;
}

int table_delete(char *table_name, int key)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    for (int i = 0; i < t->rec_count; i++)
    {
        if (t->ndxArray[i].key == key && !t->ndxArray[i].is_deleted)
        {
            t->ndxArray[i].old_key = key;
            t->ndxArray[i].key = -1;
            t->ndxArray[i].is_deleted = true;

            // cascade delete to any open relationships
            for (int r = 0; r < db_info.num_rel; r++)
            {
                struct relInfo *ri = db_info.rinfo[r];
                if (!ri->rfile)
                    continue;
                bool is_pri = (strcmp(ri->pritable, table_name) == 0);
                bool is_rel = (strcmp(ri->reltable, table_name) == 0);
                if (!is_pri && !is_rel)
                    continue;
                struct relPair rp;
                fseek(ri->rfile, 200, SEEK_SET);
                while (fread(&rp, sizeof(struct relPair), 1, ri->rfile) == 1)
                {
                    int match = is_pri ? (rp.prikey == key) : (rp.relkey == key);
                    if (match && !rp.is_deleted)
                    {
                        rp.is_deleted = true;
                        fseek(ri->rfile, -(long)sizeof(struct relPair), SEEK_CUR);
                        fwrite(&rp, sizeof(struct relPair), 1, ri->rfile);
                    }
                }
            }
            return SUCCESS;
        }
    }
    return FAILURE;
}

int table_undelete(char *table_name, int key)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    for (int i = 0; i < t->rec_count; i++)
    {
        if (t->ndxArray[i].old_key == key && t->ndxArray[i].is_deleted)
        {
            t->ndxArray[i].key = key;
            t->ndxArray[i].old_key = -1;
            t->ndxArray[i].is_deleted = false;
            return SUCCESS;
        }
    }
    return FAILURE;
}

/*
 * table_search_field: find the first non-deleted record where the field
 * at field_offset matches field_value.
 * Pass field_size=0 for null-terminated string comparison (strcmp).
 * Pass field_size>0 for raw binary comparison (memcmp).
 */
int table_search_field(char *table_name, int field_offset, int field_size, void *field_value, void *coutput)
{
    struct tableInfo *t = get_tableInfo(table_name);
    if (!t)
        return FAILURE;
    char *buf = malloc(t->rec_size);
    if (!buf)
        return FAILURE;
    for (int i = 0; i < t->rec_count; i++)
    {
        if (t->ndxArray[i].is_deleted)
            continue;
        fseek(t->tfile, t->ndxArray[i].loc + sizeof(int), SEEK_SET);
        fread(buf, t->rec_size, 1, t->tfile);
        int match = (field_size == 0) ? (strcmp(buf + field_offset, (char *)field_value) == 0) : (memcmp(buf + field_offset, field_value, field_size) == 0);
        if (match)
        {
            memcpy(coutput, buf, t->rec_size);
            free(buf);
            return SUCCESS;
        }
    }
    free(buf);
    return REC_NOT_FOUND;
}

int create_rel(char *rel_name, char *tname1, char *tname2)
{
    char relfile[210];
    strcpy(relfile, rel_name);
    strcat(relfile, ".rel");
    FILE *rf = fopen(relfile, "wb");
    if (!rf)
        return FAILURE;
    char t1[100], t2[100];
    strcpy(t1, tname1);
    strcpy(t2, tname2);
    fwrite(t1, 100, 1, rf);
    fwrite(t2, 100, 1, rf);
    fclose(rf);

    struct relInfo *r = calloc(1, sizeof(struct relInfo));
    if (!r)
        return FAILURE;
    strcpy(r->rname, rel_name);
    strcpy(r->pritable, tname1);
    strcpy(r->reltable, tname2);
    r->rfile = NULL;

    db_info.rinfo = realloc(db_info.rinfo, (db_info.num_rel + 1) * sizeof(struct relInfo *));
    if (!db_info.rinfo)
        return FAILURE;
    db_info.rinfo[db_info.num_rel++] = r;

    save_schema();
    return SUCCESS;
}

int open_rel(char *rel_name)
{
    if (db_info.status == DB_CLOSE)
        return FAILURE;
    struct relInfo *r = get_relInfo(rel_name);
    if (!r)
        return FAILURE;
    if (r->rfile != NULL)
        return FAILURE;
    if (!get_tableInfo(r->pritable) || !get_tableInfo(r->reltable))
        return FAILURE;
    char relfile[210];
    strcpy(relfile, rel_name);
    strcat(relfile, ".rel");
    r->rfile = fopen(relfile, "rb+");
    if (!r->rfile)
        return FAILURE;
    db_info.rel_status = DB_OPEN;
    return SUCCESS;
}

int store_rel(char *rel_name, int prikey, int relkey)
{
    struct relInfo *r = get_relInfo(rel_name);
    if (!r || !r->rfile)
        return FAILURE;
    struct tableInfo *pt = get_tableInfo(r->pritable);
    struct tableInfo *rt = get_tableInfo(r->reltable);
    if (!pt || !rt)
        return FAILURE;
    int fp = 0, fr = 0;
    for (int i = 0; i < pt->rec_count; i++)
        if (pt->ndxArray[i].key == prikey && !pt->ndxArray[i].is_deleted)
        {
            fp = 1;
            break;
        }
    for (int i = 0; i < rt->rec_count; i++)
        if (rt->ndxArray[i].key == relkey && !rt->ndxArray[i].is_deleted)
        {
            fr = 1;
            break;
        }
    if (!fp || !fr)
        return FAILURE;
    struct relPair rp = {prikey, relkey, false};
    fseek(r->rfile, 0, SEEK_END);
    fwrite(&rp, sizeof(struct relPair), 1, r->rfile);
    return SUCCESS;
}

int get_rel(char *rel_name, void *related_rec, int prikey)
{
    struct relInfo *r = get_relInfo(rel_name);
    if (!r || !r->rfile)
        return FAILURE;
    struct relPair rp;
    fseek(r->rfile, 200, SEEK_SET);
    while (fread(&rp, sizeof(struct relPair), 1, r->rfile) == 1)
        if (rp.prikey == prikey && !rp.is_deleted)
            return table_get(r->reltable, rp.relkey, related_rec);
    return REC_NOT_FOUND;
}

int delete_rel(char *rel_name, int key, bool is_primary)
{
    struct relInfo *r = get_relInfo(rel_name);
    if (!r || !r->rfile)
        return FAILURE;
    struct relPair rp;
    fseek(r->rfile, 200, SEEK_SET);
    while (fread(&rp, sizeof(struct relPair), 1, r->rfile) == 1)
    {
        int match = is_primary ? (rp.prikey == key) : (rp.relkey == key);
        if (match && !rp.is_deleted)
        {
            rp.is_deleted = true;
            fseek(r->rfile, -(long)sizeof(struct relPair), SEEK_CUR);
            fwrite(&rp, sizeof(struct relPair), 1, r->rfile);
            return SUCCESS;
        }
    }
    return REC_NOT_FOUND;
}

int undelete_rel(char *rel_name, int key, bool is_primary)
{
    struct relInfo *r = get_relInfo(rel_name);
    if (!r || !r->rfile)
        return FAILURE;
    struct relPair rp;
    fseek(r->rfile, 200, SEEK_SET);
    while (fread(&rp, sizeof(struct relPair), 1, r->rfile) == 1)
    {
        int match = is_primary ? (rp.prikey == key) : (rp.relkey == key);
        if (match && rp.is_deleted)
        {
            rp.is_deleted = false;
            fseek(r->rfile, -(long)sizeof(struct relPair), SEEK_CUR);
            fwrite(&rp, sizeof(struct relPair), 1, r->rfile);
            return SUCCESS;
        }
    }
    return REC_NOT_FOUND;
}

int close_rel(char *rel_name)
{
    struct relInfo *r = get_relInfo(rel_name);
    if (!r || !r->rfile)
        return FAILURE;
    fclose(r->rfile);
    r->rfile = NULL;
    db_info.rel_status = DB_CLOSE;
    for (int i = 0; i < db_info.num_rel; i++)
        if (db_info.rinfo[i]->rfile != NULL)
        {
            db_info.rel_status = DB_OPEN;
            break;
        }
    return SUCCESS;
}