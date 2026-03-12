#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdbool.h>
#include "pds4.h"

// Record type used in all tests
typedef struct {
    int  hospital_id;
    char name[100];
    char address[200];
    char email[50];
} Hospital;

#define TREPORT(a1,a2) do { printf("Status: %s - %s\n\n", a1, a2); fflush(stdout); } while(0)

void process_line(char *line);

int main(int argc, char *argv[]) {
    if (argc != 2) { fprintf(stderr, "Usage: %s testfile\n", argv[0]); exit(1); }
    FILE *f = fopen(argv[1], "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", argv[1]); exit(1); }
    char line[1024];
    while (fgets(line, sizeof(line)-1, f)) {
        if (!strcmp(line,"\n") || !strcmp(line,"")) continue;
        process_line(line);
    }
    fclose(f);
    return 0;
}

void process_line(char *line) {
    char command[30], info[1024];
    int  expected_status, status;
    Hospital h, out;

    sscanf(line, "%s", command);
    printf("Test case: %s", line); fflush(stdout);

    /* ------------------------------------------------------------------ */
    if (!strcmp(command, "CREATE")) {
        /*  CREATE <dbname> <ntables> <t1> <rs1> <t2> <rs2> ... <expected> */
        char dbname[100];
        int  ntables;
        sscanf(line, "%*s %s %d", dbname, &ntables);

        char  *tnames[32];
        int    rsizes[32];
        char   namebuf[32][100];
        /* parse pairs after dbname and ntables */
        char *p = line;
        /* skip command, dbname, ntables */
        p = strchr(p,' ')+1; p = strchr(p,' ')+1; p = strchr(p,' ')+1;
        for (int i = 0; i < ntables; i++) {
            sscanf(p, "%s %d", namebuf[i], &rsizes[i]);
            tnames[i] = namebuf[i];
            p = strchr(p,' ')+1; p = strchr(p,' ')+1;
        }
        sscanf(p, "%d", &expected_status);

        status = db_create(dbname, tnames, rsizes, ntables);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"db_create returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "OPEN")) {
        /* OPEN <dbname> <expected> */
        char dbname[100];
        sscanf(line, "%*s %s %d", dbname, &expected_status);
        status = db_open(dbname);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"db_open returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "CLOSE")) {
        /* CLOSE <expected> */
        sscanf(line, "%*s %d", &expected_status);
        status = db_close();
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"db_close returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "STORE")) {
        /* STORE <tname> <key> <expected> <name> <addr> <email> */
        char tname[100]; int key;
        char name[100], addr[200], email[50];
        sscanf(line, "%*s %s %d %d %s %s %s", tname, &key, &expected_status, name, addr, email);
        h.hospital_id = key;
        strcpy(h.name, name); strcpy(h.address, addr); strcpy(h.email, email);
        status = table_store(tname, key, &h);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"table_store returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "SEARCH")) {
        /* SEARCH <tname> <key> <expected> [name addr email] */
        char tname[100]; int key;
        sscanf(line, "%*s %s %d %d", tname, &key, &expected_status);
        status = table_get(tname, key, &out);
        if (status != expected_status) {
            sprintf(info,"table_get returned %d", status); TREPORT("FAIL",info);
        } else if (status == SUCCESS) {
            char name[100], addr[200], email[50];
            sscanf(line, "%*s %*s %*d %*d %s %s %s", name, addr, email);
            if (out.hospital_id == key && !strcmp(out.name, name))
                TREPORT("PASS","");
            else { sprintf(info,"data mismatch: got id=%d name=%s", out.hospital_id, out.name); TREPORT("FAIL",info); }
        } else TREPORT("PASS","");
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "UPDATE")) {
        /* UPDATE <tname> <key> <expected> <name> <addr> <email> */
        char tname[100]; int key;
        char name[100], addr[200], email[50];
        sscanf(line, "%*s %s %d %d %s %s %s", tname, &key, &expected_status, name, addr, email);
        h.hospital_id = key;
        strcpy(h.name, name); strcpy(h.address, addr); strcpy(h.email, email);
        status = table_update(tname, key, &h);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"table_update returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "DELETE")) {
        /* DELETE <tname> <key> <expected> */
        char tname[100]; int key;
        sscanf(line, "%*s %s %d %d", tname, &key, &expected_status);
        status = table_delete(tname, key);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"table_delete returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "UNDELETE")) {
        /* UNDELETE <tname> <key> <expected> */
        char tname[100]; int key;
        sscanf(line, "%*s %s %d %d", tname, &key, &expected_status);
        status = table_undelete(tname, key);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"table_undelete returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "SEARCH_FIELD")) {
        /*
         * SEARCH_FIELD <tname> <field> <value> <expected> [key name addr email]
         * field: name | address | email | hospital_id
         */
        char tname[100], field[50], value[200];
        sscanf(line, "%*s %s %s %s %d", tname, field, value, &expected_status);

        int offset, fsize;
        if (!strcmp(field, "name"))        { offset = offsetof(Hospital,name);    fsize = 0; }
        else if (!strcmp(field, "address")){ offset = offsetof(Hospital,address); fsize = 0; }
        else if (!strcmp(field, "email"))  { offset = offsetof(Hospital,email);   fsize = 0; }
        else { /* hospital_id — numeric */
            int ival; sscanf(value, "%d", &ival);
            offset = offsetof(Hospital, hospital_id);
            fsize  = sizeof(int);
            status = table_search_field(tname, offset, fsize, &ival, &out);
            goto search_field_done;
        }
        status = table_search_field(tname, offset, fsize, value, &out);

        search_field_done:
        if (status != expected_status) {
            sprintf(info,"table_search_field returned %d", status); TREPORT("FAIL",info);
        } else if (status == SUCCESS) {
            int ekey; char ename[100], eaddr[200], eemail[50];
            sscanf(line, "%*s %*s %*s %*s %*d %d %s %s %s", &ekey, ename, eaddr, eemail);
            if (out.hospital_id == ekey && !strcmp(out.name, ename))
                TREPORT("PASS","");
            else { sprintf(info,"mismatch: got id=%d name=%s", out.hospital_id, out.name); TREPORT("FAIL",info); }
        } else TREPORT("PASS","");
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_CREATE")) {
        /* REL_CREATE <relname> <t1> <t2> <expected> */
        char relname[200], t1[100], t2[100];
        sscanf(line, "%*s %s %s %s %d", relname, t1, t2, &expected_status);
        status = create_rel(relname, t1, t2);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"create_rel returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_OPEN")) {
        /* REL_OPEN <relname> <expected> */
        char relname[200];
        sscanf(line, "%*s %s %d", relname, &expected_status);
        status = open_rel(relname);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"open_rel returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_STORE")) {
        /* REL_STORE <relname> <prikey> <relkey> <expected> */
        char relname[200]; int prikey, relkey;
        sscanf(line, "%*s %s %d %d %d", relname, &prikey, &relkey, &expected_status);
        status = store_rel(relname, prikey, relkey);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"store_rel returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_SEARCH")) {
        /* REL_SEARCH <relname> <prikey> <expected> [relkey name addr email] */
        char relname[200]; int prikey;
        sscanf(line, "%*s %s %d %d", relname, &prikey, &expected_status);
        status = get_rel(relname, &out, prikey);
        if (status != expected_status) {
            sprintf(info,"get_rel returned %d", status); TREPORT("FAIL",info);
        } else if (status == SUCCESS) {
            int ekey; char ename[100], eaddr[200], eemail[50];
            sscanf(line, "%*s %*s %*d %*d %d %s %s %s", &ekey, ename, eaddr, eemail);
            if (out.hospital_id == ekey && !strcmp(out.name, ename))
                TREPORT("PASS","");
            else { sprintf(info,"mismatch: got id=%d name=%s", out.hospital_id, out.name); TREPORT("FAIL",info); }
        } else TREPORT("PASS","");
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_DELETE")) {
        /* REL_DELETE <relname> <key> <pri|rel> <expected> */
        char relname[200], side[10]; int key;
        sscanf(line, "%*s %s %d %s %d", relname, &key, side, &expected_status);
        bool is_primary = !strcmp(side, "pri");
        status = delete_rel(relname, key, is_primary);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"delete_rel returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_UNDELETE")) {
        /* REL_UNDELETE <relname> <key> <pri|rel> <expected> */
        char relname[200], side[10]; int key;
        sscanf(line, "%*s %s %d %s %d", relname, &key, side, &expected_status);
        bool is_primary = !strcmp(side, "pri");
        status = undelete_rel(relname, key, is_primary);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"undelete_rel returned %d", status); TREPORT("FAIL",info); }
    }
    /* ------------------------------------------------------------------ */
    else if (!strcmp(command, "REL_CLOSE")) {
        /* REL_CLOSE <relname> <expected> */
        char relname[200];
        sscanf(line, "%*s %s %d", relname, &expected_status);
        status = close_rel(relname);
        if (status == expected_status) TREPORT("PASS","");
        else { sprintf(info,"close_rel returned %d", status); TREPORT("FAIL",info); }
    }
}
