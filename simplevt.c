// Simplevt.c
// (c) Martin Thomas 2010

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <assert.h>
// helpful macros
#define DP  printf("simplevt: %s [line %d]\n", __FUNCTION__, __LINE__);

#define MAX_MAP_SIZE 1000000

typedef struct simplevt_vtab simplevt_vtab;
typedef struct simplevt_cursor simplevt_cursor;
typedef struct simplevt_col simplevt_col;

/* simplevt column object */
struct simplevt_col {
	char *filename;
	char type;
	void *last_mmap;
	off_t mmap_size;
	off_t mmap_pos;
};

/* A simplevt table object */
struct simplevt_vtab {
  sqlite3_vtab base;
  sqlite3 *db;
  int filecount;
  int max_rowid;
  simplevt_col** cols;
};

/* A simplevt table cursor object */
struct simplevt_cursor {
  sqlite3_vtab_cursor base;
  int rowid;
  int *filedesc;
  int lastfd; /* last fd used */
  void* last_mmap;
  off_t mmap_size;
  off_t mmap_pos;
  int max_rowid; // this should be same as table max_rowid until columns can be updated
  int metafd;
  FILE* lasttext;
};


char inferType(char *tmp)
{
	DP;
	// find column type // allowable - text or integer or blank (default to integer)
    char **words = sqlite3_malloc(sizeof (char*) * 3); //
    char result = 'i';
    int wordcount = 0;
    while(strlen(tmp) && wordcount < 3){
        words[wordcount++] = strsep(&tmp, " ");
    }
    if(wordcount == 2){
        if (strcasestr(words[1], "text"))
        	result = 't';
        else result = 'i';
    }
    sqlite3_free(words);
    printf("Parm: %s : %c\n", words[1], result);
    assert(result=='i' || result=='t');
    return result;
}


void constructMetaData(int fcnt, simplevt_col** cols)
{
	DP;
	int idx;
	printf ("Fcnt: %i\n", fcnt);
	for	(idx=0; idx < fcnt; idx++){
		printf ("type: %c\n", cols[idx]->type);
		assert( (cols[idx]->type == 't') || (cols[idx]->type =='i'));
		if (cols[idx]->type == 't'){
			printf ("colidx: %i\n", idx);
			//create metadata file and write
			//scan lines
			FILE* ifile = fopen(cols[idx]->filename, "r");
			char* metafilename = strdup(cols[idx]->filename);
			strcat(metafilename, ".meta");
			//for now we look to see if the metadata file already exists.. later check if it is newer than the text file
			struct stat rest;
			int stat_result = stat(metafilename, &rest);
			if (-1 == stat_result){
				perror("Stat");
				FILE* ofile = fopen(metafilename, "w");
				int linecnt = 0;
				int conv = 0;
				char noddy[4096];
				do
				{
					int start = ftell(ifile);
					conv = fscanf(ifile, "%[:;!?a-zA-Z0-9.,/ -]", noddy);
					assert(conv);
					assert(strlen(noddy));
					if (1 == conv){
						linecnt++;
						fwrite(&start, sizeof(start), 1, ofile);
						printf ("idx: %i, %s\n", start, noddy);
					}
	//				printf("%s\n", noddy);
					fseek(ifile, 1, SEEK_CUR);
				} while (!feof(ifile) && conv == 1);
				printf("Line count: %i\n", linecnt);

				fclose(ofile);
			} else {
				printf ("File exists\n");
			}
			fclose(ifile);
			free(metafilename);
		}
	}
}

/*
 * Build schema string for use in connect and create
 * Call sqlite3_declare_vtab and return result
 */
int constructSchema(sqlite3 *db, int argc, const char *const*argv, struct simplevt_col** cols, int *fcnt){
  DP;
  char *schema;
  
  schema = sqlite3_malloc(4096);
  memset(schema, 0 , 4096);

  int idx;
  /* build the schema string.. remove extra syntax for virtual table*/

  strcat(schema, "CREATE TABLE ");
  strcat(schema, argv[2]);
 
  if (argc > 3){
    strcat(schema, " (");
    for (idx = 3; idx < argc; idx++){
      char *tmp = index(argv[idx], ':');
      if (tmp){
    	  // create a new col object and populate
    	  cols[*fcnt] = sqlite3_malloc(sizeof(simplevt_col));
    	  cols[*fcnt]->filename = sqlite3_mprintf("%s", tmp+1);
    	  printf ("Fname: %s\n", tmp+1);

    	  tmp -=4 ;
    	  if (argv[idx]<= tmp || *tmp == 'f')
    		  *tmp = 0; // chop off end of string
		  tmp = argv[idx];
		  cols[*fcnt]->type = inferType(tmp);
		  assert(cols[*fcnt]->type=='i' || cols[*fcnt]->type=='t');
		  (*fcnt)++;
	  }

      strcat(schema, argv[idx]);
      if (idx != argc - 1)
    	strcat(schema, ", ");
    }
    strcat(schema, ")" );
  }
  printf ("Schema: %s\n", schema);
  int rc = sqlite3_declare_vtab(db, schema);
  if (rc != SQLITE_OK){
    printf("Simplevt error: %s\n", sqlite3_errmsg(db));
  }
 
  sqlite3_free(schema);
  return rc;
}

/*
 * Create
 */
int simplevtCreate(sqlite3 *db, void *pAux, int argc, const char * const *argv,
		sqlite3_vtab **ppVTab, char **pzErr)
{
	DP; //debug print
	int rc = SQLITE_ERROR;
	pzErr = 0;
	struct simplevt_col** cols = sqlite3_malloc(sizeof(simplevt_col*) * 32);
	int fcnt = 0;

	rc = constructSchema(db, argc, argv, cols, &fcnt);
	printf("Fname count: %i\n", fcnt);
	int idx = 0;
	if (rc == SQLITE_OK)
	{
		simplevt_vtab *vtab = sqlite3_malloc(sizeof(simplevt_vtab));
		if (vtab)
		{
			*ppVTab = (sqlite3_vtab*) vtab;
			memset(vtab, 0, sizeof(simplevt_vtab));
			vtab->filecount = fcnt;
			vtab->cols = cols;

			constructMetaData(fcnt, cols);
			// now find length of first col
			char *tmpfname = strdup(cols[0]->filename);
			if (cols[0]->type == 't')
				strcat(tmpfname, ".meta");
			int fd = open(tmpfname, O_RDONLY);
			if (fd < 0)
				perror("fopen");
			int end = lseek(fd, 0, SEEK_END);
			if (end < 0)
				perror("lseek");
			else
				vtab->max_rowid = end/sizeof(int);

			close(fd);
		}
	}
	return rc;

}


int simplevtConnect(sqlite3 *db, void *pAux,
                 int argc, const char *const*argv,
                 sqlite3_vtab **ppVTab,
		    char **pzErr){
  DP;
  simplevtCreate(db, pAux, argc, argv, ppVTab, pzErr);
}

char* mapConstraintToName(int constraint){
	DP;
	switch (constraint) {
	case SQLITE_INDEX_CONSTRAINT_EQ:
		return "equal";
		break;
	case SQLITE_INDEX_CONSTRAINT_GT:
		return "greater than";
		break;
	case SQLITE_INDEX_CONSTRAINT_LE:
		return "less than or equal";
		break;
	case SQLITE_INDEX_CONSTRAINT_LT:
		return "less than";
		break;
	case SQLITE_INDEX_CONSTRAINT_GE:
		return "greater than or equal";
		break;
	case SQLITE_INDEX_CONSTRAINT_MATCH:
		return "match";
		break;
		default:
			return "";
			break;
	}

}

int simplevtBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info* info){
  DP; //debug print
  int rc = SQLITE_OK;
  printf("Constraint count: %i\n", info->nConstraint);
  int idx;
  for (idx =0; idx < info->nConstraint; idx++){
    if (info->aConstraint[idx].usable){
      printf("Constraint %i on col %i is '%s'\n", idx, info->aConstraint[idx].iColumn, mapConstraintToName(info->aConstraint[idx].op));
      info->aConstraintUsage->argvIndex = info->aConstraint[idx].iColumn+1;
    }
  }
  printf("Orderby count: %i\n", info->nOrderBy);

  return rc;
}


int simplevtDisconnect(sqlite3_vtab *pVTab){
  DP; //debug print
  int rc = SQLITE_ERROR;
  return rc;
}


int simplevtDestroy(sqlite3_vtab *pVTab){
  DP; //debug print
  int rc = SQLITE_OK;
  sqlite3_free(pVTab);
  return rc;
}

/*
 * create a simplevt cursor
 */
int simplevtOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  DP; //debug print
  simplevt_cursor *pCur;
  int rc = SQLITE_NOMEM;

  pCur = sqlite3_malloc(sizeof(simplevt_cursor));
  if (!pCur)
	  goto end;

  memset(pCur, 0, sizeof(simplevt_cursor));
  int idx;
  simplevt_vtab *tab = (simplevt_vtab*)pVTab;
  int *fds = malloc(sizeof(int));
  memset(fds, 0, tab->filecount);
  pCur->filedesc = fds;

  *ppCursor = (sqlite3_vtab_cursor *)pCur;
  rc = SQLITE_OK;

  end:
  return rc;
}

/*
 * open column files and store in svt cursor struct on demand
 */
int openColumn(sqlite3_vtab_cursor* cur, int colidx){
	//TODO
	// prepopulate fd array with nulls
	// check if fd array has fd for column of interest
	DP;// debug print
	int rc = SQLITE_OK;
	int fd;
	simplevt_vtab *tab = (simplevt_vtab*)(cur->pVtab);
	simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
	//printf("[%s] fd[%i]: %i filename %s\n ", __FUNCTION__, colidx, svt_cur->filedesc[colidx], tab->filenames[colidx]);
	fd = open(tab->cols[colidx]->filename, O_RDONLY);
	if (fd != -1){
		svt_cur->filedesc[colidx] = fd;
		printf("New fd: %i for col %i\n", svt_cur->filedesc[colidx], colidx);
	} else {
		perror("Column file open");
		rc = SQLITE_ERROR;
	}

	if (tab->cols[colidx]->type == 't'){
		printf("Need meta\n");
		char* metafilename = strdup(tab->cols[colidx]->filename);
		strcat(metafilename, ".meta");
		fd = open(metafilename, O_RDONLY);
		if (-1 != fd){
			svt_cur->metafd = fd;
			printf("New meta fd: %i for col %i\n", fd, colidx);
		} else perror ("meta fail");
		free(metafilename);
	}
	printf("[%s] fd[%i]: %i filename %s\n ", __FUNCTION__, colidx, svt_cur->filedesc[colidx], tab->cols[colidx]->filename);
	return rc;
}


int simplevtClose(sqlite3_vtab_cursor* cur){
	//DP; //debug print
	int rc = SQLITE_OK;

	int idx;
	simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
	simplevt_vtab *tab = (simplevt_vtab*)(cur->pVtab);
	if (svt_cur->lastfd && svt_cur->last_mmap){
		if(munmap(svt_cur->last_mmap, svt_cur->mmap_size)){
			perror("Munmap");
		}
	}
	for(idx=0; idx < tab->filecount; idx++){
		int fd;
		if (svt_cur->filedesc[idx]){
//			printf("Fd[%i] %i\n", idx, svt_cur->filedesc[idx]);
			fd = close(svt_cur->filedesc[idx]);
			if (fd == -1){
				perror("Column file close");
			} else {
				svt_cur->filedesc[idx] = 0;
			}
		}
	}
  if (svt_cur->metafd)
	  close(svt_cur->metafd);
  sqlite3_free(cur);
  return rc;
}

int simplevtFilter(sqlite3_vtab_cursor* cur, int idxNum, const char *idxStr,
		   int argc, sqlite3_value **argv){
  DP; //debug print
  int rc = SQLITE_OK;
  if (argc){
    printf("Argc: %i\n", argc);
  }
  simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
  svt_cur->rowid = 0;
  return rc;
}


int simplevtNext(sqlite3_vtab_cursor *cur){
  //DP; // debug print
  int rc = SQLITE_OK;
  simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
  svt_cur->rowid++;
  return rc;
}


int simplevtEof(sqlite3_vtab_cursor *cur){
  //DP; // debug print
  //int rc = SQLITE_ERROR;
  simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
  simplevt_vtab *tab = (simplevt_vtab*)(cur->pVtab);
  return svt_cur->rowid >= tab->max_rowid;

//	off_t pos = lseek(svt_cur->lastfd, 0, SEEK_CUR);
//    off_t end = lseek(svt_cur->lastfd, 0, SEEK_END);
//    //printf("pos, end: %i %i\n", pos, end);
//    //return (pos >= end);
//    return (svt_cur->rowid*sizeof(int) > end);

}


int simplevtColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx , int colid){
//	DP; // debug print

	int rc = SQLITE_ERROR;
	int intres;
	simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
	//printf("Colid %i Rowid: %i\n", colid, svt_cur->rowid);
	simplevt_vtab *tab = (simplevt_vtab*)(cur->pVtab);
	//char **fnames = tab->filenames;

	if (colid >= tab->filecount)
		goto end;

	if (!tab->cols[colid]->filename)
		goto end;

	if (tab->cols[colid]->type != 'i' && tab->cols[colid]->type != 't')
		goto end;

	if (!svt_cur->filedesc[colid]){
		rc = openColumn(cur, colid);
		if (rc != SQLITE_OK)
			goto end;
		printf("Col %i: %s\n", colid, tab->cols[colid]->filename);

		if(tab->cols[colid]->type == 't'){
			svt_cur->lasttext = fdopen(svt_cur->filedesc[colid], "r");
			if (!svt_cur->lasttext){
				perror("Fdopen");
				goto end;
			} else {
				printf("Set lasttext\n");
			}
		}
	}

	if (tab->cols[colid]->type == 'i'){
		if (svt_cur->lastfd != svt_cur->filedesc[colid]){
			if(svt_cur->last_mmap && svt_cur->lastfd){
				if(munmap(svt_cur->last_mmap, svt_cur->mmap_size)){
					perror("Munmap");
				}
			}
			svt_cur->last_mmap = mmap(0, MAX_MAP_SIZE*sizeof(int), PROT_READ, MAP_FILE|MAP_SHARED, svt_cur->filedesc[colid], 0 /*offset*/);
			printf("Mmap (fd: %i) rowid: %i\n", svt_cur->filedesc[colid/////], svt_cur->rowid);

			if (svt_cur->last_mmap == MAP_FAILED){
				svt_cur->last_mmap = 0;
				svt_cur->lastfd = 0;
				perror("Map:");
				goto end;
			}
			svt_cur->mmap_size = MAX_MAP_SIZE;
			svt_cur->mmap_pos = sizeof(int)*svt_cur->rowid;
			svt_cur->lastfd = svt_cur->filedesc[colid];
		}
		int val = ((int*)(svt_cur->last_mmap))[svt_cur->rowid];
		sqlite3_result_int(ctx, val);
	} else {
//		printf("Need row: %i\n", svt_cur->rowid);
		int val = 0;
		if (-1 == lseek(svt_cur->metafd, sizeof(val)*svt_cur->rowid, SEEK_SET)){
			perror("Text idx");
			goto end;
		}
		int res = read(svt_cur->metafd, &val, sizeof(val) );
		if (-1 == res){
			perror("Text idx2");
			goto end;
		} else if(!res){
			perror("Text idx2 EOF");
			goto end;
		}

//		printf ("ptr: %p\n", val);
		if (-1==fseek(svt_cur->lasttext, val, SEEK_SET)){
			perror("Text read");
			goto end;
		}

		// read text data from binary file. Dunno how long the string is..
		char *noddy = sqlite3_malloc(1024);
		int conv;

		conv = fscanf (svt_cur->lasttext, "%[:;!?a-zA-Z0-9.,/ -]", noddy);
		//printf("Tell: %lu Len: %lu [%s]\n", ftell(svt_cur->lasttext), strlen(noddy), noddy);

		if (conv)
			sqlite3_result_text(ctx, noddy, strlen(noddy), SQLITE_STATIC);

	}
	rc = SQLITE_OK;


	end:
	return rc;
}


int simplevtRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  DP; // debug print
  simplevt_cursor *svt_cur = (simplevt_cursor*)cur;
  //printf("-Rowid: %i\n", svt_cur->rowid);
  *pRowid = svt_cur->rowid;
  int rc = SQLITE_OK;
  return rc;
}


int simplevtUpdate(sqlite3_vtab *vtab, int intval, sqlite3_value **sqlval, sqlite_int64 *bigint){
  DP; // debug print
  int rc = SQLITE_ERROR;
  return rc;
}


int simplevtBegin(sqlite3_vtab *pVTab){
  DP; // debug print
  int rc = SQLITE_ERROR;
  return rc;
}

int simplevtSync(sqlite3_vtab *pVTab){
  DP; // debug print
  int rc = SQLITE_ERROR;

  rc = SQLITE_OK;
  return rc;
}

int simplevtCommit(sqlite3_vtab *pVTab){
  DP; // debug print
  int rc = SQLITE_ERROR;
  return rc;
}

int simplevtRollback(sqlite3_vtab *pVTab){
  DP; // debug print
  int rc = SQLITE_ERROR;
  return rc;
}


int simplevtFindFunction(sqlite3_vtab *pVtab, int nArg, const char *zName,
                       void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
			 void **ppArg){
  DP; // debug print
  int rc = SQLITE_ERROR;
  return rc;
}


int simplevtRename(sqlite3_vtab *pVtab, const char *zNew){
  DP; // debug print
  int rc = SQLITE_ERROR;
  return rc;
}


/*
int (*xCreate)(sqlite3*, void *pAux,
                 int argc, char **argv,
                 sqlite3_vtab **ppVTab,
                 char **pzErr);
    int (*xConnect)(sqlite3*, void *pAux,
                 int argc, char **argv,
                 sqlite3_vtab **ppVTab,
                 char **pzErr);
    int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*);
    int (*xDisconnect)(sqlite3_vtab *pVTab);
    int (*xDestroy)(sqlite3_vtab *pVTab);
    int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
    int (*xClose)(sqlite3_vtab_cursor*);
    int (*xFilter)(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                  int argc, sqlite3_value **argv);
    int (*xNext)(sqlite3_vtab_cursor*);
    int (*xEof)(sqlite3_vtab_cursor*);
    int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context*, int);
    int (*xRowid)(sqlite3_vtab_cursor*, sqlite_int64 *pRowid);
    int (*xUpdate)(sqlite3_vtab *, int, sqlite3_value **, sqlite_int64 *);
    int (*xBegin)(sqlite3_vtab *pVTab);
    int (*xSync)(sqlite3_vtab *pVTab);
    int (*xCommit)(sqlite3_vtab *pVTab);
    int (*xRollback)(sqlite3_vtab *pVTab);
    int (*xFindFunction)(sqlite3_vtab *pVtab, int nArg, const char *zName,
                       void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
                       void **ppArg);
    int (*Rename)(sqlite3_vtab *pVtab, const char *zNew);
*/


void destroy() {
  DP //debug print
}


static const sqlite3_module simplevtModule = {
  /* iVersion      */ 0,
  /* xCreate       */ simplevtCreate,
  /* xConnect      */ simplevtCreate,
  /* xBestIndex    */ simplevtBestIndex,
  /* xDisconnect   */ simplevtDisconnect,
  /* xDestroy      */ simplevtDestroy,
  /* xOpen         */ simplevtOpen,
  /* xClose        */ simplevtClose,
  /* xFilter       */ simplevtFilter,
  /* xNext         */ simplevtNext,
  /* xEof          */ simplevtEof,
  /* xColumn       */ simplevtColumn,
  /* xRowid        */ simplevtRowid,
  /* xUpdate       */ simplevtUpdate,
  /* xBegin        */ simplevtBegin,
  /* xSync         */ simplevtSync,
  /* xCommit       */ simplevtCommit,
  /* xRollback     */ simplevtRollback,
  /* xFindFunction */ 0,
  /* xRename       */ simplevtRename
};


/* SQLite invokes this routine once when it loads the extension.
** Create new functions, collating sequences, and virtual table
** modules here.  This is usually the only exported symbol in
** the shared library.
*/
int sqlite3_extension_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi)
    sqlite3_create_module_v2(db, "simplevt", &simplevtModule, 0, &destroy);
  return 0;
}
