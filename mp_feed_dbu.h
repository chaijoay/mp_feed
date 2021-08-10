///
///
/// FACILITY    : db utility for rating and mapping of ir cdr
///
/// FILE NAME   : mp_feed_dbu.h
///
/// AUTHOR      : Thanakorn Nitipiromchai
///
/// CREATE DATE : 04-Feb-2021
///
/// CURRENT VERSION NO : 1.0
///
/// LAST RELEASE DATE  : 04-Feb-2021
///
/// MODIFICATION HISTORY :
///     1.0         04-Feb-2021     First Version
///
///
#ifndef __MP_FEED_DBU_H__
#define __MP_FEED_DBU_H__

#ifdef  __cplusplus
    extern "C" {
#endif

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <stdlib.h>
#include <sqlite3.h>

// #include <sqlca.h>
// #include <sqlda.h>
// #include <sqlcpr.h>

#include "strlogutl.h"
#include "glb_str_def.h"


#define NOT_FOUND               1403
#define FETCH_NULL              1405
#define KEY_DUP                 -1
#define DEADLOCK                -60
#define FETCH_OUTOFSEQ          -1002

#define SIZE_GEN_STR            100
#define SIZE_SQL_STR            1024


//int   connAllDb(char *szFrmUsr, char *szFrmPwd, char *szFrmSvr, char *szSffUsr, char *szSffPwd, char *szSffSvr);
int   connectDbErm(char *szDbUsr, char *szDbPwd, char *szDbSvr, int nRetryCnt, int nRetryWait);
//void  discAllDb();
void  disconnErm(char *dbsvr);

int   loadFruadLov(sqlite3 **db);



#ifdef  __cplusplus
    }
#endif

#endif
