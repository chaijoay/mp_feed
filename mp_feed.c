///
/// FACILITY    : reformat and prepare mpay transaction (MPTRANS_CC, Supper Duper) for eFIT
///
/// FILE NAME   : mp_feed.c
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
#define _XOPEN_SOURCE           700         // Required under GLIBC for nftw()
#define _POSIX_C_SOURCE         200809L
#define _XOPEN_SOURCE_EXTENDED  1

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <dirent.h>
#include <libgen.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sqlite3.h>

#include "minIni.h"
#include "procsig.h"
#include "mp_feed.h"
#include "strlogutl.h"
#include "mp_feed_dbu.h"

#define  TMPSUF     "._tmp"

char gszAppName[SIZE_ITEM_S];
char gszIniFile[SIZE_FULL_NAME];
char gszToday[SIZE_DATE_ONLY+1];
char gszOutFname[SIZE_ITEM_L];

char    *pbuf_rec[SIZE_BUFF];
char    *pbuf_idd[SIZE_ITEM_S];

ST_OUT_COMMON    gOutCommon;
FILE    *gfpSnap;
FILE    *gfpState;
int     gnSnapCnt;
int     gnLenPreTxcc;
int     gnLenSufTxcc;
int     gnLenPreSpdp;
int     gnLenSufSpdp;

short   gnFileSeq = 0;
time_t  gzLastTimeT = 0;
int     gnPrcId;
int     gnIddCnt = 0;

const char gszIniStrSection[E_NOF_SECTION][SIZE_ITEM_T] = {
    "INPUT",
    "OUTPUT",
    "MAPPING",
    "COMMON",
    "DB_CONNECTION"
};

const char gszIniStrInput[E_NOF_PAR_INPUT][SIZE_ITEM_T] = {
    "TXCC_INPUT_DIR",
    "TXCC_FILE_PREFIX",
    "TXCC_FILE_SUFFIX",
    "TXCC_BACKUP",
    "SPDP_INPUT_DIR",
    "SPDP_FILE_PREFIX",
    "SPDP_FILE_SUFFIX",
    "SPDP_BACKUP"
};

const char gszIniStrOutput[E_NOF_PAR_OUTPUT][SIZE_ITEM_T] = {
    "BASE_OUTPUT_DIR",
    "NOF_OUTPUT_DIR",
    "OUT_FILE_PREFIX",
    "OUT_FILE_SUFFIX"
};

const char gszIniStrMap[E_NOF_PAR_MAPSVC][SIZE_ITEM_T] = {
    "SVCID_DIR",
    "SVCID_FILE_PREFIX",
    "SVCID_FILE_SUFFIX",
    "SVCID_PURGE_DAY"
};

const char gszIniStrCommon[E_NOF_PAR_COMMON][SIZE_ITEM_T] = {
    "REJ_INVALID",
    "REJ_OUT_DIR",
    "TMP_DIR",
    "BACKUP_DIR",
    "STATE_DIR",
    "KEEP_STATE_DAY",
    "SKIP_OLD_FILE",
    "LOG_DIR",
    "LOG_LEVEL",
    "SLEEP_SECOND"
};

const char gszIniStrDbConn[E_NOF_PAR_DBCONN][SIZE_ITEM_T] = {
    "ERM_USER_NAME",
    "ERM_PASSWORD",
    "ERM_DB_SID"
};

char gszIniParInput[E_NOF_PAR_INPUT][SIZE_ITEM_L];
char gszIniParOutput[E_NOF_PAR_OUTPUT][SIZE_ITEM_L];
char gszIniParMap[E_NOF_PAR_MAPSVC][SIZE_ITEM_L];
char gszIniParCommon[E_NOF_PAR_COMMON][SIZE_ITEM_L];
char gszIniParDbConn[E_NOF_PAR_DBCONN][SIZE_ITEM_L];

sqlite3 *g_SqliteDb = NULL;
char    *g_SqliteErr_msg = NULL;
char    gAppPath[SIZE_ITEM_L];
char    gzErrMsg[SIZE_ITEM_L];
char    gsdb_file_path[SIZE_ITEM_L+30];

int main(int argc, char *argv[])
{
    FILE *ifp = NULL;
    gfpState = NULL;
    char szSnap[SIZE_ITEM_L], snp_line[SIZE_BUFF];
    int retryBldSnap = 3, nInpFileCntDay = 0, nInpFileCntRnd = 0;
    time_t t_bat_start = 0, t_bat_stop = 0;

    memset(gszAppName, 0x00, sizeof(gszAppName));
    memset(gszIniFile, 0x00, sizeof(gszIniFile));
    memset(gszToday, 0x00, sizeof(gszToday));

    // 1. read ini file
    if ( readConfig(argc, argv) != SUCCESS ) {
        return EXIT_FAILURE;
    }

    if ( procLock(gszAppName, E_CHK) != SUCCESS ) {
        fprintf(stderr, "another instance of %s is running\n", gszAppName);
        return EXIT_FAILURE;
    }

    if ( handleSignal() != SUCCESS ) {
        fprintf(stderr, "init handle signal failed: %s\n", getSigInfoStr());
        return EXIT_FAILURE;
    }

    if ( startLogging(gszIniParCommon[E_LOG_DIR], gszAppName, atoi(gszIniParCommon[E_LOG_LEVEL])) != SUCCESS ) {
       return EXIT_FAILURE;
    }

    if ( validateIni() == FAILED ) {
        return EXIT_FAILURE;
    }
    logHeader();

    char inp_file[SIZE_ITEM_L]; memset(inp_file, 0x00, sizeof(inp_file));
    char inp_type[10];          memset(inp_type, 0x00, sizeof(inp_type));
    long cont_pos = 0L;

    cont_pos = checkPoint(NULL, inp_file, inp_type, gszIniParCommon[E_TMP_DIR], gszAppName, E_CHK);

    strcpy(gszToday, getSysDTM(DTM_DATE_ONLY));

    // Main processing loop
    while ( TRUE ) {

        procLock(gszAppName, E_SET);

        if ( isTerminated() == TRUE ) {
            break;
        }

        // main process flow:
        // 1. build snapshot file -> list all files to be processed.
        // 2. connect to dbs (and also retry if any)
        // 3. recognise and reformat file according to source type (TAP, NRT or SCP)
        // 4. rating and fill additional field to complete common format
        // 5. write out common rated common format to further merging process
        // 6. disconnect from dbs to release resouce then give a process to rest (sleep)
        // 7. start over from step 1
        gnSnapCnt = 0;
        memset(szSnap, 0x00, sizeof(szSnap));
        sprintf(szSnap, "%s/%s.snap", gszIniParCommon[E_TMP_DIR], gszAppName);
        if ( cont_pos <= 0 ) {  // skip buildsnap if it need to process from last time check point
            if ( buildSnapFile(szSnap) != SUCCESS ) {
                if ( --retryBldSnap <= 0 ) {
                    fprintf(stderr, "retry build snap exceeded\n");
                    break;
                }
                sleep(10);
                continue;
            }
            retryBldSnap = 3;
            // check snap against state file
            gnSnapCnt = chkSnapVsState(szSnap);
        }
        if ( gnSnapCnt < 0 ) {
            writeLog(LOG_SYS, "problem found at chkSnapVsState (%s, %d, %d)", __func__, __LINE__, gnSnapCnt);
            break;  // There are some problem in reading state file
        }

        if ( gnSnapCnt > 0 || cont_pos > 0 ) {

            if ( manageMapTab() == FAILED ) {
                break;
            }

            if ( (ifp = fopen(szSnap, "r")) == NULL ) {
                writeLog(LOG_SYS, "unable to open %s for reading (%s)", szSnap, strerror(errno));
                break;
            }
            else {

                if ( cont_pos > 0 ) {   // continue from last time first
                    writeLog(LOG_INF, "continue process %s from last time", inp_file);
                    procSynFiles(dirname(inp_file), basename(inp_file), inp_type, cont_pos);
                    cont_pos = 0;
                    continue;           // back to build snap to continue normal loop
                }

                nInpFileCntRnd = 0;
                t_bat_start = time(NULL);
                while ( fgets(snp_line, sizeof(snp_line), ifp) ) {

                    if ( isTerminated() == TRUE ) {
                        break;
                    }

                    trimStr((unsigned char*)snp_line);  // snap record format => <path>|<filename>
                    char sdir[SIZE_ITEM_M], sfname[SIZE_ITEM_M], inptype[10];
                    memset(sdir, 0x00, sizeof(sdir));
                    memset(sfname, 0x00, sizeof(sfname));
                    memset(inptype, 0x00, sizeof(inptype));

                    getTokenItem(snp_line, 1, '|', inptype);
                    getTokenItem(snp_line, 2, '|', sdir);
                    getTokenItem(snp_line, 3, '|', sfname);

                    if ( ! olderThan(atoi(gszIniParCommon[E_SKIP_OLD_FILE]), sdir, sfname) ) {
                        procSynFiles(sdir, sfname, inptype, 0L);
                    }

                    nInpFileCntDay++;
                    nInpFileCntRnd++;
                }
                t_bat_stop = time(NULL);
                writeLog(LOG_INF, "total processed files for this round=%d round_time_used=%d sec", nInpFileCntRnd, (t_bat_stop - t_bat_start));

                fclose(ifp);

            }
        }

        if ( isTerminated() == TRUE ) {
            if ( gfpState != NULL ) {
                fclose(gfpState);
                gfpState = NULL;
            }
            break;
        }
        else {
            writeLog(LOG_INF, "sleep %s sec", gszIniParCommon[E_SLEEP_SEC]);
            sleep(atoi(gszIniParCommon[E_SLEEP_SEC]));
        }

        if ( strcmp(gszToday, getSysDTM(DTM_DATE_ONLY)) ) {
            if ( gfpState != NULL ) {
                fclose(gfpState);
                gfpState = NULL;
            }
            writeLog(LOG_INF, "total processed files for today=%d", nInpFileCntDay);
            strcpy(gszToday, getSysDTM(DTM_DATE_ONLY));
            clearOldState();
            manageLogFile();
            nInpFileCntDay = 0;
        }

    }

    procLock(gszAppName, E_CLR);
    //freeTab();
    writeLog(LOG_INF, "%s", getSigInfoStr());
    writeLog(LOG_INF, "------- %s %d process completely stop -------", _APP_NAME_, gnPrcId);
    stopLogging();

    return EXIT_SUCCESS;

}

int buildSnapFile(const char *snapfile)
{
    char cmd[SIZE_BUFF];
    gnSnapCnt = 0;

    gnLenPreTxcc = strlen(gszIniParInput[E_TXCC_FPREF]);
    gnLenSufTxcc = strlen(gszIniParInput[E_TXCC_FSUFF]);
    gnLenPreSpdp = strlen(gszIniParInput[E_SPDP_FPREF]);
    gnLenSufSpdp = strlen(gszIniParInput[E_SPDP_FSUFF]);

    // open snap file for writing
    if ( (gfpSnap = fopen(snapfile, "w")) == NULL ) {
        writeLog(LOG_SYS, "unable to open %s for writing: %s\n", snapfile, strerror(errno));
        return FAILED;
    }

    // recursively walk through directories and file and check matching
    if ( *gszIniParInput[E_TXCC_INP_DIR] != '\0' ) {
        writeLog(LOG_INF, "scaning sync file in directory %s", gszIniParInput[E_TXCC_INP_DIR]);
        if ( nftw(gszIniParInput[E_TXCC_INP_DIR], _chkTxccFile, 32, FTW_DEPTH) ) {
            writeLog(LOG_SYS, "unable to read path %s: %s\n", gszIniParInput[E_TXCC_INP_DIR], strerror(errno));
            fclose(gfpSnap);
            gfpSnap = NULL;
            return FAILED;
        }
    }

    // recursively walk through directories and file and check matching
    if ( *gszIniParInput[E_SPDP_INP_DIR] != '\0' ) {
        writeLog(LOG_INF, "scaning sync file in directory %s", gszIniParInput[E_SPDP_INP_DIR]);
        if ( nftw(gszIniParInput[E_SPDP_INP_DIR], _chkSpdpFile, 32, FTW_DEPTH) ) {
            writeLog(LOG_SYS, "unable to read path %s: %s\n", gszIniParInput[E_SPDP_INP_DIR], strerror(errno));
            fclose(gfpSnap);
            gfpSnap = NULL;
            return FAILED;
        }
    }

    fclose(gfpSnap);
    gfpSnap = NULL;

    // if there are sync files then sort the snap file
    if ( gnSnapCnt > 0 ) {
        memset(cmd, 0x00, sizeof(cmd));
        sprintf(cmd, "sort -T %s -u %s > %s.tmp 2>/dev/null", gszIniParCommon[E_TMP_DIR], snapfile, snapfile);
writeLog(LOG_DB3, "buildSnapFile cmd '%s'", cmd);
        if ( system(cmd) != SUCCESS ) {
            writeLog(LOG_SYS, "cannot sort file %s (%s)", snapfile, strerror(errno));
            sprintf(cmd, "rm -f %s %s.tmp", snapfile, snapfile);
            system(cmd);
            return FAILED;
        }
        sprintf(cmd, "mv %s.tmp %s 2>/dev/null", snapfile, snapfile);
writeLog(LOG_DB3, "buildSnapFile cmd '%s'", cmd);
        system(cmd);
    }
    else {
        writeLog(LOG_INF, "no input file");
    }

    return SUCCESS;

}

int _chkTxccFile(const char *fpath, const struct stat *info, int typeflag, struct FTW *ftwbuf)
{

    const char *fname = fpath + ftwbuf->base;
    int fname_len = strlen(fname);
    char path_only[SIZE_ITEM_L];

    if ( typeflag != FTW_F && typeflag != FTW_SL && typeflag != FTW_SLN )
        return 0;

    if ( strncmp(fname, gszIniParInput[E_TXCC_FPREF], gnLenPreTxcc) != 0 ) {
        return 0;
    }

    if ( strcmp(fname + (fname_len - gnLenSufTxcc), gszIniParInput[E_TXCC_FSUFF]) != 0 ) {
        return 0;
    }

    if ( !(info->st_mode & (S_IRUSR|S_IRGRP|S_IROTH)) ) {
        writeLog(LOG_WRN, "no read permission for %s skipped", fname);
        return 0;
    }

    memset(path_only, 0x00, sizeof(path_only));
    strncpy(path_only, fpath, ftwbuf->base - 1);

    gnSnapCnt++;
    fprintf(gfpSnap, "%s|%s|%s\n", gszIniParInput[E_TXCC_FPREF], path_only, fname);    // write snap output format -> <INP_TYPE>|<DIR>|<FILE>
    return 0;

}

int _chkSpdpFile(const char *fpath, const struct stat *info, int typeflag, struct FTW *ftwbuf)
{

    const char *fname = fpath + ftwbuf->base;
    int fname_len = strlen(fname);
    char path_only[SIZE_ITEM_L];

    if ( typeflag != FTW_F && typeflag != FTW_SL && typeflag != FTW_SLN )
        return 0;

    if ( strncmp(fname, gszIniParInput[E_SPDP_FPREF], gnLenPreSpdp) != 0 ) {
        return 0;
    }

    if ( strcmp(fname + (fname_len - gnLenSufSpdp), gszIniParInput[E_SPDP_FSUFF]) != 0 ) {
        return 0;
    }

    if ( !(info->st_mode & (S_IRUSR|S_IRGRP|S_IROTH)) ) {
        writeLog(LOG_WRN, "no read permission for %s skipped", fname);
        return 0;
    }

    memset(path_only, 0x00, sizeof(path_only));
    strncpy(path_only, fpath, ftwbuf->base - 1);

    gnSnapCnt++;
    fprintf(gfpSnap, "%s|%s|%s\n", gszIniParInput[E_SPDP_FPREF], path_only, fname);  // write snap output format -> <INP_TYPE>|<DIR>|<FILE>
    return 0;

}

int chkSnapVsState(const char *snap)
{
    char cmd[SIZE_BUFF];
    char tmp_stat[SIZE_ITEM_L], tmp_snap[SIZE_ITEM_L];
    FILE *fp = NULL;

    memset(tmp_stat, 0x00, sizeof(tmp_stat));
    memset(tmp_snap, 0x00, sizeof(tmp_snap));
    memset(cmd, 0x00, sizeof(cmd));

    sprintf(tmp_stat, "%s/tmp_%s_XXXXXX", gszIniParCommon[E_TMP_DIR], gszAppName);
    sprintf(tmp_snap, "%s/osnap_%s_XXXXXX", gszIniParCommon[E_TMP_DIR], gszAppName);
    mkstemp(tmp_stat);
    mkstemp(tmp_snap);

	// close and flush current state file, in case it's opening
	if ( gfpState != NULL ) {
		fclose(gfpState);
		gfpState = NULL;
	}

    // create state file of current day just in case there is currently no any state file.
    sprintf(cmd, "touch %s/%s_%s%s", gszIniParCommon[E_STATE_DIR], gszAppName, gszToday, STATE_SUFF);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);

    if ( chkStateAndConcat(tmp_stat) == SUCCESS ) {
        // sort all state files (<APP_NAME>_<PROC_TYPE>_<YYYYMMDD>.proclist) to tmp_stat file
        // state files format is <DIR>|<FILE_NAME>
        //sprintf(cmd, "sort -T %s %s/%s_*%s > %s 2>/dev/null", gszIniParCommon[E_TMP_DIR], gszIniParCommon[E_STATE_DIR], gszAppName, STATE_SUFF, tmp_stat);
        sprintf(cmd, "sort -T %s %s > %s.tmp 2>/dev/null", gszIniParCommon[E_TMP_DIR], tmp_stat, tmp_stat);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
        system(cmd);
    }
    else {
        unlink(tmp_stat);
        return FAILED;
    }

    // compare tmp_stat file(sorted all state files) with sorted first_snap to get only unprocessed new files list
    sprintf(cmd, "comm -23 %s %s.tmp > %s 2>/dev/null", snap, tmp_stat, tmp_snap);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);
    sprintf(cmd, "rm -f %s %s.tmp", tmp_stat, tmp_stat);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);

    sprintf(cmd, "mv %s %s", tmp_snap, snap);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);

    // get record count from output file (snap)
    memset(tmp_stat, 0x00, sizeof(tmp_stat));
    sprintf(cmd, "cat %s | wc -l", snap);
    fp = popen(cmd, "r");
    fgets(tmp_stat, sizeof(tmp_stat), fp);
    pclose(fp);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s' -> %s", cmd, tmp_stat);

    return atoi(tmp_stat);

}

int logState(const char *dir, const char *file_name, const char *inp_type)
{
    int result = 0;
    if ( gfpState == NULL ) {
        char fstate[SIZE_ITEM_L];
        memset(fstate, 0x00, sizeof(fstate));
        sprintf(fstate, "%s/%s_%s%s", gszIniParCommon[E_STATE_DIR], gszAppName, gszToday, STATE_SUFF);
        gfpState = fopen(fstate, "a");
    }
    result = fprintf(gfpState, "%s|%s|%s\n", inp_type, dir, file_name);
    fflush(gfpState);
    return result;
}

void clearOldState()
{
    struct tm *ptm;
    time_t lTime;
    char tmp[SIZE_ITEM_L];
    char szOldestFile[SIZE_ITEM_S];
    char szOldestDate[SIZE_DATE_TIME_FULL+1];
    DIR *p_dir;
    struct dirent *p_dirent;
    int len1 = 0, len2 = 0;

    /* get oldest date to keep */
    time(&lTime);
    ptm = localtime( &lTime);
//printf("ptm->tm_mday = %d\n", ptm->tm_mday);
    ptm->tm_mday = ptm->tm_mday - atoi(gszIniParCommon[E_KEEP_STATE_DAY]);
//printf("ptm->tm_mday(after) = %d, keepState = %d\n", ptm->tm_mday, atoi(gszIniParCommon[E_KEEP_STATE_DAY]));
    lTime = mktime(ptm);
    ptm = localtime(&lTime);
    strftime(szOldestDate, sizeof(szOldestDate)-1, "%Y%m%d", ptm);
//printf("szOldestDate = %s\n", szOldestDate);

	writeLog(LOG_INF, "purge state file up to %s (keep %s days)", szOldestDate, gszIniParCommon[E_KEEP_STATE_DAY]);
    sprintf(szOldestFile, "%s%s", szOldestDate, STATE_SUFF);     // YYYYMMDD.proclist
    len1 = strlen(szOldestFile);
    if ( (p_dir = opendir(gszIniParCommon[E_STATE_DIR])) != NULL ) {
        while ( (p_dirent = readdir(p_dir)) != NULL ) {
            // state file name: <APP_NAME>_<PROC_TYPE>_YYYYMMDD.proclist
            if ( strcmp(p_dirent->d_name, ".") == 0 || strcmp(p_dirent->d_name, "..") == 0 )
                continue;
            if ( strstr(p_dirent->d_name, STATE_SUFF) != NULL &&
                 strstr(p_dirent->d_name, gszAppName) != NULL ) {

                len2 = strlen(p_dirent->d_name);
                // compare only last term of YYYYMMDD.proclist
                if ( strcmp(szOldestFile, (p_dirent->d_name + (len2-len1))) > 0 ) {
                    char old_state[SIZE_ITEM_L];
                    memset(old_state, 0x00, sizeof(old_state));
                    sprintf(old_state, "%s/%s", gszIniParCommon[E_STATE_DIR], p_dirent->d_name);

                    purgeOldData(old_state);

                    sprintf(tmp, "rm -f %s 2>/dev/null", old_state);
                    writeLog(LOG_INF, "remove state file: %s", p_dirent->d_name);
                    system(tmp);
                }
            }
        }
        closedir(p_dir);
    }
}

void purgeOldData(const char *old_state)
{
    FILE *ofp = NULL;
    char line[SIZE_ITEM_L], sdir[SIZE_ITEM_L], sfname[SIZE_ITEM_L], cmd[SIZE_ITEM_L];

    if ( (ofp = fopen(old_state, "r")) != NULL ) {
        memset(line, 0x00, sizeof(line));
        while ( fgets(line, sizeof(line),ofp) ) {
            memset(sdir,   0x00, sizeof(sdir));
            memset(sfname, 0x00, sizeof(sfname));
            memset(cmd,    0x00, sizeof(cmd));

            getTokenItem(line, 1, '|', sdir);
            getTokenItem(line, 2, '|', sfname);

            sprintf(cmd, "rm -f %s/%s", sdir, sfname);
            writeLog(LOG_DB3, "\told file %s/%s purged", sdir, sfname);
            system(cmd);
        }
        fclose(ofp);
        ofp = NULL;
    }
}

int readConfig(int argc, char *argv[])
{
    int key, i;

    memset(gszIniFile, 0x00, sizeof(gszIniFile));
    memset(gszAppName, 0x00, sizeof(gszAppName));

    memset(gszIniParInput,  0x00, sizeof(gszIniParInput));
    memset(gszIniParOutput, 0x00, sizeof(gszIniParOutput));
    memset(gszIniParMap, 0x00, sizeof(gszIniParMap));
    memset(gszIniParCommon, 0x00, sizeof(gszIniParCommon));
    memset(gszIniParDbConn, 0x00, sizeof(gszIniParDbConn));

    strcpy(gAppPath, argv[0]);
    char *p = strrchr(gAppPath, '/');
    *p = '\0';

    for ( i = 1; i < argc; i++ ) {
        if ( strcmp(argv[i], "-n") == 0 ) {     // specified ini file
            strcpy(gszIniFile, argv[++i]);
        }
        else if ( strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0 ) {
            printUsage();
            return FAILED;
        }
        else if ( strcmp(argv[i], "-mkini") == 0 ) {
            makeIni();
            return FAILED;
        }
    }

    sprintf(gszAppName, "%s_%d", _APP_NAME_, gnPrcId);
    if ( gszIniFile[0] == '\0' ) {
        sprintf(gszIniFile, "%s/%s_%d.ini", gAppPath, _APP_NAME_, gnPrcId);
    }

    if ( access(gszIniFile, F_OK|R_OK) != SUCCESS ) {
        sprintf(gszIniFile, "%s/%s.ini", gAppPath, _APP_NAME_);
        if ( access(gszIniFile, F_OK|R_OK) != SUCCESS ) {
            fprintf(stderr, "unable to access ini file %s (%s)\n", gszIniFile, strerror(errno));
            return FAILED;
        }
    }

    // Read config of INPUT Section
    for ( key = 0; key < E_NOF_PAR_INPUT; key++ ) {
        ini_gets(gszIniStrSection[E_INPUT], gszIniStrInput[key], "NA", gszIniParInput[key], sizeof(gszIniParInput[key]), gszIniFile);
    }

    // Read config of OUTPUT Section
    for ( key = 0; key < E_NOF_PAR_OUTPUT; key++ ) {
        ini_gets(gszIniStrSection[E_OUTPUT], gszIniStrOutput[key], "NA", gszIniParOutput[key], sizeof(gszIniParOutput[key]), gszIniFile);
    }

    // Read config of MAPPING Section
    for ( key = 0; key < E_NOF_PAR_MAPSVC; key++ ) {
        ini_gets(gszIniStrSection[E_MAPPING], gszIniStrMap[key], "NA", gszIniParMap[key], sizeof(gszIniParMap[key]), gszIniFile);
    }

    // Read config of COMMON Section
    for ( key = 0; key < E_NOF_PAR_COMMON; key++ ) {
        ini_gets(gszIniStrSection[E_COMMON], gszIniStrCommon[key], "NA", gszIniParCommon[key], sizeof(gszIniParCommon[key]), gszIniFile);
    }

    // Read config of DB Connection Section
    for ( key = 0; key < E_NOF_PAR_DBCONN; key++ ) {
        ini_gets(gszIniStrSection[E_DBCONN], gszIniStrDbConn[key], "NA", gszIniParDbConn[key], sizeof(gszIniParDbConn[key]), gszIniFile);
    }

    return SUCCESS;

}

void logHeader()
{
    writeLog(LOG_INF, "---- Start %s (v%s) with following parameters ----", _APP_NAME_, _APP_VERS_);
    // print out all ini file
    ini_browse(_ini_callback, NULL, gszIniFile);
}

void printUsage()
{
    fprintf(stderr, "\nusage: %s version %s\n", _APP_NAME_, _APP_VERS_);
    fprintf(stderr, "\tcreate common output format of mPay transaction for eFIT\n\n");
    fprintf(stderr, "%s.exe [-n <ini_file>] [-mkini]\n", _APP_NAME_);
    fprintf(stderr, "\tini_file\tto specify ini file other than default ini\n");
    fprintf(stderr, "\t-mkini\t\tto create ini template\n");
    fprintf(stderr, "\n");

}

int validateIni()
{
    int result = SUCCESS;

    // ----- Input Section -----
    if ( *gszIniParInput[E_TXCC_INP_DIR] != '\0' ) {
        if ( access(gszIniParInput[E_TXCC_INP_DIR], F_OK|R_OK) != SUCCESS ) {
            result = FAILED;
            fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrInput[E_TXCC_INP_DIR], gszIniParInput[E_TXCC_INP_DIR], strerror(errno));
        }
    }
    if ( *gszIniParInput[E_SPDP_INP_DIR] != '\0' ) {
        if ( access(gszIniParInput[E_SPDP_INP_DIR], F_OK|R_OK) != SUCCESS ) {
            result = FAILED;
            fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrInput[E_SPDP_INP_DIR], gszIniParInput[E_SPDP_INP_DIR], strerror(errno));
        }
    }

    // ----- Output Section -----
    int i, nof_dir = atoi(gszIniParOutput[E_OUT_BASE_NOF]);
    char path[SIZE_ITEM_L];
    if ( nof_dir > 1 ) {
        for ( i=0; i<nof_dir; i++ ) {
            memset(path, 0x00, sizeof(path));
            sprintf(path, "%s%d", gszIniParOutput[E_OUT_BASE_DIR], i);
            if ( access(path, F_OK|R_OK) != SUCCESS ) {
                result = FAILED;
                fprintf(stderr, "unable to access %s (%s)\n", path, strerror(errno));
            }
        }
    }
    else {
        if ( access(gszIniParOutput[E_OUT_BASE_DIR], F_OK|R_OK) != SUCCESS ) {
            result = FAILED;
            fprintf(stderr, "unable to access %s (%s)\n", gszIniStrOutput[E_OUT_BASE_DIR], strerror(errno));
        }
    }

    // ----- Common Section -----
    if ( *gszIniParCommon[E_REJ_INVALID] == 'Y' || *gszIniParCommon[E_REJ_INVALID] == 'y' ) {
        strcpy(gszIniParCommon[E_REJ_INVALID], "Y");
        if ( access(gszIniParCommon[E_REJ_OUT_DIR], F_OK|R_OK) != SUCCESS ) {
            result = FAILED;
            fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_REJ_OUT_DIR], gszIniParCommon[E_REJ_OUT_DIR], strerror(errno));
        }
    }
    if ( *gszIniParInput[E_TXCC_BCK] == 'Y' || *gszIniParInput[E_TXCC_BCK] == 'y' ) {
        strcpy(gszIniParInput[E_TXCC_BCK], "Y");
    }
    if ( *gszIniParInput[E_SPDP_BCK] == 'Y' || *gszIniParInput[E_SPDP_BCK] == 'y' ) {
        strcpy(gszIniParInput[E_SPDP_BCK], "Y");
    }
    if ( *gszIniParInput[E_TXCC_BCK] == 'Y' || *gszIniParInput[E_SPDP_BCK] == 'Y' ) {
        if ( access(gszIniParCommon[E_BCK_DIR], F_OK|R_OK) != SUCCESS ) {
            result = FAILED;
            fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_BCK_DIR], gszIniParCommon[E_BCK_DIR], strerror(errno));
        }
    }
    if ( access(gszIniParCommon[E_TMP_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_TMP_DIR], gszIniParCommon[E_TMP_DIR], strerror(errno));
    }
    if ( access(gszIniParCommon[E_STATE_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_STATE_DIR], gszIniParCommon[E_STATE_DIR], strerror(errno));
    }
    if ( atoi(gszIniParCommon[E_KEEP_STATE_DAY]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_KEEP_STATE_DAY], gszIniParCommon[E_KEEP_STATE_DAY]);
    }
    if ( atoi(gszIniParCommon[E_SKIP_OLD_FILE]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_SKIP_OLD_FILE], gszIniParCommon[E_SKIP_OLD_FILE]);
    }
    if ( access(gszIniParCommon[E_LOG_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_LOG_DIR], gszIniParCommon[E_LOG_DIR], strerror(errno));
    }
    if ( atoi(gszIniParCommon[E_SLEEP_SEC]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_SLEEP_SEC], gszIniParCommon[E_SLEEP_SEC]);
    }

    // ----- Db Connection Section -----
    if ( *gszIniParDbConn[E_ERM_USER] == '\0' || strcmp(gszIniParDbConn[E_ERM_USER], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s '%s'\n", gszIniStrDbConn[E_ERM_USER], gszIniParDbConn[E_ERM_USER]);
    }
    if ( *gszIniParDbConn[E_ERM_PASSWORD] == '\0' || strcmp(gszIniParDbConn[E_ERM_PASSWORD], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s '%s'\n", gszIniStrDbConn[E_ERM_PASSWORD], gszIniParDbConn[E_ERM_PASSWORD]);
    }
    if ( *gszIniParDbConn[E_ERM_DB_SID] == '\0' || strcmp(gszIniParDbConn[E_ERM_DB_SID], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s '%s'\n", gszIniStrDbConn[E_ERM_DB_SID], gszIniParDbConn[E_ERM_DB_SID]);
    }
    return result;

}

int _ini_callback(const char *section, const char *key, const char *value, void *userdata)
{
    if ( strstr(key, "PASSWORD") ) {
        writeLog(LOG_INF, "[%s]\t%s = ********", section, key);
    }
    else {
        writeLog(LOG_INF, "[%s]\t%s = %s", section, key, value);
    }
    return 1;
}

void procSynFiles(const char *dir, const char *fname, const char *inp_type, long cont_pos)
{

    FILE *rfp = NULL, *wfp = NULL, *ofp_rej = NULL;
    char full_fname[SIZE_ITEM_L], ofile_dtm[SIZE_DATE_TIME_FULL+1];
    char read_rec[SIZE_BUFF_20X], read_rec_ori[SIZE_BUFF_20X], cSep;
    char rej_msg[SIZE_BUFF_20X];
    int inp_no_field = 0, parse_field_cnt = 0;
    int line_cnt = 0, backup = 0;
    int cntr_wrt = 0, cntr_rej = 0, cnt_skip = 0;
    time_t t_start = 0, t_stop = 0;
    static int dir_no = -1;

    memset(full_fname, 0x00, sizeof(full_fname));
    memset(read_rec, 0x00, sizeof(read_rec));
    memset(ofile_dtm, 0x00, sizeof(ofile_dtm));

    ( ++gnFileSeq > 999 ? gnFileSeq = 0 : gnFileSeq );
    sprintf(ofile_dtm, "%s_%03d_%d", getSysDTM(DTM_DATE_TIME), gnFileSeq, gnPrcId);

    sprintf(full_fname, "%s/%s", dir, fname);
    if ( (rfp = fopen(full_fname, "r")) == NULL ) {
        writeLog(LOG_SYS, "unable to open read %s (%s)", full_fname, strerror(errno));
        return;
    }
    else {
        writeLog(LOG_INF, "processing file %s", fname);

        t_start = time(NULL);
        if ( cont_pos > 0 ) {
            fseek(rfp, cont_pos, SEEK_SET);
        }
        while ( fgets(read_rec, sizeof(read_rec), rfp) ) {

            memset(pbuf_rec, 0x00, sizeof(pbuf_rec));
            memset(&gOutCommon, 0x00, sizeof(gOutCommon));
            trimStr((unsigned char*)read_rec);
            memset(read_rec_ori, 0x00, sizeof(read_rec_ori));

            // safe original read record for later use, since getTokenAll modifies input string.
            strcpy(read_rec_ori, read_rec);
            line_cnt++;

            if ( strcmp(inp_type, gszIniParInput[E_TXCC_FPREF]) == 0 ) {
                inp_no_field = NOF_TXCC_FLD;
                cSep = '|';
                verifyField = verifyInpFieldTxcc;
                strchrremove(read_rec, '"');
                if ( *gszIniParInput[E_TXCC_BCK] == 'Y' ) {
                    backup = 1;
                }
            }
            else if ( strcmp(inp_type, gszIniParInput[E_SPDP_FPREF]) == 0 ) {
                inp_no_field = NOF_SPDP_FLD;
                cSep = '|';
                verifyField = verifyInpFieldSpdp;
                if ( *gszIniParInput[E_SPDP_BCK] == 'Y' ) {
                    backup = 1;
                }
            }

            // parse field
            if ( (parse_field_cnt = getTokenAll(pbuf_rec, inp_no_field, read_rec, cSep)) < inp_no_field ) {
                if ( *gszIniParCommon[E_REJ_INVALID] == 'Y' ) {
                    memset(rej_msg, 0x00, sizeof(rej_msg));
                    sprintf(rej_msg, "invalid num field %d expected %d | %s", parse_field_cnt, inp_no_field, read_rec_ori);
                    wrtOutReject(gszIniParCommon[E_REJ_OUT_DIR], fname, &ofp_rej, rej_msg);
                    cntr_rej++;
                }
                continue;
            }

            // do validation and ratinge here ...
            memset(rej_msg, 0x00, sizeof(rej_msg));
            if ( !verifyField(pbuf_rec, inp_no_field, fname, rej_msg) ) {
                if ( *gszIniParCommon[E_REJ_INVALID] == 'Y' ) {
                    sprintf(rej_msg, "%s | %s", rej_msg, read_rec_ori);
                    wrtOutReject(gszIniParCommon[E_REJ_OUT_DIR], fname, &ofp_rej, rej_msg);
                    cntr_rej++;
                }
                continue;
            }

            // do rating cdr
            //rate_result = calcOneTariff();
            //getPmnInfo(gOutCommon.pmn, gOutCommon.pmn_name, gOutCommon.roam_country, gOutCommon.roam_region);

            if ( wrtOutCommon(gszIniParCommon[E_TMP_DIR], inp_type, ofile_dtm, &wfp) == SUCCESS ) {
                cntr_wrt++;
            }
            else {
                cnt_skip++;
            }

            if ( (cntr_wrt % 500) == 0 && cntr_wrt > 0 ) {
                writeLog(LOG_INF, "%10d records have been processed", cntr_wrt);
                checkPoint(&rfp, full_fname, (char*)inp_type, gszIniParCommon[E_TMP_DIR], gszAppName, E_SET);
            }

            if ( isTerminated() == TRUE ) {
                checkPoint(&rfp, full_fname, (char*)inp_type, gszIniParCommon[E_TMP_DIR], gszAppName, E_SET);
                break;
            }

        }
        if ( isTerminated() != TRUE ) {
            // clear check point in case whole file has been processed
            checkPoint(NULL, "", "", gszIniParCommon[E_TMP_DIR], gszAppName, E_CLR);
        }
        t_stop = time(NULL);

        if ( rfp   != NULL ) fclose(rfp);
        if ( ofp_rej  != NULL ) fclose(ofp_rej);
        if ( wfp   != NULL ) {
            char cmd[SIZE_FULL_NAME];   memset(cmd, 0x00, sizeof(cmd));
            fclose(wfp);
            writeLog(LOG_INF, "processed %s -> %s", fname, basename(gszOutFname));
            int num = atoi(gszIniParOutput[E_OUT_BASE_NOF]);
            if ( num > 1 ) {
                dir_no = (dir_no + 1) % num;
                sprintf(cmd, "mv %s%s %s%d/%s", gszOutFname, TMPSUF, gszIniParOutput[E_OUT_BASE_DIR], dir_no, basename(gszOutFname));
            }
            else {
                sprintf(cmd, "mv %s%s %s/%s", gszOutFname, TMPSUF, gszIniParOutput[E_OUT_BASE_DIR], basename(gszOutFname));
            }
            system(cmd);
            chmod(gszOutFname, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
        }

        logState(dir, fname, inp_type);
        writeLog(LOG_INF, "%s done, process(id%d) common=%d skip=%d reject=%d total=%d file_time_used=%d sec", fname, gnPrcId, cntr_wrt, cnt_skip, cntr_rej, line_cnt, (t_stop - t_start));

        if ( backup ) {
            char cmd[SIZE_ITEM_L];
            memset(cmd, 0x00, sizeof(cmd));
            sprintf(cmd, "cp -p %s %s", full_fname, gszIniParCommon[E_BCK_DIR]);
            system(cmd);
        }
        unlink(full_fname);

    }

}

int olderThan(int day, const char *sdir, const char *fname)
{
    struct stat stat_buf;
    time_t systime = 0;
    int    result = FALSE;
    char   full_name[SIZE_ITEM_L];
    long   file_age = 0;
    long   bound = (long)(day * SEC_IN_DAY);

    memset(full_name, 0x00, sizeof(full_name));

    memset(&stat_buf, 0x00, sizeof(stat_buf));
    if ( !lstat(full_name, &stat_buf) ) {
        systime = time(NULL);
        file_age = (long)(systime - stat_buf.st_mtime);
        if ( file_age > bound ) {
            result = TRUE;
        }
    }
writeLog(LOG_DB2, "%s check olderThan %d days (%ld sec) ", fname, day, file_age);
    return result;

}

int verifyInpFieldTxcc(char *pbuf[], int bsize, const char *fname, char *err_msg)
{
    char yyyy[5], mm[3], dd[3];
    char HH[3], MM[3], SS[3];
    char tmp[SIZE_ITEM_X];

    if ( *pbuf[E_TXC_RECTYPE] != 'D' ) {    // reject head/trailer record, data record always be 2xx
        return FALSE;
    }

    if ( atoi(gszIniParCommon[E_LOG_LEVEL]) >= LOG_DB3 ) {
        int i; char _rec[SIZE_BUFF_2X]; memset(_rec, 0x00, sizeof(_rec));
        for ( i=0; i<bsize; i++ ) {
            if ( i == 0 )
                sprintf(_rec, "'%s'", pbuf[i]);
            else
                sprintf(_rec, "%s, '%s'", _rec, pbuf[i]);
        }
        writeLog(LOG_DB3, "%s", _rec);
    }

    strcpy(gOutCommon.evtsrc, "MPTRANS_CC");
    sprintf(gOutCommon.softype, "%d", SOF_TXN_UNKNOWN);
    sprintf(gOutCommon.sofis3ds, "%d", SOF_NON3D);

    memset(yyyy, 0x00, sizeof(yyyy)); memset(mm, 0x00, sizeof(mm)); memset(dd, 0x00, sizeof(dd));
    memset(HH, 0x00, sizeof(HH)); memset(MM, 0x00, sizeof(MM)); memset(SS, 0x00, sizeof(SS));
    // transaction dtm format -> "09022021145608" <=> (ddmmyyyyHHMMSS)
    strncpy(dd, pbuf[E_TXC_TXN_DTM], 2);
    strncpy(mm, pbuf[E_TXC_TXN_DTM]+2, 2);
    strncpy(yyyy, pbuf[E_TXC_TXN_DTM]+4, 4);
    strncpy(HH, pbuf[E_TXC_TXN_DTM]+8, 2);
    strncpy(MM, pbuf[E_TXC_TXN_DTM]+10, 2);
    strncpy(SS, pbuf[E_TXC_TXN_DTM]+12, 2);
    sprintf(gOutCommon.timekey, "%s%s%s%s%s%s", yyyy, mm, dd, HH, MM, SS);

    if ( *pbuf[E_TXC_CCARD_NO] == '\0' ) {  // no given card number => ""
        sprintf(gOutCommon.categories, "%d", CAT_TOTAL|CAT_OTHER);
    }
    else {
        sprintf(gOutCommon.categories, "%d", CAT_TOTAL|CAT_PREMIUM|CAT_APP_PURCHASE);
    }

    sprintf(gOutCommon.mppaymenttype, "%d", TXN_PAYMENT);

    sprintf(gOutCommon.mpbankmerchantid, "%s", pbuf[E_TXC_BANK_NAME]);
    sprintf(gOutCommon.creditcardnumber, "%s", pbuf[E_TXC_CCARD_NO]);

    strcpy(gOutCommon.txnstatus, pbuf[E_TXC_TX_STATUS]);
    sprintf(gOutCommon.txnstatusmsg, "%s~%s", pbuf[E_TXC_IPAY_ERR_CODE], pbuf[E_TXC_IPAY_ERR_DESC]);

    memset(tmp, 0x00, sizeof(tmp));
    sprintf(tmp, "%s~%s~%s~%s~%s~%s", pbuf[E_TXC_REF1], pbuf[E_TXC_REF2], pbuf[E_TXC_REF3], pbuf[E_TXC_REF4], pbuf[E_TXC_REF5], pbuf[E_TXC_REF6]);
    strncpy(gOutCommon.mprefnums, tmp, 100);

    strcpy(gOutCommon.msisdn, pbuf[E_TXC_MOB_NO]);
    strcpy(gOutCommon.tonumber, pbuf[E_TXC_MRCH_ID]);
    strcpy(gOutCommon.fromnumber, pbuf[E_TXC_SVC_ID]);

    getMapTab(E_SVC, pbuf[E_TXC_SVC_ID], gOutCommon.mpservicename);

    strcpy(gOutCommon.brandid, pbuf[E_TXC_BRAND_ID]);

    sprintf(gOutCommon.charge, "%d", atoi(pbuf[E_TXC_TOT_AMT])*100);    // makes baht to satang

    strcpy(gOutCommon.txnrefid, pbuf[E_TXC_CHNL_REF_ID]);
    strcpy(gOutCommon.aisrefno, pbuf[E_TXC_AIS_REF_NO]);
    strcpy(gOutCommon.sofbankrefid, pbuf[E_TXC_BANK_REF_NO]);
    strcpy(gOutCommon.recptnum, pbuf[E_TXC_RCPT_NO]);

    return TRUE;
}

int verifyInpFieldSpdp(char *pbuf[], int bsize, const char *fname, char *err_msg)
{

    char yyyy[5], mm[3], dd[3];
    char HH[3], MM[3], SS[3];
    int  tmp_cat = 0;

    if ( atoi(gszIniParCommon[E_LOG_LEVEL]) >= LOG_DB3 ) {
        int i; char _rec[SIZE_BUFF_2X]; memset(_rec, 0x00, sizeof(_rec));
        for ( i=0; i<bsize; i++ ) {
            if ( i == 0 )
                sprintf(_rec, "'%s'", pbuf[i]);
            else
                sprintf(_rec, "%s, '%s'", _rec, pbuf[i]);
        }
        writeLog(LOG_DB3, "%s", _rec);
    }

    if ( strlen(pbuf[E_SPD_MOB_NO]) < 9 ) {
        return FALSE;
    }
    strcpy(gOutCommon.msisdn, pbuf[E_SPD_MOB_NO]);

    strcpy(gOutCommon.evtsrc, "SPDP");
    /// "format :  input field = category
    ///     Input filed 'sof_cc_is_3ds' => 1 = Premium , 0= Local
    ///     Input filed  'payment_type' => VOID = Port, REFUND = Termiated
    ///     Input filed 'sof_type'  => SOF_CC = AppPurchase  , SOF_xx = Other"
    getMapTab(E_LOV, pbuf[E_SPD_SOF_TYPE], gOutCommon.softype);
    if ( strcmp(pbuf[E_SPD_SOF_TYPE], "SOF_CC") == 0 ) {
        tmp_cat |= CAT_APP_PURCHASE;
    }
    else if ( strncmp(pbuf[E_SPD_SOF_TYPE], "SOF_", 4) == 0 ) {
        tmp_cat |= CAT_OTHER;
    }

    if ( atoi(pbuf[E_SPD_IS_CCARD_3DS]) == 1 ) {
        sprintf(gOutCommon.sofis3ds, "%d", SOF_3D);
        tmp_cat |= CAT_PREMIUM;
    }
    else {
        sprintf(gOutCommon.sofis3ds, "%d", SOF_NON3D);
        tmp_cat |= CAT_LOCAL;
    }

    memset(yyyy, 0x00, sizeof(yyyy)); memset(mm, 0x00, sizeof(mm)); memset(dd, 0x00, sizeof(dd));
    memset(HH, 0x00, sizeof(HH)); memset(MM, 0x00, sizeof(MM)); memset(SS, 0x00, sizeof(SS));

    // transaction dtm format -> "DD/MM/YYYY HH:MM:SS" <=> (DDMMYYYYHHMMSS)
    strncpy(dd, pbuf[E_SPD_TXN_DATE], 2);
    strncpy(mm, pbuf[E_SPD_TXN_DATE]+3, 2);
    strncpy(yyyy, pbuf[E_SPD_TXN_DATE]+6, 4);
    strncpy(HH, pbuf[E_SPD_TXN_DATE]+11, 2);
    strncpy(MM, pbuf[E_SPD_TXN_DATE]+14, 2);
    strncpy(SS, pbuf[E_SPD_TXN_DATE]+17, 2);
    sprintf(gOutCommon.timekey, "%s%s%s%s%s%s", yyyy, mm, dd, HH, MM, SS);

    if ( strcmp(pbuf[E_SPD_PAYMENT_TYPE], "PAYMENT") == 0 ) {
        sprintf(gOutCommon.mppaymenttype, "%d", TXN_PAYMENT);
    }
    else if ( strcmp(pbuf[E_SPD_PAYMENT_TYPE], "VOID") == 0 ) {
        sprintf(gOutCommon.mppaymenttype, "%d", TXN_VOID);
        tmp_cat |= CAT_PORT;
    }
    else if ( strcmp(pbuf[E_SPD_PAYMENT_TYPE], "REFUND") == 0 ) {
        sprintf(gOutCommon.mppaymenttype, "%d", TXN_REFUND);
        tmp_cat |= CAT_TERMINATED;
    }
    else {
        sprintf(gOutCommon.mppaymenttype, "%d", TXN_UNKNOWN);
    }

    sprintf(gOutCommon.mpbankmerchantid, "%s", pbuf[E_SPD_BANK_CODE]);
    sprintf(gOutCommon.creditcardnumber, "%s", pbuf[E_SPD_CCARD_NO]);
    sprintf(gOutCommon.custname, "%s", pbuf[E_SPD_SOF_CCARD_NAME]);

    strcpy(gOutCommon.txnstatus, pbuf[E_SPD_TXN_STATUS]);
    sprintf(gOutCommon.txnstatusmsg, "%s~%s", pbuf[E_SPD_TXN_STATUS_CODE], pbuf[E_SPD_TXN_STATUS_MSG]);
    strcpy(gOutCommon.mprefnums, pbuf[E_SPD_MRCH_RSN_DET]);

    strcpy(gOutCommon.tonumber, pbuf[E_SPD_MRCH_ID]);
    strcpy(gOutCommon.fromnumber, pbuf[E_SPD_SVC_ID]);
    getMapTab(E_SVC, pbuf[E_SPD_SVC_ID], gOutCommon.mpservicename);

    sprintf(gOutCommon.charge, "%d", atoi(pbuf[E_SPD_TXN_AMT])*100);    // makes baht to satang

    strcpy(gOutCommon.txnrefid, pbuf[E_SPD_TXN_REF_ID]);

    strcpy(gOutCommon.sofbankrefid, pbuf[E_SPD_SOF_BANK_REF_ID]);

    sprintf(gOutCommon.categories, "%d", tmp_cat);

    return TRUE;

}

int wrtOutCommon(const char *odir, const char *inp_type, const char *file_dtm, FILE **ofp)
{
    char full_irfile[SIZE_ITEM_L];
    if ( *ofp == NULL ) {
        memset(gszOutFname, 0x00, sizeof(gszOutFname));
        memset(full_irfile, 0x00, sizeof(full_irfile));
        sprintf(gszOutFname, "%s/%s_%s_%s%s", odir, gszIniParOutput[E_OUT_FPREF], inp_type, file_dtm, gszIniParOutput[E_OUT_FSUFF]);
        sprintf(full_irfile, "%s%s", gszOutFname, TMPSUF);
        if ( (*ofp = fopen(full_irfile, "a")) == NULL ) {
            writeLog(LOG_SYS, "unable to open append %s (%s)", full_irfile, strerror(errno));
            return FAILED;
        }
    }

    fprintf(*ofp, "%02d|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|\n",
                MPAY_EVENT_TYPE, MPAY_PARTITION, gOutCommon.evtsrc, gOutCommon.timekey, gOutCommon.charge,
                gOutCommon.categories, gOutCommon.tonumber, gOutCommon.fromnumber, gOutCommon.mpservicename,
                gOutCommon.msisdn, gOutCommon.mppaymenttype, gOutCommon.softype, gOutCommon.mpbankmerchantid,
                gOutCommon.creditcardnumber, gOutCommon.custname, gOutCommon.sofis3ds, gOutCommon.txnstatus,
                gOutCommon.txnstatusmsg, gOutCommon.sofbrandtype, gOutCommon.sofbankrefid, gOutCommon.creditcardtype,
                gOutCommon.sofgwname, gOutCommon.merchchanneltype, gOutCommon.merchproductname, gOutCommon.merchreasoncode,
                gOutCommon.txnid, gOutCommon.txnrefid, gOutCommon.aisrefno, gOutCommon.recptnum, gOutCommon.brandid,
                gOutCommon.mprefnums, gOutCommon.feecharge, gOutCommon.productamt, gOutCommon.mdramt, gOutCommon.refundamt, gOutCommon.sofpcitoken);

    writeLog(LOG_DB3, "final rec> time_key(%s) service_id('%s' > '%s') merchant_id(%s) mobile_no(%s) chg(%s[satang]) src(%s)",
                gOutCommon.timekey, gOutCommon.fromnumber, gOutCommon.mpservicename, gOutCommon.tonumber, gOutCommon.msisdn , gOutCommon.charge, gOutCommon.evtsrc);

    return SUCCESS;

}

int wrtOutReject(const char *odir, const char *fname, FILE **ofp, const char *record)
{

    char full_rejfile[SIZE_ITEM_L];
    if ( *ofp == NULL ) {
        sprintf(full_rejfile, "%s/%s.%dREJ", odir, fname, gnPrcId);
        if ( (*ofp = fopen(full_rejfile, "a")) == NULL ) {
            writeLog(LOG_SYS, "unable to open append %s (%s)", full_rejfile, strerror(errno));
            return FAILED;
        }
    }

    fprintf(*ofp, "%s\n", record);
    return SUCCESS;

}

int manageMapTab()
{
    time_t curr_time = time(NULL);
    int reload_hour = 4 * 60 * 60;  // reload every 4 hours
    int result = SUCCESS;

    if ( (curr_time - gzLastTimeT) > reload_hour || gzLastTimeT == 0 ) {
        if ( connectDbErm(gszIniParDbConn[E_ERM_USER], gszIniParDbConn[E_ERM_PASSWORD], gszIniParDbConn[E_ERM_DB_SID], 1, 10) != SUCCESS ) {
            writeLog(LOG_ERR, "connectDbErm failed");
            return FAILED;
        }
        writeLog(LOG_INF, "loading db tables ...");
        initMapDbTab();
        result = loadFruadLov(&g_SqliteDb);
        disconnErm(gszIniParDbConn[E_ERM_DB_SID]);
        loadMapDb();
        gzLastTimeT = time(NULL);

    }
    return result;
}

void makeIni()
{

    int key;
    char cmd[SIZE_ITEM_S];
    char tmp_ini[SIZE_ITEM_S];
    char tmp_itm[SIZE_ITEM_S];

    sprintf(tmp_ini, "./%s_XXXXXX", _APP_NAME_);
    mkstemp(tmp_ini);
    strcpy(tmp_itm, "<place_holder>");

    // Write config of INPUT Section
    for ( key = 0; key < E_NOF_PAR_INPUT; key++ ) {
        ini_puts(gszIniStrSection[E_INPUT], gszIniStrInput[key], tmp_itm, tmp_ini);
    }

    // Write config of OUTPUT Section
    for ( key = 0; key < E_NOF_PAR_OUTPUT; key++ ) {
        ini_puts(gszIniStrSection[E_OUTPUT], gszIniStrOutput[key], tmp_itm, tmp_ini);
    }

    // Write config of COMMON Section
    for ( key = 0; key < E_NOF_PAR_COMMON; key++ ) {
        ini_puts(gszIniStrSection[E_COMMON], gszIniStrCommon[key], tmp_itm, tmp_ini);
    }

    // Write config of BACKUP Section
    for ( key = 0; key < E_NOF_PAR_DBCONN; key++ ) {
        ini_puts(gszIniStrSection[E_DBCONN], gszIniStrDbConn[key], tmp_itm, tmp_ini);
    }

    sprintf(cmd, "mv %s %s.ini", tmp_ini, tmp_ini);
    system(cmd);
    fprintf(stderr, "ini template file is %s.ini\n", tmp_ini);

}

int chkStateAndConcat(const char *oFileName)
{
    int result = FAILED;
    DIR *p_dir;
    struct dirent *p_dirent;
    char cmd[SIZE_BUFF];
    memset(cmd, 0x00, sizeof(cmd));
    unlink(oFileName);

    if ( (p_dir = opendir(gszIniParCommon[E_STATE_DIR])) != NULL ) {
        while ( (p_dirent = readdir(p_dir)) != NULL ) {
            // state file name: <APP_NAME>_<PROC_TYPE>_YYYYMMDD.proclist
            if ( strcmp(p_dirent->d_name, ".") == 0 || strcmp(p_dirent->d_name, "..") == 0 )
                continue;

            if ( strstr(p_dirent->d_name, STATE_SUFF) != NULL &&
                 strstr(p_dirent->d_name, gszAppName) != NULL ) {
                char state_file[SIZE_ITEM_L];
                memset(state_file, 0x00, sizeof(state_file));
                sprintf(state_file, "%s/%s", gszIniParCommon[E_STATE_DIR], p_dirent->d_name);
                if ( access(state_file, F_OK|R_OK|W_OK) != SUCCESS ) {
                    writeLog(LOG_ERR, "unable to read/write file %s", state_file);
                    result = FAILED;
                    break;
                }
                else {
                    sprintf(cmd, "cat %s >> %s 2>/dev/null", state_file, oFileName);
                    system(cmd);
                    result = SUCCESS;
                }
            }
        }
        closedir(p_dir);
        return result;
    }
    else {
        return result;
    }
}

int initMapDbTab()
{

    memset(gsdb_file_path, 0x00, sizeof(gsdb_file_path));
    sprintf(gsdb_file_path, "%s/.sqmpdb.db", gAppPath);

    int rc = sqlite3_open(gsdb_file_path, &g_SqliteDb);

    if (rc != SQLITE_OK) {
        sprintf(gzErrMsg, "cannot open sqlite dbs: %s\n", sqlite3_errmsg(g_SqliteDb));
        return FAILED;
    }

    char *sql1 = "CREATE TABLE IF NOT EXISTS MP_SERVICES ("
                 "SVCID   TEXT NOT NULL UNIQUE,"
                 "SVCNAME TEXT NOT NULL"
                 ");"
                 "CREATE UNIQUE INDEX IF NOT EXISTS IDX_SVC_ID ON SERVICES(SVCID);";
    char *sql2 = "CREATE TABLE IF NOT EXISTS LOV_CODE ("
                 "LOV_KEY TEXT NOT NULL UNIQUE,"
                 "LOV_VAL TEXT NOT NULL"
                 ");"
                 "DELETE FROM LOV_CODE;";
    rc = sqlite3_exec(g_SqliteDb, sql1, 0, 0, &g_SqliteErr_msg);
    rc = sqlite3_exec(g_SqliteDb, sql2, 0, 0, &g_SqliteErr_msg);

    if (rc != SQLITE_OK) {
        sprintf(gzErrMsg, "sql error: %s\n", g_SqliteErr_msg);
        sqlite3_free(g_SqliteErr_msg);
        g_SqliteErr_msg = NULL;
        sqlite3_close(g_SqliteDb);
        g_SqliteDb = NULL;
        return FAILED;
    }
    if ( g_SqliteErr_msg != NULL ) {
        sqlite3_free(g_SqliteErr_msg);
        g_SqliteErr_msg = NULL;
    }
    //sqlite3_close(g_SqliteDb);

    return SUCCESS;

}

int loadMapDb()
{
    char cmd[SIZE_BUFF];
    char uniqf[SIZE_ITEM_L], file_tmp[SIZE_ITEM_L], line[SIZE_ITEM_L];
    char insstr[SIZE_ITEM_L*100];
    int rc;
    memset(cmd, 0x00, sizeof(cmd));
    memset(uniqf, 0x00, sizeof(uniqf));
    memset(file_tmp, 0x00, sizeof(file_tmp));
    sprintf(uniqf   , "%s/mp_insert_service_id"    , gszIniParCommon[E_TMP_DIR]);
    sprintf(file_tmp, "%s/mp_insert_service_id_cat", gszIniParCommon[E_TMP_DIR]);

    sprintf(cmd, "rm -f %s", uniqf);
    system(cmd);
    memset(cmd, 0x00, sizeof(cmd));

    sprintf(cmd, "zcat %s/SUBS_MPSERVICE_*.DAT.gz | sed s/\\'//g > %s", gszIniParMap[E_MAPSVC_DIR], file_tmp);
    system(cmd);
writeLog(LOG_DB2, "%s", cmd);

    sprintf(cmd, "gawk -F \"|\" -v q=\"'\" '{ printf(\"INSERT INTO MP_SERVICES VALUES(%%s%%s%%s, %%s%%s%%s);\\n\", q, $2, q, q, $4, q); }' %s | sort -T %s -u > %s 2>/dev/null", file_tmp, gszIniParCommon[E_TMP_DIR], uniqf);
writeLog(LOG_DB2, "%s", cmd);
    if ( system(cmd) != SUCCESS ) {
        //writeLog(LOG_SYS, "cannot sort file %s (%s)", snapfile, strerror(errno));
        return FAILED;
    }

    if ( g_SqliteDb == NULL ) {
        rc = sqlite3_open(gsdb_file_path, &g_SqliteDb);
    }

    FILE *f = fopen(uniqf, "r");
    if (f != NULL) {

        memset(insstr, 0x00, sizeof(insstr));
        int line_cnt = 0;
        while ( fgets(line, SIZE_ITEM_L, f) ) {
            strcat(insstr, line);
            line_cnt++;
            if (line_cnt >= 200) {
                //printf("%s\n", insstr);
                sqlite3_exec(g_SqliteDb, "BEGIN;", NULL, NULL, NULL);
                rc = sqlite3_exec(g_SqliteDb, insstr, 0, 0, &g_SqliteErr_msg);
                memset(insstr, 0x00, sizeof(insstr));

                line_cnt = 0;
                if ( !(rc == SQLITE_OK || rc == SQLITE_CONSTRAINT) ) {
                    writeLog(LOG_SYS, "mapping: insert error: %s", g_SqliteErr_msg);
                    if ( g_SqliteErr_msg != NULL ) {
                        sqlite3_free(g_SqliteErr_msg);
                        g_SqliteErr_msg = NULL;
                    }
                    sqlite3_close(g_SqliteDb);
                    fclose(f);
                    sqlite3_exec(g_SqliteDb, "ROLLBACK;", NULL, NULL, NULL);
                    return FAILED;
                }
                sqlite3_exec(g_SqliteDb, "COMMIT;", NULL, NULL, NULL);
            }
        }
        fclose(f);
        if (line_cnt > 0) {
            sqlite3_exec(g_SqliteDb, "BEGIN;", NULL, NULL, NULL);
            rc = sqlite3_exec(g_SqliteDb, insstr, 0, 0, &g_SqliteErr_msg);
            if ( !(rc == SQLITE_OK || rc == SQLITE_CONSTRAINT) ) {
                writeLog(LOG_SYS, "mapping: insert error: %s", g_SqliteErr_msg);
                if ( g_SqliteErr_msg != NULL ) {
                    sqlite3_free(g_SqliteErr_msg);
                    g_SqliteErr_msg = NULL;
                }
                sqlite3_close(g_SqliteDb);
                sqlite3_exec(g_SqliteDb, "ROLLBACK;", NULL, NULL, NULL);
                return FAILED;
            }
            sqlite3_exec(g_SqliteDb, "COMMIT;", NULL, NULL, NULL);
        }
    }
    //sqlite3_close(g_SqliteDb);
    memset(cmd, 0x00, sizeof(cmd));
    sprintf(cmd, "find %s -type f -mtime +7 -exec rm -f {} \\;", gszIniParMap[E_MAPSVC_DIR]);
    system(cmd);
    return SUCCESS;
}

int getMapTab(int type, char *key, char *value)
{
    sqlite3_stmt *stmt = NULL;
    char sql[SIZE_ITEM_L];
    memset(sql, 0x00, sizeof(sql));

    if ( type == E_SVC ) {
        strcpy(sql, "SELECT SVCNAME FROM MP_SERVICES WHERE SVCID = ?");
    }
    else if ( type == E_LOV ) {
        strcpy(sql, "SELECT LOV_KEY FROM LOV_CODE WHERE LOV_VAL = ?");
    }

    //sqlite3_open(gsdb_file_path, &g_SqliteDb);
    if (sqlite3_prepare_v2(g_SqliteDb, sql, -1, &stmt, NULL)) {
        if ( stmt != NULL ) {
            sqlite3_finalize(stmt);
            stmt = NULL;
        }
       //sqlite3_Error executing sqlclose(g_SqliteDb);
       return FAILED;
    }

    sqlite3_bind_text(stmt, 1, key, -1, NULL);

    if ( sqlite3_step(stmt) == SQLITE_ROW ) {
        sprintf(value, "%s", sqlite3_column_text(stmt, 0));
    }

    if ( stmt != NULL ) {
       sqlite3_finalize(stmt);
       stmt = NULL;
    }
    //sqlite3_close(g_SqliteDb);
    return SUCCESS;
}

