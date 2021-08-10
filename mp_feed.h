///
///
/// FACILITY    : rating 3 sources of ir cdr(TAP, NRTRDE and SCP) and output to one common format
///
/// FILE NAME   : mp_feed.h
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
#ifndef __MP_FEED_H__
#define __MP_FEED_H__

#ifdef  __cplusplus
    extern "C" {
#endif
#include <ftw.h>

#define _APP_NAME_              "mp_feed"
#define _APP_VERS_              "1.1"

//#define     TYPE_MTCC           "MTCC"      // MP_Trans_CC
//#define     TYPE_SPDP           "SPDP"      // Supper Duper

#define     STATE_SUFF          ".proclist"
#define     ALERT_SUFF          ".alrt"

#include "frm_ais_glob.h"

// ----- INI Parameters -----
// All Section
typedef enum {
    E_INPUT = 0,
    E_OUTPUT,
    E_MAPPING,
    E_COMMON,
    E_DBCONN,
    E_NOF_SECTION
} E_INI_SECTION;


typedef enum {
    // INPUT Section
    E_TXCC_INP_DIR = 0,
    E_TXCC_FPREF,
    E_TXCC_FSUFF,
    E_TXCC_BCK,
    E_SPDP_INP_DIR,
    E_SPDP_FPREF,
    E_SPDP_FSUFF,
    E_SPDP_BCK,
    E_NOF_PAR_INPUT
} E_INI_INPUT_SEC;

typedef enum {
    // OUTPUT Section
    E_OUT_BASE_DIR = 0,
    E_OUT_BASE_NOF,
    E_OUT_FPREF,
    E_OUT_FSUFF,
    E_NOF_PAR_OUTPUT
} E_INI_OUTPUT_SEC;

typedef enum {
    // Mapping Section
    E_MAPSVC_DIR = 0,
    E_MAPSVC_FPREF,
    E_MAPSVC_FSUFF,
    E_MAPSVC_PURGE,
    E_NOF_PAR_MAPSVC
} E_INI_MAP_SEC;

typedef enum {
    // COMMON Section
    E_REJ_INVALID = 0,
    E_REJ_OUT_DIR,
    E_TMP_DIR,
    E_BCK_DIR,
    E_STATE_DIR,
    E_KEEP_STATE_DAY,
    E_SKIP_OLD_FILE,
    E_LOG_DIR,
    E_LOG_LEVEL,
    E_SLEEP_SEC,
    E_NOF_PAR_COMMON
} E_INI_COMMON_SEC;

typedef enum {
    // DB Section
    E_ERM_USER = 0,
    E_ERM_PASSWORD,
    E_ERM_DB_SID,
    E_NOF_PAR_DBCONN
} E_INI_DBCONN_SEC;

typedef enum {
    E_SVC = 0,
    E_LOV
} E_MAPPING_TYPE;

typedef struct _out_common_ {
    char evtsrc[FMS_MAX_STR_LEN+1];               // 01 event_source
    char timekey[FMS_MAX_STR_LEN+1];              // 02 event_datetime
    char charge[FMS_MAX_STR_LEN+1];               // 03 charge
    char categories[FMS_MAX_STR_LEN+1];           // 04 category
    char tonumber[FMS_MAX_STR_LEN+1];             // 05 merchant_id
    char fromnumber[FMS_MAX_STR_LEN+1];           // 06 service_id
    char mpservicename[FMS_MAX_STR_LEN+1];        // 07 service_name
    char msisdn[FMS_MAX_STR_LEN+1];               // 08 mobile_no
    char mppaymenttype[FMS_MAX_STR_LEN+1];        // 09 payment_type
    char softype[FMS_MAX_STR_LEN+1];              // 10 sof_type
    char mpbankmerchantid[FMS_MAX_STR_LEN+1];     // 11 bank_code || BANK_NAME
    char creditcardnumber[FMS_MAX_STR_LEN+1];     // 12 creditcard_no
    char custname[FMS_MAX_STR_LEN+1];             // 13 sof_cc_holder_name
    char sofis3ds[FMS_MAX_STR_LEN+1];             // 14 sof_cc_is_3ds
    char txnstatus[FMS_MAX_STR_LEN+1];            // 15 txn_status
    char txnstatusmsg[FMS_MAX_STR_LEN+1];         // 16 txn_status_code || txn_status_message
    char sofbrandtype[FMS_MAX_STR_LEN+1];         // 17 txn_sof_brand_type
    char sofbankrefid[FMS_MAX_STR_LEN+1];         // 18 sof_bank_ref_id
    char creditcardtype[FMS_MAX_STR_LEN+1];       // 19 sof_cc_card_type || sof_cc_card_country_code
    char sofgwname[FMS_MAX_STR_LEN+1];            // 20 sof_gateway_name
    char merchchanneltype[FMS_MAX_STR_LEN+1];     // 21 merchant_channel_type
    char merchproductname[FMS_MAX_STR_LEN+1];     // 22 merchant_product_name
    char merchreasoncode[FMS_MAX_STR_LEN+1];      // 23 merchant_reason_code
    char txnid[FMS_MAX_STR_LEN+1];                // 24 txn_id
    char txnrefid[FMS_MAX_STR_LEN+1];             // 25 txn_ref_id
    char aisrefno[FMS_MAX_STR_LEN+1];             // 26 ais_ref_no
    char recptnum[FMS_MAX_STR_LEN+1];             // 27 receipt_num
    char brandid[FMS_MAX_STR_LEN+1];              // 28 brand_id
    char mprefnums[FMS_MAX_STR_LEN+1];            // 29 ref1 ~ ref2 ~ ref3 ~ ref4 ~ ref5 ~ ref6 
    char feecharge[FMS_MAX_STR_LEN+1];            // 30 txn_fee_cust_net_amt
    char productamt[FMS_MAX_STR_LEN+1];           // 31 product_amount
    char mdramt[FMS_MAX_STR_LEN+1];               // 32 mdr_amount
    char refundamt[FMS_MAX_STR_LEN+1];            // 33 refunded_amount
    char sofpcitoken[FMS_MAX_STR_LEN+1];          // 34 sof_cc_pci_token
} ST_OUT_COMMON;

typedef enum {
    E_TXC_RECTYPE = 0               // 01       D
    , E_TXC_TXN_ID                  // 02       "6748009307"
    , E_TXC_TXN_DT                  // 03       "09022021"              (ddmmyyyy)
    , E_TXC_TXN_DTM                 // 04 *     "09022021145608"        (ddmmyyyyHHMMSS)
    , E_TXC_MOB_NO                  // 05 *     ""
    , E_TXC_MRCH_ID                 // 06 *     "13134"
    , E_TXC_SVC_ID                  // 07 *     "3000000000010904"
    , E_TXC_OFFER_ID                // 08       ""
    , E_TXC_BASKET_ID               // 09       ""
    , E_TXC_BRAND_ID                // 10 *     "53003"
    , E_TXC_BANK_NAME               // 11 *     "Kasikornbank Public Company Limited"
    , E_TXC_CCARD_TYPE              // 12       ""
    , E_TXC_CCARD_NO                // 13 *     "416202XXXXXX8536"
    , E_TXC_TOT_AMT                 // 14 *     "1800"                  (BAHT)
    , E_TXC_PROD_AMT                // 15       "1800"                  (BAHT)
    , E_TXC_INC_CST_FEE             // 16       "0"
    , E_TXC_EXC_CST_FEE             // 17       "0"
    , E_TXC_VAT_CST_FEE             // 18       "0"
    , E_TXC_MRCH_FEE                // 19       "0"
    , E_TXC_MRCH_CHG_FEE            // 20       "0"
    , E_TXC_MRCH_SHR_FEE            // 21       ""
    , E_TXC_SHOP_CHNL               // 22       "INERNETPAYMENT"
    , E_TXC_INET_CHNL               // 23       ""
    , E_TXC_TX_STATUS               // 24 *     "SUCCESS"
    , E_TXC_IPAY_ERR_CODE           // 25 *     "I0000"
    , E_TXC_IPAY_ERR_DESC           // 26 *     "SUCCESS"
    , E_TXC_CHNL_REF_ID             // 27 *     "MPAY20210209084490"
    , E_TXC_AIS_REF_NO              // 28 *     "090221376654"
    , E_TXC_BANK_REF_NO             // 29 *     ""
    , E_TXC_REF1                    // 30 *     "8000015692"
    , E_TXC_REF2                    // 31 *     "8000015692"
    , E_TXC_REF3                    // 32 *     "8000015692"
    , E_TXC_REF4                    // 33 *     ""
    , E_TXC_REF5                    // 34 *     ""
    , E_TXC_REF6                    // 35 *     ""
    , E_TXC_RCPT_NO                 // 36 *     ""
    , E_TXC_DTL_STATUS              // 37       "SUCCESS"
    , E_TXC_ERR_CODE                // 38       ""
    , E_TXC_ERR_MSG                 // 39       ""
    , E_TXC_LST_UPD_BY              // 40       "UPPTN"
    , E_TXC_LST_UPD_DTM             // 41       "09022021145721"
    , E_TXC_REF7                    // 42       ""
    , E_TXC_NO_MP_CST_ID            // 43       ""
    , E_TXC_PI_SEQ_NO               // 44       ""
    , NOF_TXCC_FLD                  // 45
} E_TXC_FLD;
//
// The following NRT format is an output of asn1conv_nrtrde.exe which is provided by eFIT - HPE ERM
//
typedef enum {
    E_SPD_TXN_ID = 0                // 01 
    , E_SPD_TXN_DATE                // 02 
    , E_SPD_MRCH_ID                 // 03 
    , E_SPD_MRCH_NAME               // 04 - 
    , E_SPD_SVC_ID                  // 05 
    , E_SPD_SVC_NAME                // 06 
    , E_SPD_MOB_NO                  // 07 
    , E_SPD_PAYMENT_TYPE            // 08 
    , E_SPD_SOF_TYPE                // 09 
    , E_SPD_BANK_CODE               // 10
    , E_SPD_CCARD_NO                // 11
    , E_SPD_SOF_CCARD_NAME          // 12
    , E_SPD_IS_CCARD_3DS            // 13
    , E_SPD_TXN_AMT                 // 14
    , E_SPD_TXN_STATUS              // 15
    , E_SPD_TXN_STATUS_CODE         // 16
    , E_SPD_TXN_STATUS_MSG          // 17
    , E_SPD_TXN_SOF_BRAND_TYPE      // 18
    , E_SPD_SOF_BANK_REF_ID         // 19
    , E_SPD_SOF_CCARD_TYPE          // 20
    , E_SPD_SOF_CCARD_CNTRY_CODE    // 21
    , E_SPD_MRCH_CHNL_TYPE          // 22
    , E_SPD_MRCH_PROD_NAME          // 23
    , E_SPD_MRCH_RSN_CODE           // 24
    , E_SPD_MRCH_RSN_DET            // 25
    , E_SPD_TXN_REF_ID              // 26
    , E_SPD_TXN_FEE_CST_NET_AMT     // 27
    , E_SPD_PRODUCT_AMT             // 28
    , E_SPD_MDR_AMT                 // 29
    , E_SPD_REFUNDED_AMT            // 30
    , E_SPD_SOF_CC_PCI_TOKEN        // 31
    , NOF_SPDP_FLD                  // 32
} E_SPD_FLD;

int     buildSnapFile(const char *snapfile);
int     _chkTxccFile(const char *fpath, const struct stat *info, int typeflag, struct FTW *ftwbuf);
int     _chkSpdpFile(const char *fpath, const struct stat *info, int typeflag, struct FTW *ftwbuf);

int     chkSnapVsState(const char *snap);
void    procSynFiles(const char *dir, const char *fname, const char *inp_type, long cont_pos);
int     olderThan(int day, const char *sdir, const char *fname);
int     (*verifyField)(char *pbuf[], int bsize, const char *fname, char *err_msg);
int     verifyInpFieldTxcc(char *pbuf[], int bsize, const char *fname, char *err_msg);
int     verifyInpFieldSpdp(char *pbuf[], int bsize, const char *fname, char *err_msg);

int     wrtOutCommon(const char *odir, const char *inp_type, const char *file_dtm, FILE **ofp);
int     wrtOutReject(const char *odir, const char *fname, FILE **ofp, const char *record);

int     manageMapTab();

int     logState(const char *dir, const char *file_name, const char *inp_type);
void    clearOldState();
void    purgeOldData(const char *old_state);
int     readConfig(int argc, char *argv[]);
void    logHeader();
void    printUsage();
int     validateIni();
int     _ini_callback(const char *section, const char *key, const char *value, void *userdata);
void    makeIni();
int     chkStateAndConcat(const char *oFileName);

int     initMapDbTab();
int     loadMapDb();
int     getMapTab();

#ifdef  __cplusplus
    }
#endif


#endif  // __MP_FEED_H__

