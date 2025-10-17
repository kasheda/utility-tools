CREATE OR REPLACE FUNCTION "AP_CO_CONSIS_CHECK_LN_MISC"(var_pi_stream NUMBER)
  RETURN NUMBER AS

  var_l_no_of_streams     NUMBER := 64; --nvl(ap_get_data_mig_param('NO_OF_STREAMS'), 4);
  var_mig_user_id         VARCHAR2(12) := 'CONVTELLER'; --nvl(ap_get_data_mig_param('MIGUSER_ID'), 'CONVTELLER');
  var_mig_auth_id         VARCHAR2(12) := 'CONVSUPER'; --nvl(ap_get_data_mig_param('MIGAUTH_ID'), 'CONVSUPER');
  var_mig_lcy             NUMBER := nvl(ap_get_data_mig_param('BANK_LCY'),
                                        '104');
  var_l_count             NUMBER := 0;
  var_l_dist_count        NUMBER := 0;
  var_l_consis_no         NUMBER := 0;
  var_mig_date            DATE;
  var_bank_mast_dt_to_use CHAR(1) := 'N'; --nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'), 'Y');
  var_l_dat_process       DATE; --:= pk_ba_global.dat_process;
  var_l_gst_code          NUMBER := 555; --nvl(ap_get_data_mig_param('GST_COD_SC'), 555);
  var_l_gst_percent       NUMBER := 18; --nvl(ap_get_data_mig_param('GST_PERCENT'), 18);
  var_l_gst_date          DATE; --:= nvl(ap_get_data_mig_param('GST_DATE'), '01-Jul-2017');
  var_l_max_dtls_log      NUMBER := 1000; --nvl(ap_get_data_mig_param('MAX_DTLS_LOG'), 1000);
  var_l_function_name     VARCHAR2(100) := 'AP_CO_CONSIS_CHECK_LN_MISC';
  var_l_table_name        VARCHAR2(100);
  var_l_consis_message    VARCHAR2(200) := NULL;
BEGIN
  ap_bb_mig_log_string('00000 #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --Beginning of function
  /* IF ( var_bank_mast_dt_to_use = 'N' ) THEN
          var_l_dat_process      := var_mig_date; --nvl(ap_get_data_mig_param('DAT_PROCESS'), var_mig_date);
      END IF;
  */

  BEGIN
    select param_value
      into var_mig_date
      from BB_PARAMS
     where PARAM_NAME = 'MIG_DATE';
  END;
  BEGIN
    select dat_process into var_l_dat_process from cbsfchost.ba_bank_mast;
  END;

  DELETE FROM co_warn_table WHERE check_no BETWEEN 3101 AND 3200;

  COMMIT;

  /*consis 3101: RECORDS WHERE -ve RPA balance*/
  var_l_consis_no      := 3101;
  var_l_table_name     := 'LN_ACCT_BALANCES';
  var_l_consis_message := ' accounts have -ve RPA balance';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM civ_ln_acct_balances
     WHERE NVL(amt_rpa_balance, 0) < 0;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*consis 3102: RECORDS WHERE closed accounts having balance*/
  var_l_consis_no      := 3102;
  var_l_table_name     := 'civ_ln_acct_balances';
  var_l_consis_message := ' closed accounts having balance';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM (SELECT a.cod_acct_no,
                   amt_arrears_fees,
                   amt_arrears_legal,
                   amt_arrears_outgoing,
                   amt_arrears_prem,
                   amt_princ_balance,
                   amt_arrears_regular_int,
                   amt_arrears_penalty_int,
                   amt_arrears_pmi_int
              FROM civ_ln_acct_balances a, civ_ln_acct_dtls b
             WHERE a.cod_acct_no = b.cod_acct_no
               AND b.cod_acct_stat = 1
               AND (nvl(amt_arrears_fees, 0) + nvl(amt_arrears_legal, 0) +
                   nvl(amt_arrears_outgoing, 0) + nvl(amt_arrears_prem, 0) +
                   nvl(amt_princ_balance, 0) +
                   nvl(amt_arrears_regular_int, 0) +
                   nvl(amt_arrears_penalty_int, 0) +
                   nvl(amt_arrears_pmi_int, 0) + NVL(amt_rpa_balance, 0) --FA : 22-Mar-2024 Run : Include RPA balance also
                   ) != 0);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*consis 3103: RECORDS WHERE amt_arrears_assessed < amt_arrears_due + amt_arrears_cap*/
  var_l_consis_no      := 3103;
  var_l_table_name     := 'civ_ln_acct_balances';
  var_l_consis_message := ' accounts where amt_arrears_assessed < amt_arrears_due + amt_arrears_cap';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM civ_ln_arrears_table
     WHERE nvl(amt_arrears_assessed, 0) <
           (nvl(amt_arrears_due, 0) + nvl(amt_arrears_waived, 0) +
            nvl(amt_arrears_cap, 0));
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*consis 3104: accounts where disbursement amount is more than sanctioned amount*/
  var_l_consis_no      := 3104;
  var_l_table_name     := 'LN_ACCT_BALANCES';
  var_l_consis_message := ' accounts where disbursement amount is more than sanctioned amount';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT a.cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM civ_ln_acct_dtls A, civ_ln_acct_balances B
     WHERE A.cod_acct_no = B.cod_acct_no
       AND (A.amt_face_value < B.amt_disbursed)
       AND cod_acct_stat not in (1, 11);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*consis 3106: Accounts without IOI stage*/
  var_l_consis_no      := 3106;
  var_l_table_name     := 'LN_ACCT_SCHEDULE';
  var_l_consis_message := ' Accounts without IOI stage';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM (SELECT a.COD_ACCT_NO,
                   LISTAGG(DISTINCT NAM_STAGE, ', ') WITHIN GROUP(ORDER BY CTR_STAGE_NO) STAGE_LIST
              FROM CIV_LN_ACCT_SCHEDULE A, CIV_LN_ACCT_DTLS D
             WHERE NOT EXISTS
             (SELECT 1
                      FROM CIV_LN_ACCT_DTLS B, CIV_LN_ACCT_BALANCES C
                     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
                       AND AMT_FACE_VALUE = AMT_DISBURSED
                    --                           AND CTR_DISB = 1 --FA: 06-May-2024: Commented as Full disbursement can happen before next due date
                    )
               AND A.COD_ACCT_NO = D.COD_ACCT_NO
               AND D.COD_ACCT_STAT <> 1
             GROUP BY a.COD_ACCT_NO)
     WHERE STAGE_LIST NOT LIKE '%IOI%';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'INFO-PRE', --CRITICAL. PRE#23060
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*consis 3107: Accounts where amt_face_value < amt_disbursed*/
  var_l_consis_no      := 3107;
  var_l_table_name     := 'LN_ACCT_BALANCES';
  var_l_consis_message := ' Accounts where amt_face_value < amt_disbursed';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT A.cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM civ_ln_acct_dtls A, civ_ln_acct_balances B
     WHERE A.cod_acct_no = B.cod_acct_no
       AND (A.amt_face_value < B.amt_disbursed OR
           A.amt_face_value < B.amt_net_disbursed)
          --         AND A.cod_acct_no not in (select cod_acct_no from conv_ln_x_acct_exclude where consis_no = 24015) --FA:29-Apr-2024: Excluded Demand Loan
       AND 1 <> 1;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  /*consis 3108: Accounts without EPI stage*/
  var_l_consis_no      := 3108;
  var_l_table_name     := 'LN_ACCT_SCHEDULE';
  var_l_consis_message := ' Accounts without EPI stage';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1), COUNT(DISTINCT A.cod_acct_no)
      INTO var_l_count, var_l_dist_count
      FROM (SELECT cod_acct_no,
                   COUNT(1),
                   LISTAGG(DISTINCT nam_stage, ', ') WITHIN GROUP(ORDER BY ctr_stage_no) nam_stage
              FROM civ_ln_acct_schedule
             GROUP BY cod_acct_no) A
     WHERE nam_stage NOT LIKE '%EPI%';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'INFO-PRE', --CRITICAL Pre#23055
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ---new
  /*consis 3109: aborted in mock1*/
  var_l_consis_no      := 3109;
  var_l_table_name     := 'LN_ACCT_DTLS';
  var_l_consis_message := 'DAT_IOA_INS_WAIVE should be 1-Jan-1950';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM CO_LN_ACCT_DTLS
     WHERE DAT_IOA_INS_WAIVE <> '01-Jan-1950';
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;

  /*consis 3110: aborted in mock1*/
  var_l_consis_no      := 3110;
  var_l_table_name     := 'LN_ACCT_DTLS';
  var_l_consis_message := 'Parent limit id should null';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM CO_LN_ACCT_DTLS
     WHERE parent_limit_id is not null;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;

  /*consis 3111: aborted in mock1*/
  var_l_consis_no      := 3111;
  var_l_table_name     := 'Civ_Ln_Acct_Payinstrn';
  var_l_consis_message := 'Remitter account not present ch_accT_mast';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM Civ_Ln_Acct_Payinstrn a
     WHERE a.cod_remitter_acct not in
           (select cod_AccT_no from CIV_ch_AccT_mast);
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;

  /*consis 3112: aborted in mock1*/
  var_l_consis_no      := 3112;
  var_l_table_name     := 'civ_ln_acct_attributes';
  var_l_consis_message := 'Remitter account not present ch_accT_mast';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_attributes a
     WHERE a.TOT_MORT_MONTHS_ORIG is null;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  /*consis 3113: aborted in mock1*/
  var_l_consis_no      := 3113;
  var_l_table_name     := 'civ_ln_acct_int_balance_dtls';
  var_l_consis_message := 'currenecy is not 104';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_int_balance_dtls a
     WHERE a.cod_ccy <> 104
    
    ;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;

  /*consis 3114: aborted in mock1*/
  var_l_consis_no      := 3114;
  var_l_table_name     := 'civ_ln_arrears_table';
  var_l_consis_message := 'dat_arrears_Assed should not be migration date';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_arrears_table a
     WHERE to_char(a.dat_arrears_assessed, 'dd-mon-yyyy') = var_mig_date
    
    ;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;

  /*consis 3115: aborted in mock1*/
  var_l_consis_no      := 3115;
  var_l_table_name     := 'civ_ln_int_base_hist';
  var_l_consis_message := 'currency code is incorrect';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_int_base_hist a
     WHERE cod_ccy <> 104;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  /*consis 3116: aborted in mock1*/
  var_l_consis_no      := 3116;
  var_l_table_name     := 'civ_ln_acct_int_balance_dtls';
  var_l_consis_message := 'currency code is incorrect';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_int_balance_dtls a
     WHERE cod_ccy <> 104;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  /*consis 3117: aborted in mock1*/
  var_l_consis_no      := 3117;
  var_l_table_name     := 'civ_ln_acct_int_balance_dtls';
  var_l_consis_message := 'currency code is incorrect';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      from civ_ba_lo_coll_acct_xref a, cbsfchost.ba_prod_coll_xref b
     where a.cod_prod = b.cod_prod
       and a.cod_coll = b.cod_coll
       and a.flt_margin <> b.flt_margin;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ---
  /*consis 3118: aborted in mock1*/
  var_l_consis_no      := 3118;
  var_l_count          := 0;
  var_l_table_name     := 'CIV_TS_INCOME_TAX_LOG';
  var_l_consis_message := 'COD_TDS not in ba_tax_codes';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      from civ_ts_income_tax_log
     where cod_Tds not in (select cod_tds from cbsfchost.ba_Tax_codes)
        or cod_Tds_2 not in (select cod_tds from cbsfchost.ba_Tax_codes);
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  ---
  /*consis 3119: aborted in mock1*/
  var_l_consis_no      := 3119;
  var_l_count          := 0;
  var_l_table_name     := 'BA_BANK_CLDR';
  var_l_consis_message := 'Calender should be maintained for +3 and -3 year';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      FROM cbsfchost.ba_bank_mast t1
     WHERE NOT EXISTS (SELECT 1
              FROM cbsfchost.ba_bank_cldr t2
             WHERE t2.cod_cc_brn = 0
               AND t2.ctr_cldr_year =
                   TO_NUMBER(TO_CHAR(t1.dat_process, 'YYYY')) + 3)
        OR NOT EXISTS
     (SELECT 1
              FROM cbsfchost.ba_bank_cldr t3
             WHERE t3.cod_cc_brn = 0
               AND t3.ctr_cldr_year =
                   TO_NUMBER(TO_CHAR(t1.dat_process, 'YYYY')) - 3);
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count = 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;
  --
  ---
  /*consis 3119: aborted in mock1*/
  var_l_consis_no      := 3120;
  var_l_count          := 0;
  var_l_table_name     := 'XDUAL';
  var_l_consis_message := 'Xdual table entry is missing';
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_count
      from cbsfchost.xdual t1;
  
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(sqlcode,
                              'Select Failed: Consistency Check ' ||
                              var_l_consis_no,
                              var_l_consis_no);
  END;

  IF var_l_count = 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (cod_module, cod_cc_brn, table_name, severity, remarks, check_no)
      VALUES
        ('LN',
         0,
         var_l_table_name,
         'CRITICAL',
         var_l_dist_count || var_l_consis_message,
         var_l_consis_no);
    
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(sqlcode,
                                'Insert Failed: Consistency Check No ' ||
                                var_l_consis_no,
                                var_l_consis_no);
    END;
  END IF;

  COMMIT;

  ap_bb_mig_log_string(LPAD(var_l_consis_no, 5, 0) || ' #' ||
                       var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --after each consis

  ap_bb_mig_log_string('99999 #' || var_l_function_name || '# Stream = ' ||
                       var_pi_stream); --Ending of function
  RETURN 0;
END;
/
