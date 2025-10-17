CREATE OR REPLACE FUNCTION "AP_CO_CONSIS_CHECK_LN_NO_STREAM" (var_cod_stream_id NUMBER)
  RETURN NUMBER AS
  var_dat_process        DATE;
  var_dat_last_process   DATE;
  var_l_count            NUMBER;
  var_makerid            CHAR(12) := 'CONVTELLER';--nvl(ap_get_data_mig_param('MIGUSER_ID'), 'CONVTELLER');
  var_bank_mast_dt_to_use VARCHAR2(1) := 'N';--nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'), 'Y');
  var_cod_cc_brn         NUMBER := 0;
  var_l_cod_prod_ln_from NUMBER;
  var_l_cod_prod_ln_to   NUMBER;
  var_cod_entity_vpd     NUMBER(5);
  CURSOR consis_brn IS
    SELECT cod_cc_brn
      FROM conv_brn_stream_proc_xref a
     WHERE a.cod_proc_nam = 'AP_CO_CONSIS_CHECK_LN'
     --  AND a.cod_stream_id = var_cod_stream_id
       AND a.flg_processed = 'N'; -- and cod_cc_brn = 2200;
BEGIN
--  EXECUTE IMMEDIATE 'ALTER SESSION SET PARALLEL_DEGREE_POLICY = AUTO';
 -- EXECUTE IMMEDIATE 'ALTER session enable parallel query';
  /*BEGIN
    SELECT dat_process, dat_last_process
      INTO var_dat_process, var_dat_last_process
      FROM ba_bank_mast;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select From co_civ_dates Failed.');
      ora_raiserror(SQLCODE, 'Select From co_civ_dates Failed.', 94);
  END;*/
  select dat_process, dat_last_process
    into var_dat_process, var_dat_last_process
    from cbsfchost.ba_bank_mast;

   /* IF ( var_bank_mast_dt_to_use = 'N' ) THEN
        var_dat_process := nvl(ap_get_data_mig_param('DAT_PROCESS'), var_dat_process);
        var_dat_last_process := nvl(ap_get_data_mig_param('DAT_LAST_PROCESS'), var_dat_last_process);
    END IF;*/
  /*BEGIN
    SELECT dat_last_process INTO var_dat_last_process FROM ba_bank_mast;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select From co_civ_dates Failed.');
      ora_raiserror(SQLCODE, 'Select From co_civ_dates Failed.', 95);
  END;*/
  /*BEGIN
    SELECT cod_entity_vpd INTO var_cod_entity_vpd FROM ba_entity_mast;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select From ba_bank_mast Failed.');
      ora_raiserror(SQLCODE, 'Select From ba_bank_mast Failed.', 96);
  END;*/
--  BEGIN
--    /*SELECT  cod_prod_ln_from,cod_prod_ln_to
--    INTO      var_l_cod_prod_ln_from,var_l_cod_prod_ln_to
--    FROM   al_global
--    WHERE flg_mnt_status = 'A';*/
--    --var_l_cod_prod_ln_from := pk_al_global.cod_prod_ln_from;
--    var_l_cod_prod_ln_from := 900;
--    var_l_cod_prod_ln_to   := 910;
--    --var_l_cod_prod_ln_to   := pk_al_global.cod_prod_ln_to;
--  EXCEPTION
--    WHEN OTHERS THEN
--      ora_raiserror(SQLCODE,
--                          'SELECT FAILED FOR ba_prod_acct_info',
--                          107);
--  END;

  --FOR consis_brn_rec IN consis_brn LOOP
    /*    IF var_cod_cc_brn <> consis_brn_rec.cod_cc_brn then
      COMMIT;
    END IF; commit;*/
   -- var_cod_cc_brn := consis_brn_rec.cod_cc_brn;
    --ap_co_ins_consis_proc_time('ap_co_consis_check_ln',var_cod_cc_brn,1);
    /*  BEGIN
      DELETE
      FROM co_warn_table
       WHERE cod_cc_brn = var_cod_cc_brn
         AND check_no BETWEEN 401 AND 500;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE,                      'Delete FROM co_warn_table failed in ap_co_consis_check_ln.');
        ora_raiserror(SQLCODE,
                      'Delete failed in ap_co_consis_check_ln.',
                      124);
    END;
    commit;*/
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls A
       WHERE /*cod_cc_brn = var_cod_cc_brn
         AND */cod_prod IN (SELECT cod_prod
                            FROM cbsfchost.ln_prod_mast
                           WHERE cod_secured = 1
                             AND flg_mnt_status = 'A')
         AND cod_acct_no NOT IN
             (SELECT DISTINCT cod_acct_no
                FROM civ_ch_acct_cust_xref B
               WHERE B.cod_acct_no = A.cod_acct_no
               --  AND B.cod_cc_brn = var_cod_cc_brn
                 AND cod_acct_cust_rel = 'GUA');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 401');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 401',
                            149);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'INFO',
           var_l_count ||
           '  LOANS WHERE GUARANTOR MISSING FOR GUARANTOR MANDATORY PRODUCT.',
           '401');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 401');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 401',
                              162);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls
       WHERE /*cod_cc_brn = var_cod_cc_brn
         AND */amt_face_value <= 0;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 402');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 402',
                            180);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  LOANS WHERE AMT_FACE_VALUE <= zero.',
           '402');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 402');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 402',
                              192);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_balances a, civ_ln_acct_dtls b
       WHERE /*b.cod_cc_brn = var_cod_cc_brn
         AND */a.cod_Acct_no = b.cod_acct_no
         AND (amt_disbursed < 0 OR amt_net_disbursed < 0);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 403');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 403',
                            209);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           '  LOANS WHERE AMT_DISBURSED <zero OR AMT_NET_DISBURSED < zero.',
           '403');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 403');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 403',
                              221);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_balances A,
             civ_ln_acct_schedule B,
             civ_ln_acct_dtls     C
       WHERE C.cod_acct_no = B.cod_acct_no
         and C.cod_acct_no = A.cod_acct_no
            /* and trim(c.cod_acct_no) not in
                         (select trim(cod_acct_no)
                            from civ_ln_death_cases
                          UNION
                          select trim(cod_acct_no) from civ_LN_X_COLLAT_SEIZ_MAST)*/
       --  and c.cod_cc_brn = var_cod_cc_brn
         AND A.amt_arrears_princ != A.amt_princ_balance
         AND b.cod_instal_rule in
             (select cod_inst_rule
                from cbsfchost.ln_inst_rules
               where cod_inst_calc_method = 'PMI')
         AND B.dat_stage_start < var_dat_process;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 405');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 405',
                            252);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_BALANCES',
           'CRITICAL',
           var_l_count ||
           '  MATURED LOANS WHERE AMT_ARREARS_PRINC <> AMT_PRINC_BALANCE.',
           '405');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 405');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 405',
                              264);
      END;
    END IF;
    commit;
    /*    BEGIN
      SELECT COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls A, civ_ln_acct_balances B
       WHERE A.cod_cc_brn = var_cod_cc_brn
         AND A.cod_acct_no = B.cod_acct_no
         AND (A.amt_face_value <> B.amt_disbursed \*OR
             A.amt_face_value <> B.amt_net_disbursed*\);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 406');
        cbsfchost.ora_raiserror(SQLCODE, 'Select Failed: Consistency Check 406', 286);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_BALANCES',
           'INFO',
           var_l_count || '  LOANS WHERE AMT_DISBURSED <> AMT_FACE_VALUE.',
           '406');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 406');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 406',
                        298);
      END;
    END IF;
    commit;*/
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls
       WHERE /*cod_cc_brn = var_cod_cc_brn
         AND */amt_face_value_org <> 0;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 402');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 409',
                            180);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           '  LOANS WHERE AMT_FACE_VALUE_ORG should be zero.',
           '409');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 402');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 409',
                              192);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls
       WHERE (flg_accr_status NOT IN ('S', 'N') OR flg_accr_status IS NULL)/*
         AND cod_cc_brn = var_cod_cc_brn*/;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 410');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 410',
                            315);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  LOANS WHERE FLG_ACCR_STATUS NOT IN (S, N).',
           '410');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 410');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 410',
                              327);
      END;
    END IF;
    commit;

    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls
       WHERE ctr_term_months <= 0;
         --AND cod_cc_brn = var_cod_cc_brn;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 411');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 411',
                            344);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  LOANS WHERE CTR_TERM_MONTHS <= zero.',
           '411');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 411');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 411',
                              356);
      END;
    END IF;
    commit;

    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls
       WHERE dat_last_charged > var_dat_process;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 412');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 412',
                            373);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  LOANS WHERE DAT_LAST_CHARGED > DAT_PROCESS.',
           '412');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 412');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 412',
                              385);
      END;
    END IF;
    commit;

    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS
       WHERE dat_acct_open > var_dat_process;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 413');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 413',
                            402);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'WARNING',
           var_l_count || '  LOANS WHERE DAT_ACCT_OPEN > DAT_PROCESS.',
           '413');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 413');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 413',
                              414);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS
       WHERE dat_acct_open > dat_last_disb;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 414');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 414',
                            431);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'WARNING',
           var_l_count || '  LOANS WHERE DAT_ACCT_OPEN > DAT_LAST_DISB.',
           '414');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 414');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 414',
                              443);
      END;
    END IF;
    --commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS
       WHERE dat_first_disb < dat_acct_open;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 415',
                            455);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  LOANS WHERE DAT_ACCT_OPEN > DAT_FIRST_DISB.',
           '415');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 414');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 415',
                              473);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS A, civ_ln_acct_balances B
       WHERE /*A.cod_cc_brn = var_cod_cc_brn
         AND*/ B.cod_cc_brn = A.cod_cc_brn
         AND A.cod_acct_no = B.cod_acct_no
         AND A.amt_face_value < B.amt_princ_balance;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 418');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 418',
                            463);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_balances',
           'INFO',
           var_l_count ||
           '  Loans Where AMT_FACE_VALUE < AMT_PRINC_BALANCE.',
           '418');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 418');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 418',
                              475);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_acct_no
                FROM CiV_LN_ACCT_DTLS a, civ_ln_acct_balances b
               WHERE /*a.cod_cc_brn = var_cod_cc_brn
                 AND */a.cod_Acct_no = b.cod_acct_no
                 AND b.amt_disbursed > 0
              MINUS
              SELECT a.cod_acct_no
                FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
               where/* b.cod_cc_brn = var_cod_cc_brn
                 and*/ a.cod_acct_no = b.cod_acct_no);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 419');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 419',
                            498);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count || '  Loans Where LOANS SCHEDULE MISSING.',
           '419');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 419');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 419',
                              511);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(distinct a.cod_Acct_No)
        INTO var_l_count
        FROM civ_ln_acct_schedule a, CiV_LN_ACCT_DTLS b
       WHERE amt_instal is NULL
       and b.dat_of_maturity > var_dat_process
        -- and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
         and b.flg_mnt_status ='A';
      --AND cod_last_mnt_makerid = var_makerid  ak_change
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 420');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 420',
                            523);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count || '  Loans Where AMT_INSTAL is NULL.',
           '420');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 420');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 420',
                              525);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_schedule a, CiV_LN_ACCT_DTLS b
       WHERE amt_instal <= 0
        -- and b.cod_cc_brn = var_cod_cc_brn
        and b.dat_of_maturity > var_dat_process
         and b.cod_acct_no = a.cod_acct_no
         and b.flg_mnt_status ='A'
         AND a.cod_instal_rule IN
             (SELECT cod_inst_rule
                FROM cbsfchost.ln_inst_rules
               WHERE flg_mnt_status = 'A'
                 AND cod_inst_calc_method = 'EPI');
      --AND cod_last_mnt_makerid = var_makerid  ak_change
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 421');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 421',
                            532);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL', --ak_change
           var_l_count || '  Loans Where AMT_INSTAL = zero In EPI Stage.',
           '421');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 421');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 421',
                              544);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS
       WHERE cod_prod NOT IN (SELECT cod_prod
                                FROM cbsfchost.ln_prod_mast
                               WHERE flg_mnt_status = 'A')
                               and flg_mnt_status ='A'/*
         and cod_cc_brn = var_cod_cc_brn*/;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select for Consistency Check 422 failed. ');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select for Consistency Check 422 failed. ',
                            563);
    END;
    IF (var_l_count > 0) Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL', --ak change
           var_l_count || '  Loans Where COD_PROD out of LN_PROD_MAST.',
           '422');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Insert for Consistency Check 422 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 422 failed. ',
                              574);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS A
       WHERE /*A.cod_cc_brn = var_cod_cc_brn
         AND */A.cod_sched_type NOT IN --akshay reached here
             (SELECT B.cod_sched_type
                FROM cbsfchost.ln_sched_types B
               WHERE A.cod_prod = B.cod_prod
                 AND B.flg_mnt_status = 'A')
                 and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 423');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 423',
                            595);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           '  Loans Where SCH_TYPE Not Defined At Product Level.',
           '423');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 423');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 423',
                              607);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS
       WHERE /*cod_cc_brn = var_cod_cc_brn
         AND */((flg_accr_status = 'S' AND flg_past_due_status = 0) OR
             (flg_accr_status = 'N' AND flg_past_due_status != 0))
              and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 426');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 426',
                            666);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           '  Loans Where ACCR_STATUS Does not Match PAST_DUE_STATUS.',
           '426');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 426');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 426',
                              678);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_balances a, CiV_LN_ACCT_DTLS b
       WHERE /*b.cod_cc_brn = var_cod_cc_brn
         AND */A.cod_acct_no = B.cod_acct_no
         AND amt_princ_balance > amt_disbursed
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 427');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 427',
                            694);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_balances',
           'INFO',
           var_l_count || 'Loans Where AMT_PRINC_BALANCE > AMT_DISBURSED',
           '427');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 427');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 427',
                              706);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_balances a, CiV_LN_ACCT_DTLS b
       WHERE A.cod_acct_no = B.cod_acct_no
         AND amt_arrears_princ > amt_disbursed
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 428');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 428',
                            722);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'INFO',
           var_l_count ||
           '  Loans Where AMT_ARREARS_PRINC > AMT_DISBURSED. INFO As per Iteration_3.3_Post_Consis.xls',
           '428');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 428');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 428',
                              734);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM CiV_LN_ACCT_DTLS
       WHERE /*cod_cc_brn = var_cod_cc_brn
         AND */dat_last_disb > var_dat_process
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 429');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 429',
                            750);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  Loans Where DAT_LAST_DISB > DAT_PROCESS.',
           '429');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 429');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 429',
                              762);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT a.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_arrears_table a, CiV_LN_ACCT_DTLS b
       WHERE amt_arrears_due < 0
         --and b.cod_cc_brn = var_cod_cc_brn
         and a.cod_acct_no = b.cod_acct_no;
         
    


    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 431');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 431',
                            779);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count || '  Loans Where AMT_ARREARS_DUE < zero',
           '431');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 431');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 431',
                              791);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT a.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_arrears_table a, CiV_LN_ACCT_DTLS b
       WHERE amt_arrears_assessed <= 0
        -- and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 432');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 432',
                            808);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count || '  Loans Where AMT_ARREARS_ASSESSED <= zero',
           '432');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 432');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 432',
                              820);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_arrears_table a, CiV_LN_ACCT_DTLS b
       WHERE dat_arrears_due > var_dat_process
         --and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 433');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 433',
                            837);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count || '  Loans Where DAT_ARREARS_DUE > DAT_PROCESS',
           '433');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 433');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 433',
                              849);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT a.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_arrears_table a, CiV_LN_ACCT_DTLS b
       WHERE a.dat_last_payment > var_dat_process
        -- and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 434');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 434',
                            866);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count || '  Loans Where DAT_LAST_PAYMENT > DAT_PROCESS',
           '434');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 434');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 434',
                              878);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT a.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_arrears_table a, civ_LN_ACCT_DTLS b
       WHERE a.dat_last_payment < dat_arrears_due
         AND a.dat_last_payment <> TO_DATE('01011950', 'DDMMYYYY')
         --and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
          and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 435');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 435',
                            896);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_arrears_table',
           'INFO-PRE', --CRITICAL. Pre#25028
           var_l_count ||
           '  Loans Where DAT_LAST_PAYMENT < DAT_ARREARS_DUE',
           '435');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 435');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 435',
                              908);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT cod_acct_no)
        INTO var_l_count
        FROM civ_LN_ACCT_DTLS
       WHERE /*cod_cc_brn = var_cod_cc_brn
        AND */ctr_disb < 0
        and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 436');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 436',
                            924);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  Loans Where CTR_DISB < zero.',
           '436');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 436');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 436',
                              936);
      END;
    END IF;
    commit;
    BEGIN
      /*SELECT \*+PARALLEL(4) *\
       COUNT(DISTINCT A.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_acct_balances A, civ_LN_ACCT_DTLS B
       WHERE A.cod_acct_no = B.cod_acct_no
         AND B.flg_accr_status = 'N'
        -- AND b.cod_cc_brn = var_cod_cc_brn
         AND A.amt_arrears_fees <>
             (SELECT NVL(SUM(amt_arrears_due), 0)
                FROM civ_ln_arrears_table C
               WHERE A.cod_acct_no = C.cod_acct_no
                 AND C.cod_arrear_type = 'F')
                 and flg_mnt_status ='A';*/
               select count(1) 
               INTO var_l_count
        from (
SELECT /*+PARALLEL(4) */
 a.cod_acct_no,
 NVL(SUM(AMT_ARREARS_DUE), 0) arrear_table_total,
 A.AMT_ARREARS_FEES arrear_balance_total
  FROM CIV_LN_ACCT_BALANCES A, CIV_LN_ACCT_DTLS B, CIV_LN_ARREARS_TABLE C
 WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
   and B.cod_acct_no = C.cod_acct_no
   AND B.FLG_ACCR_STATUS = 'N'
   and b.flg_mnt_status = 'A'
   and C.COD_ARREAR_TYPE = 'F'
   group by a.cod_acct_no, A.AMT_ARREARS_FEES)
   where arrear_table_total<>arrear_balance_total  
                 ;
                 
                 
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 437');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 437',
                            962);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_balances',
           'CRITICAL',
           var_l_count ||
           '  NORMAL Loans Where CHARGE_ARREARS <> SUM ( CHARGE_ARREARS ) In Arrears Table.',
           '437');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 437');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 437',
                              975);
      END;
    END IF;
    commit;
    BEGIN
      /*SELECT \*+PARALLEL(4) *\
       COUNT(DISTINCT A.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_acct_balances A, civ_LN_ACCT_DTLS B
       WHERE A.cod_acct_no = B.cod_acct_no
--         AND B.flg_accr_status = 'N'
         --AND b.cod_cc_brn = var_cod_cc_brn
         AND A.amt_arrears_princ <>
             (SELECT NVL(SUM(amt_arrears_due), 0)
                FROM civ_ln_arrears_table C
               WHERE A.cod_acct_no = C.cod_acct_no
                 AND C.cod_arrear_type = 'C')
                 and flg_mnt_status ='A'*/
                 select count(1) into var_l_count
                 from (    
     SELECT /*+PARALLEL(4) */
      A.COD_ACCT_NO,
      A.AMT_ARREARS_PRINC arrear_balance_total,
      NVL(SUM(AMT_ARREARS_DUE), 0) arrear_table_total
       FROM CIV_LN_ACCT_BALANCES A,
            CIV_LN_ACCT_DTLS     B,
            CIV_LN_ARREARS_TABLE C
      WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
        AND A.cod_acct_no = c.cod_acct_no
        AND C.COD_ARREAR_TYPE = 'C'
        AND FLG_MNT_STATUS = 'A'
        group by A.COD_ACCT_NO,
      A.AMT_ARREARS_PRINC)
      where arrear_balance_total<>arrear_table_total;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 438');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 438',
                            1001);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_balances',
           'CRITICAL',
           var_l_count ||
           '  Loans Where AMT_ARREARS_PRINC <> SUM ( PRINCIPAL ARREARS ) In Arrears Table.',
           '438');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 438');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 438',
                              1014);
      END;
    END IF;
    commit;
    /*    BEGIN
      SELECT COUNT(1)
        INTO var_l_count
        FROM civ_LN_ACCT_DTLS
       WHERE cod_sched_type NOT IN
             (SELECT DISTINCT cod_sched_type
                FROM ln_sched_types
               WHERE flg_mnt_status = 'A')
         AND cod_cc_brn = var_cod_cc_brn;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 444');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 444',
                      1077);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  Loans With Schedules Not In LN_SCHED_TYPE.',
           '444');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 444');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 444',
                        1089);
      END;
    END IF;
    commit;*/
    BEGIN
      ---check if this is realy required now
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM civ_LN_ACCT_DTLS
       WHERE (flg_past_due_status NOT IN
             (select distinct flg_past_due_status
                 from cbsfchost.ac_crr_codes
                where flg_mnt_status = 'A') OR flg_past_due_status IS NULL)
                and flg_mnt_status ='A'/*
         AND cod_cc_brn = var_cod_cc_brn*/;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check No 445');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check No 445',
                            1106);
    END;
    If var_l_count > 0 Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  Accounts With Incorrect FLG_PAST_DUE_STATUS.',
           '445');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 445');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 445',
                              1118);
      END;
    END IF;
    commit;
    /*BEGIN
      SELECT \*+PARALLEL(4) *\
       COUNT(A.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_acct_schedule_detls A, civ_LN_ACCT_DTLS B
       WHERE A.cod_acct_no = B.cod_acct_no
         --AND B.cod_cc_brn = var_cod_cc_brn
         AND A.date_instal >= var_dat_process
         AND A.amt_charge_outst = 0
         AND B.cod_sched_type IN
             (SELECT cod_sched_type
                FROM ln_sched_types C, ln_inst_rules D
               WHERE C.cod_instal_rule = D.cod_inst_rule
                 AND C.cod_prod = B.cod_prod
                 AND D.cod_inst_calc_method = 'EPI'
                 AND C.cod_charge_rule IS NOT NULL
                 AND C.flg_mnt_status = 'A'
                 AND D.flg_mnt_status = 'A'); -- Select All Schedules in Which Commision is Defined.
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 463');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 463',
                            1148);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ LN_ACCT_SCHEDULE_DETLS',
           'CRITICAL',
           var_l_count ||
           '  Loans Where AMT_CHARGE_OUTST = ZERO, But COMMISION DEFINED.',
           '465');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 463');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 463',
                              1160);
      END;
    END IF;
    commit;*/
   /* BEGIN
      SELECT \*+PARALLEL(4) *\
       COUNT(A.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_acct_schedule_detls A, civ_LN_ACCT_DTLS B
       WHERE A.cod_acct_no = B.cod_acct_no
         --AND B.cod_cc_brn = var_cod_cc_brn
         AND A.date_instal >= var_dat_process
         AND A.amt_charge_outst <> 0
         AND B.cod_sched_type IN
             (SELECT cod_sched_type
                FROM ln_sched_types C, ln_inst_rules D
               WHERE C.cod_instal_rule = D.cod_inst_rule
                 AND C.cod_prod = B.cod_prod
                 AND D.cod_inst_calc_method = 'EPI'
                 AND C.cod_charge_rule IS NULL
                 AND C.flg_mnt_status = 'A'
                 AND D.flg_mnt_status = 'A'); -- Select All Schedules in Which Commision is Defined.
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 464');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 464',
                            1189);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_count ||
           '  Loans Where AMT_CHARGE_OUTST <> zero, But COMMISION NOT DEFINED.',
           '464');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 464');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 464',
                              1202);
      END;
    END IF;
   */ commit;
    --             Check bal_int1_accr Vs bal_int1_chg For UnMatured Normal Accounts.
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(DISTINCT A.cod_acct_no)
        INTO var_l_count
        FROM CIV_LN_ACCT_INT_BALANCE_DTLS A,
             civ_LN_ACCT_DTLS             B,
             co_ln_acct_accrual_amt       c --cv_ln_acct_accrual_amt
       WHERE A.cod_cc_brn = B.cod_cc_brn
         AND A.cod_acct_no = B.cod_acct_no
         and c.cod_Acct_no = b.cod_Acct_No
         and flg_mnt_status ='A'
         AND B.flg_accr_status = 'N'
         AND B.dat_of_maturity >= var_dat_last_process
            --AND                   B.dat_last_charged         <>           var_dat_last_process
         AND A.bal_int1_accr < A.bal_int1_chg
         and c.amt_accrual < (A.bal_int1_accr - A.bal_int1_chg);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 471');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 471',
                            1226);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'CIV_LN_ACCT_INT_BALANCE_DTLS',
           'CRITICAL',
           var_l_count ||
           '  REGULAR ( NORMAL ) Loans Where BAL_INT1_ACCR < BAL_INT1_CHG.',
           '471');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 471');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 471',
                              1238);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+PARALLEL(4) */
       COUNT(1)
        INTO var_l_count
        FROM (SELECT COUNT(1)
                FROM civ_ln_acct_balances a, civ_LN_ACCT_DTLS b
               WHERE /*b.cod_cc_brn = var_cod_cc_brn
                 and */a.cod_acct_no = b.cod_acct_no
                 and flg_mnt_status ='A'
               GROUP BY a.cod_acct_no
              HAVING COUNT(1) > 1);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select for Consistency Check 478 failed. ');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select for Consistency Check 478 failed. ',
                            1260);
    END;
    IF (var_l_count > 0) Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_balances',
           'CRITICAL',
           var_l_count ||
           '  RECORDS HAVE SIMILAR ACCT NO, UNIQUE CONSTRAINT WILL BE VIOLATED IN PRODUCTION',
           '478');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Insert for Consistency Check 478 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 478 failed. ',
                              1271);
      END;
    END IF;
    commit;
    BEGIN
      --ak change start
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_instal_rule
                FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
               where a.cod_instal_rule not in
                     (select cod_inst_rule
                        from cbsfchost.ln_inst_rules
                       where flg_mnt_status = 'A')
                       and b.flg_mnt_status ='A'
                -- and b.cod_cc_brn = var_cod_cc_brn
                 and a.cod_acct_no = b.cod_acct_no);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select for Consistency Check 480 failed. ');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select for Consistency Check 480 failed. ',
                            1297);
    END; ----ak change end
    IF (var_l_count > 0) Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           '  INVALID INSTALLMENT RULES IN PRESENT IN THE LOAN SCHEDULES',
           '480');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Insert for Consistency Check 480 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 480 failed. ',
                              1308);
      END;
    END IF; commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT b.cod_prod, a.cod_sched_type
                FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
               WHERE a.cod_acct_no = b.cod_acct_no
               and b.flg_mnt_status ='A'
                -- AND b.cod_cc_brn = var_cod_cc_brn

              MINUS
              SELECT cod_prod, cod_sched_type
                FROM cbsfchost.ln_sched_types
               WHERE flg_mnt_status = 'A');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select for Consistency Check 481 failed. ');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select for Consistency Check 481 failed. ',
                            1334);
    END;
    IF (var_l_count > 0) Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           '  INVALID PRODUCT SCHEDULES XFREF RULES IN PRESENT IN THE LOAN SCHEDULES',
           '481');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Insert for Consistency Check 481 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 481 failed. ',
                              1345);
      END;
    END IF; commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_LN_ACCT_DTLS a
       WHERE /*a.cod_cc_brn = var_cod_cc_brn
         AND */a.dat_last_disb IS NULL
         AND a.cod_acct_no IN
             (SELECT b.cod_acct_no
                FROM civ_ln_acct_balances b, civ_LN_ACCT_DTLS c
               WHERE b.amt_disbursed > 0
                 AND c.cod_acct_no = b.cod_acct_no)
                 and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select for Consistency Check 482 failed. ');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select for Consistency Check 482 failed. ',
                            1363);
    END;
    IF (var_l_count > 0) Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           ' LOAN ACCOUNTS WHERE AMT_DISBURSED>zero AND DAT_LAST_DISBURSEMENT IS NULL',
           '482');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Insert for Consistency Check 482 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 482 failed. ',
                              1374);
      END;
    END IF; commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_balances a, civ_LN_ACCT_DTLS b
       WHERE /*b.cod_cc_brn = var_cod_cc_brn
         AND*/ a.cod_Acct_no = b.cod_acct_no
         AND amt_net_disbursed > amt_disbursed
         and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 483');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 483',
                            1392);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           '  LOANS WHERE AMT_NET_DISBURSED > AMT_DISBURSED.',
           '483');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 483');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 483',
                              1404);
      END;
    END IF; commit;
    --commit;
    --commit;
    /*    BEGIN
      SELECT COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_acct_no, a.cod_sched_type
                FROM civ_ln_acct_schedule a);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 485');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 485',
                      1466);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           '  RECORDS SCHEDULE CODES NOT IN SYNC WITH ACCT_DTLS',
           '485');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 485');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 485',
                        1478);
      END;
    END IF;
    commit;*/
    --commit;

    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_balances a, civ_LN_ACCT_DTLS b
       WHERE (a.amt_princ_balance < 0 or a.amt_arrears_princ < 0 or
             a.amt_disbursed < 0)
        -- and b.Cod_Cc_Brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
         and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 493');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 493',
                            1625);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_balances',
           'CRITICAL',
           var_l_count ||
           '  ACCOUNTS WHERE principal balance is less than zero',
           '493');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 493');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 493',
                              1637);
      END;
    END IF; commit;
    --commit;


    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls a
       WHERE/* Cod_Cc_Brn = var_cod_cc_brn
         and */not exists (select 1
                from cbsfchost.ba_cc_brn_mast
               where cod_cc_brn = a.cod_cc_brn)
               and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 401');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 495',
                            149);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'Critical',
           var_l_count ||
           '  accounts where branch does not exist in ba_cc_brn_mast',
           '495');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 401');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 495',
                              162);
      END;
    END IF; commit;
    --commit;

    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls a
       WHERE /*Cod_Cc_Brn = var_cod_cc_brn
         and */not exists (select 1
                from civ_ci_custmast --civ_ci_custmast
               where cod_cust_id = a.cod_cust_id)
               and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 401');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 496',
                            149);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'Critical',
           var_l_count ||
           '  accounts where customer does not exist in ci_custmast',
           '496');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 401');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 495',
                              162);
      END;
    END IF; commit;
    --commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_LN_ACCT_DTLS
       WHERE/* cod_cc_brn = var_cod_cc_brn
         AND */dat_last_disb < dat_first_disb
         and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 429');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 497',
                            1847);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count || '  Loans Where DAT_LAST_DISB > DAT_FIRST_DISB',
           '497');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 497',
                              762);
      END;
    END IF; commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_schedule a, civ_ln_acct_dtls b
       where a.cod_acct_no = b.cod_accT_no
        -- and b.cod_cc_brn = var_cod_cc_brn
         and (cod_instal_rule is null or cod_int_rule is null or
             cod_ioa_rule is null or cod_ioa_rate is null or
             cod_ppf_rate is null or cod_ppf_rule is null or
             cod_efs_rule is null or cod_efs_rate is null or
             cod_term_val_rule is null or cod_instal_datepart is null)
             and b.flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 429');
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 498',
                            1901);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_SCHEDULE',
           'CRITICAL',
           var_l_count ||
           '  Loans Where con_instal_rule/cod_int_rule/cod_ioa_rule/cod_ioa_rate/cod_ppf_rate/cod_ppf_rule/cod_efs_rule/cod_efs_rate/cod_term_val_rule/cod_instal_datepart is null',
           '498');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 498',
                              762);
      END;
    END IF; commit;
    --446- 462
    /*  BEGIN
      SELECT --/*+ PARALLEL(5)
       COUNT(1)
        INTO var_l_count
        from civ_ln_acct_payinstrn a, civ_LN_ACCT_DTLS b
       WHERE b.cod_cc_brn = var_cod_cc_brn
         and a.cod_acct_no = b.cod_acct_no
         and a.cod_remitter_acct NOT IN
             (select cod_acct_no
                from ch_acct_mast
               where flg_mnt_status = 'A');
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
        /*  cbsfchost.ora_raiserror(SQLCODE,
        'Select Failed: Consistency Check 446',
        2001);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_PAYINSTRM',
           'CRITICAL',
           var_l_count ||
           ' Loans Where remitter account is not a valid CASA account',
           '446');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 446',
                        2002);
      END;
    END IF;*/
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(DISTINCT a.cod_acct_no)
        INTO var_l_count
        FROM civ_ln_arrears_table a, civ_LN_ACCT_DTLS b
       WHERE a.cod_rule_id = 0
       --  and b.cod_cc_brn = var_cod_cc_brn
       and a.amt_Arrears_due > 0
         and a.cod_acct_no = b.cod_acct_no
         and a.cod_arrear_type IN ('I', 'N', 'A', 'L')
         and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 447',
                            2003);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ARREARS_TABLE',
           'CRITICAL',
           var_l_count ||
           ' Loans Where civ_ln_arrears_table.cod_rule_id is zero for Interest type arrears',
           '447');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 447',
                              2004);
      END;
    END IF; commit;
    /* BEGIN
      SELECT --/*+ PARALLEL(5)
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls b
       WHERE b.cod_cc_brn = var_cod_cc_brn
         and cod_acct_no NOT IN
             (select cod_acct_no from civ_ln_acct_rep_rev);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 452',
                      2079);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_REPRICING_REV',
           'CRITICAL',
           var_l_count ||
           ' Loans accounts where records are not present in table civ_LN_ACCT_REPRICING_REV',
           '452');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 447',
                        2004);
      END;
    END IF;*/
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_dtls a
       WHERE a.ctr_disb = 0
        -- AND a.cod_cc_brn = var_cod_cc_brn
         AND a.dat_first_disb IS NOT NULL
         AND cod_acct_stat <> 1;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 453',
                            2079);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_LN_ACCT_DTLS',
           'CRITICAL',
           var_l_count ||
           ' Loans accounts where crt_disb is zero but dat_first_disb is not NULL in table civ_LN_ACCT_DTLS',
           '453');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 447',
                              2004);
      END;
    END IF; commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_arrears_table a, civ_LN_ACCT_DTLS b
       WHERE dat_arrears_due < dat_arrears_assessed
       and flg_mnt_status ='A'
        -- and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 454',
                            2144);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count ||
           '  Loans Where dat_arrears_due < dat_arrears_assessed in table civ_ln_arrears_table',
           '454');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 454',
                              2160);
      END;
    END IF; commit;
    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
       WHERE/* b.cod_cc_brn = var_cod_cc_brn
         and */b.cod_acct_no = a.cod_acct_no
         and b.flg_mnt_status ='A'
         and cod_instal_rule in
             (select cod_inst_rule
                from cbsfchost.ln_inst_rules
               where cod_inst_calc_method in ('MOR', 'IOI', 'PMI'))
         and amt_princ_repay <> 0;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 455',
                            2180);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           'Loans Where amt_pric_repay should be zero for IOI-PMI stage in table civ_ln_acct_schedule',
           '455');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 433');
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 455',
                              2201);
      END;
    END IF; commit;

    /*   BEGIN
      SELECT COUNT(1)
        INTO var_l_count
        FROM (SELECT cod_Acct_no
                FROM (SELECT \*+PARALLEL(4) *\
                       a.cod_Acct_no,
                       a.amt_disbursed,
                       a.amt_princ_balance,
                       a.amt_arrears_princ,
                       NVL(d.amt_arrears_due, 0) C_TYPE_ARREAR,
                       a.amt_arrears_princ - NVL(d.amt_arrears_due, 0) DIFF_PRINC_ARREARS,
                       NVL(amt_arrears_assessed, 0),
                       a.amt_princ_balance + NVL(amt_arrears_assessed, 0) -
                       NVL(d.amt_arrears_due, 0) MANUAL_REPAY_AMOUNT,
                       b.amt_princ_repay
                        from civ_ln_Acct_balances a,
                             (select cod_Acct_no,
                                     sum(amt_princ_repay) amt_princ_repay
                                from civ_ln_acct_schedule
                               group by cod_acct_no) b,
                             civ_ln_arrears_table d,
                             civ_ln_acct_dtls e
                       WHERE e.cod_cc_brn = var_cod_cc_brn
                         AND a.cod_acct_no = e.cod_acct_no
                         AND a.cod_Acct_no = b.cod_Acct_no
                         AND a.cod_acct_no = d.cod_Acct_no(+)
                         AND d.cod_arrear_type = 'C'
                         AND NOT EXISTS
                       (select 1
                                from civ_Ln_Acct_Mor_Dtls h,
                                     civ_ln_acct_Schedule i
                               WHERE ba_global.dat_process BETWEEN
                                     dat_stage_Start and dat_stage_end
                                 and cod_instal_rule = 5
                                 and h.cod_Acct_no = i.cod_Acct_no
                                 and h.cod_acct_no = a.cod_Acct_no)
                         AND (a.amt_princ_balance +
                             NVL(amt_arrears_assessed, 0) -
                             NVL(d.amt_arrears_due, 0)) <> AMT_PRINC_REPAY
                         AND amt_disbursed <> amt_princ_Repay)
              UNION
              SELECT l.cod_Acct_no
                FROM (select \*+PARALLEL(4) *\
                       h.cod_Acct_no cod_Acct_no,
                       h.amt_mor_int amt_mor_int,
                       I.Ctr_Stage_No,
                       j.nam_stage,
                       j.ctr_stage_no,
                       j.amt_princ_repay,
                       k.amt_arrears_princ,
                       k.amt_princ_balance,
                       (k.amt_princ_balance + h.amt_mor_int) manual_calc_amt_princ_repay
                        from civ_Ln_Acct_Mor_Dtls h,
                             civ_ln_acct_balances k,
                             civ_ln_acct_Schedule i,
                             civ_ln_acct_Schedule j,
                             civ_ln_Acct_dtls     m
                       WHERE m.cod_cc_brn = var_cod_cc_brn
                         AND k.cod_acct_no = m.cod_Acct_no
                         AND ba_global.dat_process BETWEEN i.dat_stage_Start and
                             i.dat_stage_end
                         and i.cod_instal_rule = 5
                         and h.cod_Acct_no = i.cod_Acct_no
                         and j.cod_Acct_no = h.cod_Acct_no
                         and j.ctr_stage_no = (I.Ctr_Stage_No + 1)
                         and k.cod_acct_no = h.cod_Acct_no
                         and ((k.amt_princ_balance + h.amt_mor_int) -
                             j.amt_princ_repay) <> 0) l);

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 456',
                      2220);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           'Loans Where sum of amt_pric_repay should be equal to amt_disbursed in table civ_ln_acct_schedule',
           '456');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 456',
                        2240);
      END;
    END IF;*/
    /*BEGIN
      SELECT --\*+ PARALLEL(5) *\
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM rate_chart_mapping a, civ_LN_ACCT_DTLS b
       WHERE b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
         and a.cod_rate_chart not in
             (select cod_rate_chart from pr_rate_chart);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 459',
                      2351);
    END;*/
   /* IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'rate_chart_mapping',
           'CRITICAL',
           var_l_count ||
           'Accounts in In rate_chart_mapping table,the used cod_rate_chart is not present in PR_RATE_CHART',
           '459');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 460',
                              2370);
      END;
    END IF; commit;*/

    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_acct_rates_detl a, civ_LN_ACCT_DTLS b
       WHERE/* b.cod_cc_brn = var_cod_cc_brn
         and */b.cod_acct_no = a.cod_acct_no
         and a.cod_int_index_slab not in
             (select cod_int_indx from cbsfchost.ba_int_indx_rate);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 460',
                            2354);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'ln_acct_rates',
           'CRITICAL',
           var_l_count ||
           'Loans Where  used cos_int_indx is not present in ba_int_indx_mast or ba_int_indx_rate',
           '460');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 460',
                              2370);
      END;
    END IF; commit;

    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_arrears_table a, civ_LN_ACCT_DTLS b
       WHERE a.cod_arrear_type = 'P'
        -- and b.cod_cc_brn = var_cod_cc_brn
         and b.cod_acct_no = a.cod_acct_no
         and NVL(a.COD_INSURANCE, 0) not in
             (select cod_insurance
                from cbsfchost.ba_insurance_mast
               where flg_mnt_status = 'A')
               and flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 461',
                            2392);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'ln_arrears_table',
           'CRITICAL',
           var_l_count ||
           'Loans Where ln_arrears_table.cod_insurance is NULL or cod_insurance is not present in ba_insurance_mast',
           '461');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 461',
                              2410);
      END;
    END IF; commit;

    /*BEGIN
      SELECT --/*+ PARALLEL(5)
       COUNT(DISTINCT A.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_arrears_table a, civ_LN_ACCT_DTLS b
       WHERE a.cod_arrear_type IN ('D', 'F')
         AND b.cod_cc_brn = var_cod_cc_brn
         AND b.cod_acct_no = a.cod_acct_no
         AND NVL(cod_arrear_charge, 0) not in
             (select cod_sc from ba_sc_code where flg_mnt_status = 'A');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 462',
                      2439);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'ln_arrears_table',
           'CRITICAL',
           var_l_count ||
           'Loans Where ln_arrears_table.cod_arrear_charge  is NULL or sc_code is not present in ba_sc_code(day-zero)',
           '462');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 462',
                        2410);
      END;
    END IF;*/

    BEGIN
      SELECT /*+PARALLEL(4)*/
       COUNT(DISTINCT a.COD_ACCT_NO)
        INTO var_l_count
        FROM civ_ln_arrears_table a, civ_LN_ACCT_DTLS b
       WHERE/* b.cod_cc_brn = var_cod_cc_brn
         AND */b.cod_acct_no = a.cod_acct_no
         and flg_mnt_status ='A'
         AND a.amt_arrears_due > a.amt_arrears_assessed;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 463',
                            2479);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           var_cod_cc_brn,
           'ln_arrears_table',
           'CRITICAL',
           var_l_count ||
           'Loans Where ln_arrears_table.amt_arrears_due  > amt_arrears_assessed',
           '463');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 462',
                              2480);
      END;
    END IF; commit;
   /* UPDATE conv_brn_stream_proc_xref
       SET flg_processed = 'Y'
     WHERE cod_stream_id = var_cod_stream_id
       AND cod_cc_brn = var_cod_cc_brn
       AND cod_proc_nam = 'AP_CO_CONSIS_CHECK_LN';*/
    COMMIT;
    --ap_co_ins_consis_proc_time('ap_co_consis_check_ln',var_cod_cc_brn,2);
 -- END LOOP;

  COMMIT;
  RETURN 0;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

    cbsfchost.ora_raiserror(SQLCODE,
                        'Execution of ap_co_consis_check_ln failed',
                        1655);
    RETURN 95;
END;
/
