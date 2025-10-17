CREATE OR REPLACE FUNCTION "AP_CO_CONSIS_CHECK_LN_3_NOSTREAM" (var_cod_stream_id NUMBER)
  RETURN NUMBER AS
  var_dat_process      DATE;
  var_dat_last_process DATE;
  var_dat_month_end    DATE;
  var_l_count          NUMBER;
 -- var_cod_cc_brn       NUMBER;
  var_cod_entity_vpd   NUMBER(5);
  var_bank_mast_dt_to_use VARCHAR2(1) := 'N';--nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'), 'Y');
  CURSOR consis_brn IS
    SELECT cod_cc_brn
      FROM conv_brn_stream_proc_xref a
     WHERE a.cod_proc_nam = 'AP_CO_CONSIS_CHECK_LN_3'
       AND a.cod_stream_id = var_cod_stream_id
       AND a.flg_processed = 'N';
BEGIN
  select dat_process,dat_last_process into var_dat_process,var_dat_last_process from cbsfchost.ba_bank_mast;

   /* IF ( var_bank_mast_dt_to_use = 'N' ) THEN
        var_dat_process := nvl(ap_get_data_mig_param('DAT_PROCESS'), var_dat_process);
        var_dat_last_process := nvl(ap_get_data_mig_param('DAT_LAST_PROCESS'), var_dat_last_process);
    END IF;*/

  /*BEGIN
    SELECT dat_process INTO var_dat_process FROM ba_bank_mast;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select From ba_bank_mast Failed.');
      ora_raiserror(SQLCODE, 'Select From ba_bank_mast Failed.', 55);
  END;*/
  var_dat_month_end := last_day(var_dat_process);
  --EXECUTE IMMEDIATE 'ALTER SESSION SET PARALLEL_DEGREE_POLICY = AUTO';
  /*BEGIN
    SELECT dat_last_process INTO var_dat_last_process FROM ba_bank_mast;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select From ba_bank_mast Failed.');
      ora_raiserror(SQLCODE, 'Select From ba_bank_mast Failed.', 56);
  END;*/
  /*  BEGIN
      SELECT cod_entity_vpd INTO var_cod_entity_vpd FROM ba_bank_mast;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select From ba_bank_mast Failed.');
        ora_raiserror(SQLCODE, 'Select From ba_bank_mast Failed.', 57);
    END;
  */
 -- FOR consis_brn_rec IN consis_brn LOOP
   /* IF var_cod_cc_brn <> consis_brn_rec.cod_cc_brn then
      COMMIT;
    END IF;*/
  --  var_cod_cc_brn := consis_brn_rec.cod_cc_brn;

    /*    ap_co_ins_consis_proc_time('ap_co_consis_check_ln_2',
                                   0,
                                   1);
    */
    BEGIN
      DELETE
      FROM co_warn_table
       WHERE /*cod_cc_brn IN (0, 0)
         AND */check_no BETWEEN 1201 AND 1400;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Delete FROM co_warn_table failed in ap_co_consis_check_ln_2.');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Delete failed in ap_co_consis_check_ln_2.',
                      73);
    END;
    COMMIT;
    /*BEGIN
      Select \*+parallel(128)*\
       count(1)
        INTO var_l_count
        from civ_ln_acct_ledg
       where dat_post > dat_value\*
         AND cod_cc_brn_txn = var_cod_cc_brn*\;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1203');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 1203',
                      146);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_ledg',
           'INFO',
           var_l_count ||
           ' RECORDS WHERE DAT_POST is greater than DAT_VALUE',
           '1203');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1203');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 1203',
                        159);
      END;
    END IF;*/
    commit;
    BEGIN
      Select /*+parallel(4) nologging*/
       count(1)
        INTO var_l_count
        from civ_ln_acct_ledg
       where amt_txn_acy = 0/*
         AND cod_cc_brn_txn = var_cod_cc_brn*/; --<=
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 1204',
                      146);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_ledg',
           'CRITICAL',
           var_l_count ||
           ' RECORDS WHERE transaction ACY is less than or equal to ZERO',
           '1204');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 1204',
                        159);
      END;
    END IF;
    commit;
--    BEGIN
--      Select /*+parallel(4) nologging*/
--       count(1)
--        INTO var_l_count
--        from civ_ba_coll_hdr
--       where cod_coll NOT IN (select cod_coll from ba_coll_codes)
--         and cod_coll_homebrn = var_cod_cc_brn;
--    EXCEPTION
--      WHEN OTHERS THEN
--        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
--        cbsfchost.ora_raiserror(SQLCODE,
--                      'Select Failed: Consistency Check 1205',
--                      146);
--    END;
--    IF var_l_count > 0 THEN
--      BEGIN
--        INSERT INTO co_warn_table
--          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--        VALUES
--          ('LN',
--           0,
--           'civ_ba_coll_hdr',
--           'CRITICAL',
--           var_l_count ||
--           ' RECORDS WHERE collateral code is not present in ba_coll_codes',
--           '1205');
--      EXCEPTION
--        WHEN OTHERS THEN
--          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
--          cbsfchost.ora_raiserror(SQLCODE,
--                        'Insert Failed: Consistency Check No 1204',
--                        159);
--      END;
--    END IF;
--    commit;

--    BEGIN
--      Select /*+parallel(4) nologging*/
--       count(1)
--        INTO var_l_count
--        from civ_ln_acct_ledg
--       where NVL(cod_sc, 0) NOT IN (Select cod_sc from ba_sc_code)
--         and NVL(cod_sc, 0) != 0
--         and cod_cc_brn_txn = var_cod_cc_brn;
--    EXCEPTION
--      WHEN OTHERS THEN
--        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
--        cbsfchost.ora_raiserror(SQLCODE,
--                      'Select Failed: Consistency Check 1207',
--                      146);
--    END;
--    IF var_l_count > 0 THEN
--      BEGIN
--        INSERT INTO co_warn_table
--          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--        VALUES
--          ('LN',
--           0,
--           'civ_ln_acct_ledg',
--           'CRITICAL',
--           var_l_count ||
--           ' RECORDS WHERE Service charge code is not present in ba_sc_code',
--           '1207');
--      EXCEPTION
--        WHEN OTHERS THEN
--          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
--          cbsfchost.ora_raiserror(SQLCODE,
--                        'Insert Failed: Consistency Check No 1204',
--                        159);
--      END;
--    END IF;
    commit;
--    BEGIN
--      Select /*+parallel(4) nologging*/
--       count(1)
--        INTO var_l_count
--        from civ_ba_acct_memo
--       where dat_acct_memo > (Select dat_process from ba_bank_mast);
--
--    EXCEPTION
--      WHEN OTHERS THEN
--        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
--        cbsfchost.ora_raiserror(SQLCODE,
--                      'Select Failed: Consistency Check 1209',
--                      146);
--    END;
--    IF var_l_count > 0 THEN
--      BEGIN
--        INSERT INTO co_warn_table
--          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--        VALUES
--          ('LN',
--           0,
--           'civ_ba_acct_memo',
--           'CRITICAL',
--           var_l_count ||
--           ' RECORDS WHERE account date memo is greater than ba_bank_mast',
--           '1209');
--      EXCEPTION
--        WHEN OTHERS THEN
--          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
--          cbsfchost.ora_raiserror(SQLCODE,
--                        'Insert Failed: Consistency Check No 1204',
--                        159);
--      END;
--    END IF;
--    commit;
--    BEGIN
--      Select /*+parallel(4) nologging*/
--       count(1)
--        INTO var_l_count
--        from civ_ba_coll_hdr
--       where dat_deed_return < '01-JAN-1950'
--         and cod_coll_homebrn = var_cod_cc_brn;
--    EXCEPTION
--      WHEN OTHERS THEN
--        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
--        cbsfchost.ora_raiserror(SQLCODE,
--                      'Select Failed: Consistency Check 1208',
--                      146);
--    END;
--    IF var_l_count > 0 THEN
--      BEGIN
--        INSERT INTO co_warn_table
--          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--        VALUES
--          ('LN',
--           0,
--           'civ_ba_coll_hdr',
--           'CRITICAL',
--           var_l_count ||
--           ' RECORDS WHERE dat_deed_return is less than or equal to  FIRST-JAN-FIFTY',
--           '1208');
--      EXCEPTION
--        WHEN OTHERS THEN
--          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
--          cbsfchost.ora_raiserror(SQLCODE,
--                        'Insert Failed: Consistency Check No 1204',
--                        159);
--      END;
--    END IF;
--    commit;
--    BEGIN
--      Select /*+parallel(4) nologging*/
--       count(1)
--        INTO var_l_count
--        from civ_ba_coll_hdr
--       where cod_coll NOT IN (select cod_coll from ba_coll_codes)
--         and cod_coll_homebrn = var_cod_cc_brn;
--    EXCEPTION
--      WHEN OTHERS THEN
--        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
--        cbsfchost.ora_raiserror(SQLCODE,
--                      'Select Failed: Consistency Check 1205',
--                      146);
--    END;
--    IF var_l_count > 0 THEN
--      BEGIN
--        INSERT INTO co_warn_table
--          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--        VALUES
--          ('LN',
--           0,
--           'civ_ba_coll_hdr',
--           'CRITICAL',
--           var_l_count ||
--           ' RECORDS WHERE collateral code is not present in ba_coll_codes',
--           '1205');
--      EXCEPTION
--        WHEN OTHERS THEN
--          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
--          cbsfchost.ora_raiserror(SQLCODE,
--                        'Insert Failed: Consistency Check No 1204',
--                        159);
--      END;
--    END IF;
--    commit;
    /*    BEGIN
          Select count(1)
            INTO var_l_count
            from civ_ln_acct_ledg
           where amt_txn_acy <= 0
             AND cod_cc_brn_txn = var_cod_cc_brn;
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select Failed: Consistency Check 1204',
                          146);
        END;
        IF var_l_count > 0 THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
            VALUES
              ('LN',
               0,
               'civ_ln_acct_ledg',
               'CRITICAL',
               var_l_count ||
               ' RECORDS WHERE transaction ACY is less than or equal to 0',
               '1204');
          EXCEPTION
            WHEN OTHERS THEN
              --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert Failed: Consistency Check No 1204',
                            159);
          END;
        END IF;
        commit;
    */ /*    BEGIN
                        Select count(1)
                          INTO var_l_count
                          from civ_ln_acct_ledg
                         where dat_post > dat_value
                           AND cod_cc_brn_txn = var_cod_cc_brn;
                      EXCEPTION
                        WHEN OTHERS THEN
                          --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1203');
                          cbsfchost.ora_raiserror(SQLCODE,
                                        'Select Failed: Consistency Check 1203',
                                        146);
                      END;
                      IF var_l_count > 0 THEN
                        BEGIN
                          INSERT INTO co_warn_table
                            (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
                          VALUES
                            ('LN',
                             0,
                             'civ_ln_acct_ledg',
                             'CRITICAL',
                             var_l_count ||
                             ' RECORDS WHERE DAT_POST is greater than DAT_VALUE',
                             '1203');
                        EXCEPTION
                          WHEN OTHERS THEN
                            --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1203');
                            cbsfchost.ora_raiserror(SQLCODE,
                                          'Insert Failed: Consistency Check No 1203',
                                          159);
                        END;
                      END IF;
                      commit;
                  */
    BEGIN
      SELECT /*+parallel(4) nologging*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_acct_no, a.dat_acct_open, a.dat_last_charged
                FROM civ_ln_acct_dtls a
           --    WHERE a.cod_cc_brn = var_cod_cc_brn
           
                 where a.dat_of_maturity < var_dat_process
                 and flg_Mnt_status ='A'
                 AND a.dat_last_charged IS NULL
                 AND a.dat_of_maturity !=
                     to_date('01-JAN-1950', 'DD-MON-YYYY'));
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1211');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 1211',
                      593);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_balances',
           'CRITICAL',
           var_l_count ||
           ' RECORDS where Dat_last_charged is null in PMI accounts',
           '1211');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1211');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 1211',
                        606);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+parallel(4) nologging*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_acct_no, a.dat_last_charged
              --, a.dat_last_accrual
                FROM civ_ln_acct_dtls     a,
                     civ_ln_acct_schedule d,
                     cbsfchost.ln_inst_rules             e,
                     civ_ln_arrears_table f
               WHERE a.cod_acct_no = d.cod_acct_no
               and a.flg_Mnt_status ='A'
                 AND d.cod_acct_no = f.cod_acct_no
                 AND d.cod_instal_rule = e.cod_inst_rule
                 AND e.cod_inst_calc_method = 'MOR'
                 AND d.ctr_stage_term != 0
           --      AND a.cod_cc_brn = var_cod_cc_brn
                    --AND a.cod_cc_brn = d.cod_cc_brn
                    --AND d.cod_cc_brn = f.cod_cc_brn
                 AND f.cod_arrear_type IN ('I', 'A')
                    --AND a.flg_mnt_status = 'A'
                    --AND a.cod_entity_vpd = var_cod_entity_vpd
                    --AND d.flg_mnt_status = 'A'
                    --AND d.cod_entity_vpd = var_cod_entity_vpd
                 AND e.flg_mnt_status = 'A');
      --AND e.cod_entity_vpd = var_cod_entity_vpd);
      --AND f.cod_entity_vpd = var_cod_entity_vpd);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1212');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 1212',
                      662);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count ||
           ' RECORDS WHERE MOR ACCOUNTS HAVING I TYPE ARREARS',
           '1212');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1212');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 1212',
                        675);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+parallel(4) nologging*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_acct_no, b.dat_last_charged, a.amt_princ_balance
                FROM civ_ln_acct_balances a, civ_ln_acct_dtls b
               WHERE a.cod_acct_no = b.cod_acct_no
           --      AND b.cod_cc_brn = var_cod_cc_brn
                 AND a.cod_cc_brn = b.cod_cc_brn
                 and flg_Mnt_status ='A'
                 AND a.amt_princ_balance = 0
                 AND b.dat_last_charged IS NULL
                 AND a.amt_disbursed != 0);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1213');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 1213',
                      702);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_dtls',
           'CRITICAL',
           var_l_count ||
           ' RECORDS WHERE Accounts which are having amt_princ_bal is ZERO and dat_last_charged is NULL',
           '1213');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1213');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 1213',
                        715);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+parallel(4) nologging*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT a.cod_acct_no,
                     e.cod_inst_calc_method,
                     d.dat_stage_start,
                     d.cod_instal_rule
                FROM civ_ln_acct_dtls a,
                     (Select cod_acct_no, ctr_stage_no
                        from civ_ln_acct_schedule
                       where dat_stage_start <= var_dat_process
                         and (dat_stage_end - 1) >= var_dat_process) c,
                     civ_ln_acct_schedule d,
                     cbsfchost.ln_inst_rules e
               WHERE a.cod_acct_no = c.cod_acct_no
                 AND c.cod_acct_no = d.cod_acct_no
                 AND c.ctr_stage_no = d.ctr_stage_no
                 and a.flg_Mnt_status ='A'
                 AND d.cod_instal_rule = e.cod_inst_rule
                 AND e.cod_inst_calc_method = 'PMI'
                 AND d.dat_stage_start > var_dat_process/*
                 AND a.cod_cc_brn = var_cod_cc_brn*/);
      --AND a.cod_cc_brn = d.cod_cc_brn
      --AND a.flg_mnt_status = 'A'
      --AND a.cod_entity_vpd = var_cod_entity_vpd
      --AND d.flg_mnt_status = 'A');
      --AND d.cod_entity_vpd = var_cod_entity_vpd);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1215');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select Failed: Consistency Check 1215',
                      851);
    END;
    IF var_l_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_arrears_table',
           'CRITICAL',
           var_l_count ||
           ' RECORDS WHERE Accounts which are in PMI and dat_stage_start is greater than Migration Date',
           '1215');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1215');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert Failed: Consistency Check No 1215',
                        864);
      END;
    END IF;
    commit;
    BEGIN
      SELECT /*+parallel(4) nologging*/
       COUNT(1)
        INTO var_l_count
        FROM civ_ln_acct_schedule a,
             civ_ln_acct_dtls     b,
             cbsfchost.ln_prod_int_attr          c
       WHERE a.cod_acct_no = b.cod_acct_no
      --   AND b.cod_cc_brn = var_cod_cc_brn
            --AND a.cod_cc_brn = b.cod_cc_brn
            AND b.flg_mnt_status = 'A'
         AND b.cod_prod = c.cod_prod
         
         AND (a.cod_int_rule NOT IN
             (SELECT d.cod_int_rule
                 FROM cbsfchost.ln_prod_int_attr d
                WHERE flg_mnt_status = 'A') OR
             a.cod_int_rule NOT IN
             (SELECT e.cod_int_rule
                 FROM cbsfchost.ln_int_rules e
                WHERE flg_mnt_status = 'A'));
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select for Consistency Check 1220 failed. ');
        cbsfchost.ora_raiserror(SQLCODE,
                      'Select for Consistency Check 1220 failed. ',
                      1125);
    END;
    IF (var_l_count > 0) Then
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           ' RECORDS FAILED FOR CHECK FOR INVALID COD_INT_RULE IN LN_ACCT_SCHEDULE',
           '1220');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert for Consistency Check 1220 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Insert for Consistency Check 1220 failed. ',
                        1136);
      END;
    END IF;
    commit;
    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          FROM (SELECT DISTINCT acctno, intrate
                  FROM (SELECT a.cod_acct_no     acctno,
                               c.cod_cc_brn,
                               c.cod_prod        prodcode,
                               a.cod_sched_type  schedtyp,
                               a.ctr_stage_no    stageno,
                               a.cod_instal_rule instalrule,
                               b.cod_int_rule    intrule,
                               b.cod_int_rate    intrate,
                               b.cod_ioa_rule    ioarule,
                               b.cod_ioa_rate    ioarate,
                               b.cod_ppf_rule    ppfrule,
                               b.cod_ppf_rate    ppfrate,
                               b.cod_efs_rule    efsrule,
                               b.cod_efs_rate    efsrate
                          FROM civ_ln_acct_schedule a,
                               cbsfchost.ln_sched_types            b,
                               civ_ln_acct_dtls     c
                         WHERE a.cod_acct_no = c.cod_acct_no
                           AND b.cod_prod = c.cod_prod
                           and c.flg_Mnt_status ='A'
                           AND b.cod_sched_type = a.cod_sched_type
                           AND a.cod_instal_rule = b.cod_instal_rule/*
                           AND c.cod_cc_brn = var_cod_cc_brn*/) schedpop
                --AND a.cod_cc_brn = c.cod_cc_brn) schedpop
                --AND c.flg_mnt_status = 'A') schedpop
                 WHERE (schedpop.prodcode, schedpop.intrate) IN
                       (SELECT cod_prod, ctr_int_srl
                          FROM cbsfchost.ln_prod_rates p
                        --where p.cod_rate_typ=0 -- EMI/FPI/IPI Stage not required as even other rule maybe tiered
                         WHERE p.cod_defn_typ = 1 -- Slab Type
                           AND p.cod_slab_typ = 2 -- Amt based Slab
                           AND p.cod_prod <> 0
                           AND p.flg_mnt_status = 'A')
           --     MINUS
--                SELECT distinct cod_acct_no, ctr_int_srl
--                  FROM civ_ln_acct_rates
--                 WHERE cod_defn_typ = 1
--                   AND cod_cc_brn = var_cod_cc_brn
);
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Select for Consistency Check 1221 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select for Consistency Check 1221 failed. ',
                        1180);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_schedule',
             'CRITICAL',
             var_l_count ||
             ' RECORDS WHICH TIERED TYPE IN LN_ACCT_SCHEDULE FOR WHICH LN_ACCT_RATES ARE NOT GIVEN',
             '1221');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Insert for Consistency Check 1221 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1221 failed. ',
                          1192);
        END;
      END IF;
      commit;
    END;
    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          FROM (SELECT a.*
                  FROM (SELECT cod_acct_no, cod_int_rate rate
                          FROM civ_ln_acct_schedule
                        --WHERE cod_cc_brn = var_cod_cc_brn
                        UNION
                        SELECT cod_acct_no, cod_ioa_rate rate
                          FROM civ_ln_acct_schedule
                        --WHERE cod_cc_brn = var_cod_cc_brn
                        UNION
                        SELECT cod_acct_no, cod_ppf_rate rate
                          FROM civ_ln_acct_schedule
                        --WHERE cod_cc_brn = var_cod_cc_brn
                        UNION
                        SELECT cod_acct_no, cod_efs_rate rate
                          FROM civ_ln_acct_schedule) a,
                       --WHERE cod_cc_brn = var_cod_cc_brn) a,
                       civ_ln_acct_dtls b
                 WHERE a.cod_acct_no = b.cod_acct_no
                      AND b.flg_mnt_status = 'A'
                 --  AND b.cod_cc_brn = var_cod_cc_brn
                   AND (b.cod_prod, a.rate) IN
                       (SELECT cod_prod, ctr_int_srl
                          FROM cbsfchost.ln_prod_rates
                         WHERE cod_slab_typ = 2
                           AND flg_mnt_status = 'A')
--                MINUS
--                SELECT cod_acct_no, ctr_int_srl rate
--                  FROM civ_ln_acct_rates
--                 WHERE cod_cc_brn = var_cod_cc_brn
);
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Select for Consistency Check 1223 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select for Consistency Check 1223 failed. ',
                        1265);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_LN_ACCT_RATES',
             'CRITICAL',
             var_l_count ||
             ' RECORDS WHERE INT_RATE, IOA_RATE, PPF_RATE, EFS_RATE NOT PRESENT',
             '1223');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Insert for Consistency Check 1223 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1223 failed. ',
                          1277);
        END;
      END IF;
      commit;
    END;
    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          FROM (SELECT distinct b.cod_acct_no
                  FROM civ_ln_acct_schedule a, civ_ln_acct_dtls b
                 WHERE a.cod_acct_no = b.cod_acct_no
                 and b.flg_Mnt_status ='A'
                  -- and b.cod_cc_brn = var_cod_cc_brn
                   and (cod_int_rule IS NULL OR cod_int_rate IS NULL));
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Select distinct for Consistency Check 1226 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1226 failed. ',
                        1401);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_schedule',
             'CRITICAL',
             var_l_count ||
             ' COD INT RULE OR INT RATE IS NULL IN LN ACCT SCHEDULE',
             '1226');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Insert for Consistency Check 1226 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1226 failed. ',
                          1413);
        END;
      END IF;
    END;
    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          FROM (SELECT cod_acct_no
                  FROM civ_ln_acct_dtls a
                 WHERE dat_last_ioa < dat_acct_open
                 and flg_Mnt_status ='A'/*
                   AND cod_cc_brn = var_cod_cc_brn*/);
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Select distinct for Consistency Check 1228 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1228 failed. ',
                        1488);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count || ' DAT_LAST_IOA IS LESS THAN DAT_ACCT_OPEN',
             '1228');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE,                          'Insert for Consistency Check 1228 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1228 failed. ',
                          1500);
        END;
      END IF;
      commit;
    END;
    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          FROM (SELECT DISTINCT  trim(a.cod_acct_no)
                  FROM civ_ln_acct_schedule a, civ_ln_acct_dtls b
                 WHERE a.cod_acct_no = b.cod_acct_no
                 and b.flg_Mnt_status ='A'
               --    AND b.cod_cc_brn = var_cod_cc_brn
                --AND b.cod_cc_brn = a.cod_cc_brn
                MINUS
                SELECT DISTINCT trim(a.cod_acct_no)
                  FROM civ_ln_acct_schedule a, civ_ln_acct_dtls b
                 WHERE a.cod_acct_no = b.cod_acct_no
                 and b.flg_Mnt_status ='A'
               --    AND b.cod_cc_brn = var_cod_cc_brn
                      --AND b.cod_cc_brn = a.cod_cc_brn
                   AND a.cod_instal_rule in
                       (SELECT cod_inst_rule
                          FROM cbsfchost.ln_inst_rules
                         WHERE cod_inst_calc_method = 'PMI'));
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Select distinct for Consistency Check 1229 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1229 failed. ',
                        1534);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count ||
             ' PMI ROW NOT PRESENT FOR ACCOUNTS IN ACCT SCHEDULE',
             '1229');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE,                          'Insert for Consistency Check 1229 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1229 failed. ',
                          1545);
        END;
      END IF;
      commit;
    END;
    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          FROM civ_ln_acct_dtls a
         WHERE a.dat_last_ioa <> var_dat_last_process
         and flg_Mnt_status ='A'/*
         WHERE a.dat_last_ioa <> var_dat_process/*
           AND a.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Select distinct for Consistency Check 1229 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1229 failed. ',
                        1536);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count ||
             ' Records where dat_last_ioa is not equal to process date',
             '1230');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE,                          'Insert for Consistency Check 1229 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1230 failed. ',
                          1547);
        END;
      END IF;
      commit;
    END;
    /*    BEGIN
          BEGIN
            SELECT COUNT(1)
              INTO var_l_count
              FROM civ_ln_acct_dtls
             WHERE COD_ACCT_DATE_BASIS NOT IN (1, 2)
               AND cod_cc_brn = var_cod_cc_brn;
          EXCEPTION
            WHEN OTHERS THEN
              --write_to_file(SQLCODE,                        'Select distinct for Consistency Check 1231 failed. ');
              cbsfchost.ora_raiserror(SQLCODE,
                            'Select distinct for Consistency Check 1231 failed. ',
                            1607);
          END;
          IF (var_l_count > 0) THEN
            BEGIN
              INSERT INTO co_warn_table
                (COD_MODULE,
                 COD_CC_BRN,
                 TABLE_NAME,
                 SEVERITY,
                 REMARKS,
                 CHECK_NO)
              VALUES
                ('LN',
                 0,
                 'civ_ln_acct_dtls',
                 'CRITICAL',
                 var_l_count ||
                 ' Records where COD_ACCT_DATE_BASIS is neither 1 or 2',
                 '1231');
            EXCEPTION
              WHEN OTHERS THEN
                --write_to_file(SQLCODE,                          'Insert for Consistency Check 1231 failed');
                cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 1231 failed. ',
                              1618);
            END;
          END IF;
          commit;
        END;
    */
    BEGIN
      BEGIN
        SELECT /* + PARALLEL(4) */
         COUNT(1)
          INTO var_l_count
          FROM civ_ln_acct_dtls a
         WHERE a.dat_first_disb <
               (select min(b.dat_eff_int_indx)
                  from civ_ln_acct_rates b
                 WHERE a.cod_acct_no = b.cod_acct_no
                   and b.ctr_int_srl = 0)
                   and flg_Mnt_status ='A'
        --   AND a.cod_cc_brn = var_cod_cc_brn
              -- Added to discount the accounts where fix+float rate is present
          /* AND a.cod_acct_no NOT IN
               (Select cod_acct_no from ln_acct_fix_float_rate);*/;
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Select distinct for Consistency Check 1233 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1233 failed. ',
                        1672);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count ||
             ' Records where dat_eff_int_indx is not equal to dat_first_disb ',
             '1233');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE,                          'Insert for Consistency Check 1233 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1233 failed. ',
                          1683);
        END;
      END IF;
      commit;
    END;

    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ln_acct_dtls a
         where not exists (select 1
                  from civ_ln_acct_schedule b
                 where a.cod_acct_no = b.cod_acct_no)
                 and flg_Mnt_status ='A'/*
           and a.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1301 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_schedule',
             'CRITICAL',
             var_l_count || ' Records where no schedule is provided',
             '1301');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1301 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;

    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ln_acct_dtls a
         where not exists (select 1
                  from civ_ln_arrears_table b
                 where a.cod_acct_no = b.cod_acct_no)
                 and flg_Mnt_status ='A'/*
           and a.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1303 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_arrears_table',
             'INFO-PRE', --CRITICAL. PRE#12124
             var_l_count || ' Records where no arrear details are provided',
             '1303');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1303 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;

    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ln_acct_dtls a
         where not exists (select 1
                  from civ_ln_acct_balances b
                 where a.cod_acct_no = b.cod_acct_no)
                 and flg_Mnt_status ='A'/*
           and a.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1304 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_balances',
             'CRITICAL',
             var_l_count ||
             ' Records where no balance details are provided',
             '1304');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1304 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;

    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ln_acct_dtls a
         where not exists (select 1
                  from civ_ln_acct_attributes b
                 where a.cod_acct_no = b.cod_acct_no)
                 and flg_Mnt_status ='A'/*
           and a.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1305 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_attributes',
             'CRITICAL',
             var_l_count ||
             ' Records where no attribute details are provided',
             '1305');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1305 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;

    BEGIN
      BEGIN
        SELECT /*+parallel(4) nologging*/
         COUNT(A.cod_acct_no)
          INTO var_l_count
          from civ_ln_acct_dtls a,
               (Select cod_acct_no, count(1)
                  from civ_ln_acct_schedule
                 group by cod_acct_no
                having count(1) = 1) b
         where a.COD_aCCT_NO = b.COD_ACCT_NO
         and flg_Mnt_status ='A'/*
           AND A.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1308 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_schedule',
             'CRITICAL',
             var_l_count || ' accounts where only one stage is defined ',
             '1308');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1308 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;


    BEGIN
      BEGIN
        SELECT /*+parallel(4)*/COUNT(cod_acct_no)
          INTO var_l_count
          from civ_ln_acct_dtls
         where dat_last_charged > var_dat_process/*
           AND cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1314 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count ||
             ' accounts where last int charging is greater than process date ',
             '1314');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1314 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;
    /*    BEGIN
          BEGIN
            SELECT COUNT(A.cod_acct_no)
              INTO var_l_count
              from civ_ln_acct_dtls a,
                   (Select cod_acct_no, count(1)
                      from civ_ln_acct_schedule_detls
                     group by cod_acct_no
                    having count(1) <= 3) b
             where a.COD_aCCT_NO = b.COD_ACCT_NO
               AND A.cod_cc_brn = var_cod_cc_brn;
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Select distinct for Consistency Check 1315 failed. ',
                            1702);
          END;
          IF (var_l_count > 0) THEN
            BEGIN
              INSERT INTO co_warn_table
                (COD_MODULE,
                 COD_CC_BRN,
                 TABLE_NAME,
                 SEVERITY,
                 REMARKS,
                 CHECK_NO)
              VALUES
                ('LN',
                 0,
                 'civ_ln_acct_schedule_detls',
                 'INFO',
                 var_l_count || ' accounts where stage details are incorrect',
                 '1315');
            EXCEPTION
              WHEN OTHERS THEN
                cbsfchost.ora_raiserror(SQLCODE,
                              'Insert for Consistency Check 1315 failed. ',
                              1713);
            END;
          END IF;
          commit;
        END;
    */
    BEGIN
      BEGIN
        Select /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ln_acct_dtls A
         where dat_acct_open >
               (Select distinct min(dat_post)
                  from civ_ln_acct_ledg B
                 WHERe A.cod_acct_no = B.cod_acct_no)
                 and flg_Mnt_status ='A'/*
           AND A.cod_cc_brn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1316 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count ||
             ' accounts where account open date is grerater than minimum posting date in ledger ',
             '1316');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1316 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;

/*      BEGIN
        BEGIN
          \*        select \*+  PARALLEL *\
          COUNT(1)
           into var_l_count
           from civ_ln_acct_dtls A, civ_ln_acct_balances b
          where a.cod_acct_no = b.cod_acct_no
            and a.cod_acct_no NOT IN
                (Select cod_acct_no from civ_ln_death_cases)
            and ABS((b.amt_princ_balance - b.amt_arrears_princ) -
                    ((Select sum(amt_principal)
                        from civ_ln_acct_schedule_detls c
                       where A.cod_acct_no = C.cod_acct_no
                         and c.cod_acct_no NOT IN
                             (Select cod_acct_no
                                from civ_ln_death_cases)
                         and date_instal > var_dat_process - 1) -
                    (a.amt_face_value - b.amt_disbursed))) > 0.01 --removing = to handle cases where first instal is due on the date of disb
            and a.cod_cc_brn = var_cod_cc_brn;*\
          SELECT COUNT(1)
            INTO var_l_count
            FROM (SELECT tab1.*,
                         tab2.*,
                         (amt_princ_balance - amt_arrears_princ) outbal_princ,
                         (((tab1.amt_princ_balance - tab1.amt_arrears_princ) +
                         NVL(tab1.amt_mor_int, 0)) - tab2.amt_principal) mismatch_amt
                    FROM (select a.cod_Acct_no,
                                 a.cod_cc_brn,
                                 b.amt_princ_balance amt_princ_balance,
                                 b.amt_arrears_princ amt_arrears_princ,
                                 NVL(d.amt_mor_int, 0) amt_mor_int,
                                 a.amt_face_value amt_face_value,
                                 b.amt_disbursed amt_disbursed
                            from civ_ln_acct_dtls A,
                                 civ_ln_acct_balances b,
                                 (select h.cod_Acct_no cod_Acct_no,
                                         h.amt_mor_int amt_mor_int
                                    from civ_Ln_Acct_Mor_Dtls h,
                                         civ_ln_acct_Schedule i
                                   WHERE ba_global.dat_process BETWEEN
                                         dat_stage_Start and dat_stage_end
                                     and cod_instal_rule = 5
                                     and h.cod_Acct_no = i.cod_Acct_no) d
                           where a.cod_acct_no = b.cod_acct_no
                             AND a.cod_Acct_no = d.cod_Acct_no(+)
                             and NOT EXISTS
                           (Select 1
                                    from civ_ln_death_cases f
                                   WHERE f.cod_Acct_no = a.cod_Acct_no)) tab1,
                         (Select \*+ parallel *\
                           c.cod_Acct_no, sum(amt_principal) amt_principal
                            from civ_ln_acct_schedule_detls c
                           where NOT EXISTS
                           (Select 1
                                    from civ_ln_death_cases g
                                   WHERE g.cod_Acct_no = c.cod_Acct_no)
                             and date_instal > ba_global.dat_process - 1
                           group by c.cod_Acct_no) tab2
                   WHERE tab1.cod_Acct_no = tab2.cod_Acct_no
                     AND (((tab1.amt_princ_balance - tab1.amt_arrears_princ) +
                         NVL(tab1.amt_mor_int, 0)) - tab2.amt_principal) > 0.01
                     and tab1.cod_cc_brn = var_cod_cc_brn);
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1319 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'civ_ln_acct_schedule_detls',
               'CRITICAL',
               var_l_count ||
               ' accounts where future principal repayment is not matching the principal amount ',
               '1319');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1319 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;*/
      BEGIN
        BEGIN
          select /*+parallel(4) nologging*/
           COUNT(1)
            into var_l_count
            from (select a.COD_CUST, a.COD_ACCT_NO, count(1)
                    from civ_CH_ACCT_CUST_XREF a,
                         civ_ln_acct_dtls      b
                   where a.cod_acct_no = b.cod_acct_no
                   and b.flg_Mnt_status ='A'
                  --   and b.cod_cc_brn = var_cod_cc_brn
                   group by a.COD_CUST, a.COD_ACCT_NO
                  having count(1) > 1);
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1321 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'CH_ACCT_CUST_XREF',
               'INFO',
               var_l_count ||
               ' account-cust xref has multiple rows for the same acct cust combination ',
               '1321');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1321 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          select /*+parallel(4) nologging*/
           COUNT(1)
            into var_l_count
            from (select a.COD_CUST, a.COD_ACCT_NO, count(1)
                    from civ_CH_ACCT_CUST_XREF a,
                         civ_ln_acct_dtls      b
                   where a.cod_acct_no = b.cod_acct_no
                   and b.flg_Mnt_status ='A'
                 --    and b.cod_cc_brn = var_cod_cc_brn
                   group by a.COD_CUST, a.COD_ACCT_NO
                  having count(1) > 1);
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1321 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'CH_ACCT_CUST_XREF',
               'INFO',
               var_l_count ||
               ' account-cust xref has multiple rows for the same acct cust combination ',
               '1321');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1321 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          select /*+parallel(4) nologging*/
           COUNT(1)
            into var_l_count
            from (select a.COD_ACCT_NO
                    from civ_ln_acct_rates a, civ_ln_acct_dtls b
                   where a.cod_acct_no = b.cod_acct_no
                   and b.flg_Mnt_status ='A'
                  --   and b.cod_cc_brn = var_cod_cc_brn
                     and not exists (select 1
                            from civ_ln_acct_rates
                           where cod_acct_no = a.cod_acct_no
                             and ctr_int_srl = 0));
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1322 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'civ_ln_acct_dtls',
               'INFO',
               var_l_count ||
               ' account rates not having regular/pmi interest row ',
               '1322');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1322 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          select /*+parallel(4) nologging*/
           COUNT(1)
            into var_l_count
            from (select a.COD_ACCT_NO
                    from civ_ln_acct_rates_detl a,
                         civ_ln_acct_dtls       b
                   where a.cod_acct_no = b.cod_acct_no
                   and flg_Mnt_status ='A'
                   --  and b.cod_cc_brn = var_cod_cc_brn
                     and not exists (select 1
                            from civ_ln_acct_rates_detl
                           where cod_acct_no = a.cod_acct_no
                             and ctr_int_srl = 0));
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1323 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'LN_ACCT_RATES',
               'CRITICAL',
               var_l_count ||
               ' account rates details not having regular/pmi interest row ',
               '1323');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1323 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;
      --ak strat

      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a, conv_ln_consis_dat_3 b
           WHERE a.cod_acct_no = b.cod_acct_no
           and flg_Mnt_status ='A'
         --    AND a.cod_cc_brn = var_cod_cc_brn
             AND greatest(a.dat_first_disb,NVL(a.dat_last_restructure,'01-Jan-1800') ) NOT BETWEEN min_sched_start AND
                 max_sched_end
             AND a.dat_first_disb != '1-Jan-1800';
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1325 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_dtls',
               'CRITICAL',
               var_l_count ||
               ' first disbursement date (dat_first_disb) not within schedule range',
               '1325');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1325 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a, conv_ln_consis_dat_3 b
           WHERE a.cod_acct_no = b.cod_acct_no
           and flg_Mnt_status ='A'
           --  AND a.cod_cc_brn = var_cod_cc_brn
             AND greatest(a.dat_last_disb,a.dat_last_restructure) NOT BETWEEN min_sched_start AND
                 max_sched_end
             AND a.dat_last_disb != '1-Jan-1800';
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1326 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_dtls',
               'CRITICAL',
               var_l_count ||
               ' last disbursement date (dat_last_disb) not within schedule range',
               '1326');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1326 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a, conv_ln_consis_dat_3 b
           WHERE a.cod_acct_no = b.cod_acct_no
           and flg_Mnt_status ='A'
          --   AND a.cod_cc_brn = var_cod_cc_brn
             AND a.dat_last_ioa NOT BETWEEN min_sched_start AND
                 max_sched_end
             AND a.dat_last_ioa != '1-Jan-1800';

        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1327 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_dtls',
               'CRITICAL',
               var_l_count ||
               ' last penalty application date (dat_last_ioa) not within schedule range',
               '1327');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1327 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a, conv_ln_consis_dat_3 b
           WHERE a.cod_acct_no = b.cod_acct_no
           and flg_Mnt_status ='A'
         --    AND a.cod_cc_brn = var_cod_cc_brn
             AND  GREATEST(a.dat_last_payment, A.DAT_LAST_RESTRUCTURE) NOT BETWEEN min_sched_start AND
                 max_sched_end
             AND a.dat_last_payment != '1-Jan-1800';
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1328 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_dtls',
               'CRITICAL',
               var_l_count ||
               ' last payment date (dat_last_payment) not within schedule range',
               '1328');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1328 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a, conv_ln_consis_dat_3 b
           WHERE a.cod_acct_no = b.cod_acct_no
           and flg_Mnt_status ='A'
         --    AND a.cod_cc_brn = var_cod_cc_brn
             AND b.min_arrear_due NOT BETWEEN min_sched_start AND
                 max_sched_end
             AND b.min_arrear_due != '1-Jan-1800';

        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1329 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_dtls',
               'CRITICAL',
               var_l_count ||
               ' minimum arrear due date min(dat_arrears_due) not within schedule range',
               '1329');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1329 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a,
                 (SELECT cod_acct_no, count(1)
                    FROM civ_ln_acct_schedule
                   where dat_stage_end > var_dat_process
                     and cod_instal_rule not in
                         (select cod_inst_rule
                            from cbsfchost.ln_inst_rules
                           where cod_inst_calc_method in
                                 ('MOR', 'IPI', 'PMI', 'IOI')
                             and flg_mnt_status = 'A')
                   group by cod_acct_no
                  having count(1) > 1) b
           WHERE a.cod_acct_no = b.cod_acct_no
           and flg_Mnt_status ='A'/*
             AND a.cod_cc_brn = var_cod_cc_brn*/;
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1330 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_dtls',
               'CRITICAL',
               var_l_count ||
               ' multiple EPI stages in schedule for the future period',
               '1330');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1330 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
         /* select \*+parallel(4) nologging*\
           COUNT(distinct a.cod_acct_no)
            into var_l_count
            from civ_ln_arrears_table a
           where a.cod_rule_id <> 0
             and a.cod_arrear_type not in ('I', 'N', 'T', 'U', 'A', 'L')
             AND EXISTS (Select 1
                    from civ_ln_acct_dtls c
                   WHERE c.cod_Acct_no = a.cod_acct_no
                   and flg_Mnt_status ='A'\*
                     and c.cod_cc_brn = var_cod_cc_brn*\);*/
                     
                     SELECT /*+parallel(4) nologging*/
 COUNT(DISTINCT A.COD_ACCT_NO) into var_l_count
  FROM CIV_LN_ARREARS_TABLE A,CIV_LN_ACCT_DTLS B
 WHERE  A.cod_acct_no =  b.cod_acct_no
 and b.flg_mnt_status ='A'
 and ref_billno_srl <>0
 AND A.COD_RULE_ID <> 0
   AND A.COD_ARREAR_TYPE NOT IN ('I', 'N', 'T', 'U', 'A', 'L');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1333 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_arrears_table',
               'CRITICAL',
               var_l_count ||
               ' accounts  where rule id is incorrect for non interest type of arrears',
               '1333');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1333 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          select /*+parallel(4) nologging*/
           COUNT(1)
            into var_l_count
            from civ_ln_acct_dtls a
           where NVL(a.flg_accr_status, 'S') <> 'N'
           and flg_Mnt_status ='A'/*
             and a.cod_cc_brn = var_cod_cc_brn*/;
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1334 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_Acct_dtls',
               'INFO', --CRITICAL. PRE#12052
               var_l_count || ' accounts where accrual status is SUSPENDED',
               '1334');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1334 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          Select /*+parallel(4) nologging*/
           COUNT(distinct a.cod_Acct_no)
            INTO var_l_count
            from civ_ln_acct_rates_detl a,
                 civ_ln_acct_rates      b,
                 civ_ln_acct_dtls       c
           where b.cod_acct_no = a.cod_acct_no
             AND b.cod_Acct_no = c.cod_Acct_no
          --   AND c.cod_cc_brn = var_cod_cc_brn
             and b.ctr_int_srl = 0
             and a.ctr_int_srl = 0
             and b.cod_defn_typ != a.cod_defn_typ
             and c.flg_Mnt_status ='A'
            /* and trim(c.cod_acct_no) NOT IN
                 (Select trim(cod_acct_no) from ln_acct_fix_float_rate)*/;
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1335 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_Acct_rate_detls',
               'CRITICAL',
               var_l_count || ' accounts where cod_defn_typ is not in sync',
               '1335');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1335 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      BEGIN
        BEGIN
          Select /*+parallel(4) nologging*/
           COUNT(distinct b.cod_acct_no)
            into var_l_count
            from civ_ln_acct_rates b,civ_ln_acct_dtls a
           where a.cod_acct_no = b.cod_acct_no
           and a.flg_mnt_status ='A'
           and (b.cod_int_index_slab != 0 OR rat_int_slab != 0)
             and ctr_int_srl = 0
             /*and exists (select 1
                    from civ_ln_acct_dtls
                   where cod_acct_no = b.cod_acct_no
                   and flg_Mnt_status ='A'
                     and cod_cc_brn = var_cod_cc_brn)*/;
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1336 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_Acct_rates',
               'INFO',
               var_l_count ||
               ' accounts where cod_int_index_slab OR rat_int_slab is migrated as non - zero',
               '1336');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1336 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;
      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_acct_dtls a, civ_ln_acct_schedule b
           where a.cod_acct_no = b.cod_acct_no
             and b.dat_stage_end >= var_dat_process
             and cod_instal_rule in
                 (select cod_inst_rule
                    from cbsfchost.ln_inst_rules
                   where cod_inst_calc_method = 'VPI'
                     and flg_mnt_status = 'A')
             and not exists (select 1
                    from civ_ln_acct_vpi_sched
                   where cod_acct_no = a.cod_acct_no)
                   and a.flg_Mnt_status ='A'/*
             AND a.cod_cc_brn = var_cod_cc_brn*/;

        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1337 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_acct_schedule',
               'CRITICAL',
               var_l_count ||
               ' no records found in ln_acct_vpi_sched for a VPI type schedule',
               '1337');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1337 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;
--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_ln_acct_dtls a,
--                 civ_ln_acct_schedule b,
--                 (select cod_acct_no, max(CTR_INSTAL_NO) max_ctr_instal_to
--                    from civ_ln_acct_vpi_sched
--                   group by cod_acct_no) c
--           where a.cod_acct_no = b.cod_acct_no
--             and a.cod_acct_no = c.cod_acct_no
--             and b.dat_stage_end >= var_dat_process
--             and b.cod_instal_rule in
--                 (select cod_inst_rule
--                    from ln_inst_rules
--                   where cod_inst_calc_method = 'VPI'
--                     and flg_mnt_status = 'A')
--             and c.max_ctr_instal_to <>
--                 (select max(ctr_instal) - 1
--                    from civ_ln_acct_schedule_detls
--                   where cod_acct_no = a.cod_acct_no)
--             AND a.cod_cc_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1338 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'ln_acct_schedule',
--               'CRITICAL',
--               var_l_count ||
--               ' max vpi instal no -one not matching with schedule detls',
--               '1338');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1338 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;
      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_arrears_table a,civ_ln_acct_dtls b
           where a.cod_acct_no = b.cod_acct_no
           and b.flg_mnt_status ='A'
           and a.cod_arrear_type IN ('T', 'U')
            /* AND exists (Select 1
                    from civ_ln_acct_dtls b
                   WHERE A.cod_acct_no = B.cod_acct_no
                   and flg_Mnt_status ='A'
                     AND b.cod_cc_brn = var_cod_cc_brn)*/;

        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1339 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_arrears_table',
               'CRITICAL',
               var_l_count || ' accounts where PMI arrears are present',
               '1339');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1339 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;
      BEGIN
        BEGIN
          SELECT /*+parallel(4) nologging*/
           COUNT(1)
            INTO var_l_count
            FROM civ_ln_arrears_table a,civ_ln_acct_dtls b
           where A.cod_acct_no = B.cod_acct_no 
           and flg_mnt_status ='A'
           and a.cod_arrear_type IN ('U', 'L', 'N', 'D', 'E', 'G', 'M')
           /*  AND exists (Select 1
                    from civ_ln_acct_dtls b
                   WHERE A.cod_acct_no = B.cod_acct_no
                   and flg_Mnt_status ='A'
                     AND b.cod_cc_brn = var_cod_cc_brn)*/;

        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ora_raiserror(SQLCODE,
                          'Select distinct for Consistency Check 1340 failed. ',
                          1702);
        END;
        IF (var_l_count > 0) THEN
          BEGIN
            INSERT INTO co_warn_table
              (COD_MODULE,
               COD_CC_BRN,
               TABLE_NAME,
               SEVERITY,
               REMARKS,
               CHECK_NO)
            VALUES
              ('LN',
               0,
               'ln_arrears_table',
               'INFO-PRE', --CRITICAL .PRE#12123
               var_l_count ||
               ' accounts where Suspended arrears are present',
               '1340');
          EXCEPTION
            WHEN OTHERS THEN
              cbsfchost.ora_raiserror(SQLCODE,
                            'Insert for Consistency Check 1340 failed. ',
                            1713);
          END;
        END IF;
        commit;
      END;

      --- Consis check for GL FCY and LCY should not be 0
--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_GL_TMP_XF_STCAP_TXNS a
--           WHERE (a.AMT_TXN_FCY = 0 OR a.AMT_TXN_LCY = 0)
--             AND a.cod_cc_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1341 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'GL_TMP_XF_STCAP_TXNS',
--               'CRITICAL',
--               var_l_count ||
--               ' accounts where GL transactions migrated with ZERO Amount',
--               '1341');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1341 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;

--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_st_postdated_cheques a,
--                 civ_ln_acct_dtls         b,
--                 civ_st_postdated_chq_ext c
--           WHERE a.cod_payee_acct = b.cod_acct_no
--             and a.cod_payee_acct = c.cod_payee_acct
--             and b.cod_acct_no = c.cod_payee_acct
--             and a.dat_instr = '01-JAN-1950'
--             and c.chq_type = 'PDC'
--             AND b.cod_cc_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1342 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'ST_POSTDATED_CHEQUES',
--               'CRITICAL',
--               var_l_count ||
--               ' records where instrument date is FIRST-JAN-FIFTY for PDC',
--               '1342');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1342 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;

--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_st_postdated_cheques a
--           WHERE cod_bank != '756'
--             AND a.cod_org_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1343 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'ST_POSTDATED_CHEQUES',
--               'INFO',
--               var_l_count ||
--               ' records where bank code is not EQUITAS bank code',
--               '1343');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1343 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;
--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            from civ_ln_acct_dtls a
--           where not exists (select 1
--                    from civ_LN_ACCT_ATTR_EXT b
--                   where a.cod_acct_no = b.cod_acct_no)
--             and a.cod_cc_brn = var_cod_cc_brn;
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1344 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'civ_ln_acct_attributes_ext',
--               'CRITICAL',
--               var_l_count ||
--               ' Records where no ext attribute details are provided',
--               '1344');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1344 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;

--      BEGIN
--        BEGIN
--          /*           SELECT COUNT(1)
--                   INTO var_l_count
--                   FROM civ_st_postdated_cheques a
--                  WHERE (amt_txn_tcy != 0 OR amt_txn_lcy != 0)
--                    AND a.cod_org_brn = var_cod_cc_brn;
--          */
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_st_postdated_cheques a,
--                 civ_ln_acct_dtls         b,
--                 civ_st_postdated_chq_ext c
--           WHERE a.cod_payee_acct = b.cod_acct_no
--             and a.cod_payee_acct = c.cod_payee_acct
--             and b.cod_acct_no = c.cod_payee_acct
--             and a.ref_instr_no = c.ref_instr_no
--             and (a.amt_txn_tcy = 0 OR a.amt_txn_lcy = 0)
--             and c.chq_type = 'PDC'
--             AND b.cod_cc_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1345 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'ST_POSTDATED_CHEQUES',
--               'CRITICAL',
--               var_l_count ||
--               ' records where instrument amount is ZERO for PDC cheques',
--               '1345');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1345 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;

--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_st_postdated_cheques a
--           WHERE length(cod_routing_no) <> 9
--             AND a.cod_org_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1346 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'ST_POSTDATED_CHEQUES',
--               'INFO',
--               var_l_count || ' records where routing code is invalid ',
--               '1346');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1346 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;
--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_st_postdated_cheques a,
--                 civ_ln_acct_dtls         b,
--                 civ_st_postdated_chq_ext c
--           WHERE a.cod_payee_acct = b.cod_acct_no
--             and a.cod_payee_acct = c.cod_payee_acct
--             and b.cod_acct_no = c.cod_payee_acct
--             and a.dat_instr != b.dat_of_maturity
--             and a.ref_instr_no = c.ref_instr_no
--             and c.chq_type = 'SPDC'
--             AND b.cod_cc_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1347 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'ST_POSTDATED_CHEQUES',
--               'CRITICAL',
--               var_l_count ||
--               ' records where instrument date is FIRST-JAN-FIFTY for SPDC',
--               '1347');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1347 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;

--      BEGIN
--        BEGIN
--          SELECT /*+parallel(4) nologging*/
--           COUNT(1)
--            INTO var_l_count
--            FROM civ_gl_tmp_xf_stcap_txns
--           WHERE cod_gl_acct not in (Select cod_gl_acct from gl_table)
--             AND cod_cc_brn = var_cod_cc_brn;
--
--        EXCEPTION
--          WHEN OTHERS THEN
--            cbsfchost.ora_raiserror(SQLCODE,
--                          'Select distinct for Consistency Check 1348 failed. ',
--                          1702);
--        END;
--        IF (var_l_count > 0) THEN
--          BEGIN
--            INSERT INTO co_warn_table
--              (COD_MODULE,
--               COD_CC_BRN,
--               TABLE_NAME,
--               SEVERITY,
--               REMARKS,
--               CHECK_NO)
--            VALUES
--              ('LN',
--               0,
--               'GL_TMP_XF_STCAP_GL_TXNS',
--               'CRITICAL',
--               var_l_count ||
--               ' records where GL codes are not avaiable in master',
--               '1348');
--          EXCEPTION
--            WHEN OTHERS THEN
--              cbsfchost.ora_raiserror(SQLCODE,
--                            'Insert for Consistency Check 1348 failed. ',
--                            1713);
--          END;
--        END IF;
--        commit;
--      END;
      BEGIN
        Select /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ba_coll_hdr
         where AMT_UNUSED_VAL > AMT_LAST_VAL/*
           and cod_coll_homebrn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select Failed: Consistency Check 1349',
                        146);
      END;
      IF var_l_count > 0 THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ba_coll_hdr',
             'CRITICAL',
             var_l_count ||
             ' RECORDS WHERE unused value  is not greater than last value',
             '1349');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert Failed: Consistency Check No 1349',
                          159);
        END;
      END IF;
      commit;
      BEGIN
        Select /*+parallel(4) nologging*/
         COUNT(1)
          INTO var_l_count
          from civ_ba_coll_hdr
         where AMT_UNUSED_VAL < 0/*
           and cod_coll_homebrn = var_cod_cc_brn*/;
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1204');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select Failed: Consistency Check 1350',
                        146);
      END;
      IF var_l_count > 0 THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ba_coll_hdr',
             'CRITICAL',
             var_l_count || ' RECORDS WHERE unused value  is negative',
             '1350');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1204');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert Failed: Consistency Check No 1350',
                          159);
        END;
      END IF;
      commit;
      /*BEGIN
            SELECT COUNT(1)
              INTO var_l_count
              FROM civ_ln_acct_schedule a,civ_ln_acct_dtls b
             where a.cod_acct_no = b.cod_acct_no
             and b.cod_cc_brn = var_cod_cc_brn
      and (a.dat_first_rest IS NULL OR a.frq_int_rest IS NULL);
          EXCEPTION
            WHEN OTHERS THEN
              --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1324');
              cbsfchost.ora_raiserror(SQLCODE,
                            'Select Failed: Consistency Check 1324',
                            1498);
          END;
          IF var_l_count > 0 THEN
            BEGIN
              INSERT INTO co_warn_table
                (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
              VALUES
                ('LN',
                 0,
                 'civ_ln_acct_schedule',
                 'CRITICAL',
                 var_l_count ||
                 '  RECORDS WHERE first rest date is IS NULL OR rest period frequency is null',
                 '588');
            EXCEPTION
              WHEN OTHERS THEN
                --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1324');
                cbsfchost.ora_raiserror(SQLCODE,
                              'Insert Failed: Consistency Check No 1324',
                              1510);
            END;
          END IF;
          commit;*/
    --END;

    /*    BEGIN
      BEGIN
        SELECT COUNT(1)
          INTO var_l_count
          FROM civ_ln_acct_dtls
         WHERE flg_commitment_type NOT IN ('U', 'L', 'N', 'E')
           AND cod_cc_brn = var_cod_cc_brn;
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Select distinct for Consistency Check 1234 failed. ');
          cbsfchost.ora_raiserror(SQLCODE,
                        'Select distinct for Consistency Check 1234 failed. ',
                        1702);
      END;
      IF (var_l_count > 0) THEN
        BEGIN
          INSERT INTO co_warn_table
            (COD_MODULE,
             COD_CC_BRN,
             TABLE_NAME,
             SEVERITY,
             REMARKS,
             CHECK_NO)
          VALUES
            ('LN',
             0,
             'civ_ln_acct_dtls',
             'CRITICAL',
             var_l_count ||
             ' Records where flg_commitment_type is neither U or L or N or E',
             '1234');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE,                          'Insert for Consistency Check 1234 failed');
            cbsfchost.ora_raiserror(SQLCODE,
                          'Insert for Consistency Check 1234 failed. ',
                          1713);
        END;
      END IF;
      commit;
    END;*/

    /*ap_co_ins_consis_proc_time('ap_co_consis_check_ln_2',
                                   0,
                                   2);
    */
  /* UPDATE conv_brn_stream_proc_xref
       SET flg_processed = 'Y'
     WHERE \*cod_stream_id = var_cod_stream_id
     --  AND cod_cc_brn = var_cod_cc_brn
       AND *\cod_proc_nam = 'AP_CO_CONSIS_CHECK_LN_3';
*/
    COMMIT;
 -- END LOOP;
  COMMIT;
  RETURN 0;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    --write_to_file(SQLCODE, 'Execution of ap_co_consis_check_ln_2 failed');
    cbsfchost.ora_raiserror(SQLCODE,
                  'Execution of ap_co_consis_check_ln_3 failed',
                  1751);
    RETURN 95;
END;
/
