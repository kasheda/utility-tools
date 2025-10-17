CREATE OR REPLACE FUNCTION AP_CO_CONSIS_CHECK_LN_4 (var_cod_stream_id NUMBER)
  RETURN NUMBER AS
  var_l_rec_count NUMBER := 0;
    var_dat_process      DATE;
  var_dat_last_process DATE;
  var_bank_mast_dt_to_use VARCHAR2(1) := 'N';--nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'), 'Y');
BEGIN

  select dat_process,dat_last_process into var_dat_process,var_dat_last_process from cbsfchost.ba_bank_mast;

 /*   IF ( var_bank_mast_dt_to_use = 'N' ) THEN
        var_dat_process := nvl(ap_get_data_mig_param('DAT_PROCESS'), var_dat_process);
        var_dat_last_process := nvl(ap_get_data_mig_param('DAT_LAST_PROCESS'), var_dat_last_process);
    END IF;*/

  BEGIN
    DELETE FROM CO_WARN_TABLE WHERE CHECK_NO IN (1319, 1801,1802,1803,1804,1805,1806,1807,416,417,499,448,449,450,451);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                    'Delete Failed: CO_WARN_TABLE failed',
                    42);

  END;
  BEGIN
    SELECT COUNT(1)
      INTO var_l_rec_count
      FROM (select /*+PARALLEL(4) */
             a.cod_acct_no
              from civ_ln_acct_dtls a, civ_ln_acct_balances b
             WHERE a.cod_Acct_no = b.cod_Acct_no
               and cod_Acct_stat NOT IN (1, 5)
               and amt_princ_balance = 0
               and amt_arrears_princ = 0
               and ctr_disb > 0
               and amt_disbursed > 0
            MINUS
            select /*+PARALLEL(4) */
            distinct a.cod_acct_no
              from civ_ln_acct_dtls     a,
                   civ_ln_acct_balances b,
                   civ_ln_arrears_table c
             WHERE a.cod_Acct_no = b.cod_Acct_no
               and cod_Acct_stat NOT IN (1, 5)
               and amt_princ_balance = 0
               and amt_arrears_princ = 0
               and ctr_disb > 0
               and amt_disbursed > 0
               and a.cod_Acct_no = c.cod_Acct_no
               and amt_arrears_due > 0);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1801', 42);
  END;

  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_balances',
         'INFO',
         var_l_rec_count ||
         'Loans Accounts with no outstanding balance  zero principal and zero arrears',
         '1801');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 1801',
                      72);
    END;
  END IF;
  /*
  BEGIN
    SELECT COUNT(distinct cod_acct_no)
      INTO var_l_rec_count
      FROM (SELECT l.cod_Acct_no
              FROM (select \*+PARALLEL(4) *\
                     i.cod_Acct_no cod_Acct_no,
                     --h.amt_mor_int amt_mor_int,
                     I.Ctr_Stage_No,
                     j.nam_stage,
                     j.ctr_stage_no,
                     j.amt_princ_repay,
                     k.amt_arrears_princ,
                     k.amt_princ_balance,
                     (k.amt_princ_balance) manual_calc_amt_princ_repay
                      from --civ_Ln_Acct_Mor_Dtls h,
                           civ_ln_acct_balances k,
                           civ_ln_acct_Schedule i,
                           civ_ln_acct_Schedule j
                     WHERE var_dat_process BETWEEN i.dat_stage_Start and
                           i.dat_stage_end
                       and i.cod_instal_rule = 5
                       --and h.cod_Acct_no = i.cod_Acct_no
                       and j.cod_Acct_no = i.cod_Acct_no
                       and j.ctr_stage_no = (I.Ctr_Stage_No + 1)
                       and k.cod_acct_no = i.cod_Acct_no
                       and ((k.amt_princ_balance  - k.amt_arrears_princ) -
                           j.amt_princ_repay) > 1) l);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 456', 94);

  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         999,
         'civ_ln_acct_schedule',
         'CRITICAL',
         var_l_rec_count ||
         '  Loans Where sum of amt_pric_repay should be equal to amt_disbursed in table civ_ln_acct_schedule',
         '456');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 456',
                      62);
    END;
  END IF;
  COMMIT;
*/
  --1802
--  BEGIN
--    SELECT COUNT(1)
--      INTO var_l_rec_count
--      FROM civ_LN_X_COLLAT_SEIZ_MAST
--     WHERE (FLG_EVENT NOT IN ('SZ', 'SL', 'RL') OR FLG_EVENT IS NULL);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1802', 260);
--  END;
--
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_ln_Acct_balances',
--         'CRITICAL',
--         var_l_rec_count ||
--         'Seized Loans Accounts WHERE FLG_EVENT NOT IN (SZ,SL,RL)',
--         '1802');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 1801',
--                      72);
--    END;
--  END IF;
  --1803
--  BEGIN
--    SELECT COUNT(1)
--      INTO var_l_rec_count
--      FROM civ_LN_X_COLLAT_SEIZ_MAST
--     WHERE (FLG_SEIZURE NOT IN ('Y', 'N') OR FLG_EVENT IS NULL);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1803', 290);
--  END;
--
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_LN_X_COLLAT_SEIZ_MAST',
--         'CRITICAL',
--         var_l_rec_count ||
--         'Seized Loans Accounts WHERE FLG_SEIZURE NOT IN (Y,N)',
--         '1803');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 1803',
--                      315);
--    END;
--  END IF;
  --1804
--  BEGIN
--    SELECT COUNT(distinct a.cod_acct_no)
--      INTO var_l_rec_count
--      FROM civ_LN_X_COLLAT_SEIZ_MAST a, civ_LN_ACCT_DTLS b
--     WHERE a.cod_acct_no = b.cod_acct_no
--       AND a.FLG_SEIZURE = 'Y'
--       AND ((a.DAT_RELEASE IS NOT NULL) OR (a.DAT_SEIZURE IS NULL) OR
--           (NVL(a.DAT_SEIZURE, '01-JAN-1800') NOT BETWEEN
--           var_Dat_process AND b.dat_last_charged) OR
--           NVL(a.DAT_SEIZURE, '01-JAN-1800') < b.dat_last_accrual);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1804', 325);
--  END;
--
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_LN_X_COLLAT_SEIZ_MAST',
--         'CRITICAL',
--         var_l_rec_count ||
--         'Seized Loans Accounts WHERE FLG_SEIZURE is Y and  DAT_RELEASE IS NOT NULL or DAT_SEIZURE IS NULL or DAT_SEIZURE is not between Migration date and last charging date or DAT_SEIZURE is before Last accrual date',
--         '1804');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 1804',
--                      350);
--    END;
--  END IF;
  --1805
--  BEGIN
--    SELECT COUNT(1)
--      INTO var_l_rec_count
--      from civ_LN_X_COLLAT_SEIZ_MAST
--     where cod_coll NOT IN (select cod_coll from fchost.ba_coll_codes);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1805', 360);
--  END;
--
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_ln_x_collat_seiz_mast',
--         'CRITICAL',
--         var_l_rec_count ||
--         'RECORDS WHERE collateral code is not present in ba_coll_codes ',
--         '1805');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 1805',
--                      375);
--    END;
--  END IF;
  --1806
--  BEGIN
--    SELECT COUNT(1)
--      INTO var_l_rec_count
--      from civ_ba_ho_coll_acct_xref
--     where cod_coll NOT IN (select cod_coll from fchost.ba_coll_codes);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1806', 390);
--  END;
--
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_ba_ho_coll_acct_xref',
--         'CRITICAL',
--         var_l_rec_count ||
--         'RECORDS WHERE collateral code is not present in ba_coll_codes ',
--         '1806');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 1806',
--                      410);
--    END;
--  END IF;
  /*
    - flg_seizure is Y, meaning collateral is currently seized,
    In such case,
                  - Release date should be null
                  - Seizure date should be the actual seizure date (less than migration date and greater than dat_last_charged)
                  - Last accrual date to be less than or equal to seizure date
  - flg_seizure is Y, dat_release will always be null.
  - Collateral ID and collateral code migrated in the new table should be present in the collateral master table and collateral linkage table

    */
  --1807
--  BEGIN
--    SELECT COUNT(1)
--      INTO var_l_rec_count
--      from civ_ln_acct_attributes
--     where trim(cod_umrn_no) is NOT NULL and cod_umrn_no  NOT IN (select cod_umrn_no from civ_pm_mandate_mast);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 1807', 435);
--  END;
--
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_pm_mandate_mast',
--         'CRITICAL',
--         var_l_rec_count ||
--         'RECORDS WHERE cod_umrn_no is not present in pm_mandate_mast',
--         '1807');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 1807',
--                      450);
--    END;
--  END IF;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_rec_count
      FROM civ_ba_coll_hdr a
     WHERE (a.amt_last_val = 0 or a.amt_orig_value = 0 or
           (a.amt_last_val = 0 and a.amt_orig_value > 0));
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 448', 2320);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0, --var_cod_cc_brn
         'civ_BA_COLL_HDR',
         'CRITICAL',
         var_l_rec_count ||
         ' Loans  in table civ_ba_coll_hdr Where amt_last_val is zero or amt_orig_value = zero ',
         '448');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 448',
                      2326);
    END;
  END IF;
  COMMIT;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_rec_count
      FROM civ_ba_coll_hdr a
     WHERE (a.dat_last_val = a.dat_orig_value and
           (a.amt_last_val <> a.amt_orig_value));
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 449', 2352);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0, --var_cod_cc_brn
         'civ_BA_COLL_HDR',
           'INFO-PRE', --CRITICAL. Pre#61022
         var_l_rec_count ||
         ' Loans  in table civ_ba_coll_hdr  Where dat_last_val = dat_orig_value and  amt_last_val <>  amt_orig_value ',
         '449');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 449',
                      2370);
    END;
  END IF;
  COMMIT;
  BEGIN
    SELECT /*+ PARALLEL(4) */
     COUNT(1)
      INTO var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE a.dat_last_due > var_dat_process;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 450', 2380);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0, --var_cod_cc_brn
         'civ_BA_COLL_HDR',
         'CRITICAL',
         var_l_rec_count ||
         ' Loans  where ln_acct_dtls.dat_last_due is greater than process date ',
         '450');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 500',
                      2401);
    END;
  END IF;
  COMMIT;
  /*BEGIN
  --since there is no premium arrears present for EEB and GB case we are commenting this.
    SELECT \*+ PARALLEL(4) *\
     COUNT(1)
      INTO var_l_rec_count
      from civ_ln_acct_balances a
     where amt_arrears_prem <>
           (select sum(b.amt_arrears_due)
              from civ_ln_arrears_table b
             where a.cod_acct_no = b.cod_acct_no
               and b.cod_arrear_type = 'P'
             group by b.cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 451', 2415);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0, --var_cod_cc_brn
         'civ_LN_ACCT_BALANCES',
         'CRITICAL',
         var_l_rec_count ||
         ' Loans  where ln_acct_balances.amt_arrears_prem <>  sum(ln_arrears_table.amt_arrears_due) for cod_arrear_type P',
         '451');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 451',
                      2433);
    END;
  END IF;*/
  COMMIT;

--  BEGIN
--    SELECT /*+ PARALLEL(4) */
--     COUNT(*)
--      INTO var_l_rec_count
--      from civ_pm_mandate_mast a
--     group by a.cod_umrn_no
--    having count(*) > 1;
--
--  EXCEPTION
--    WHEN NO_DATA_FOUND THEN
--         var_l_rec_count :=0;
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 416', 2776);
--  END;
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0, --var_cod_cc_brn
--         'PM_MANDATE_MAST',
--         'CRITICAL',
--         var_l_rec_count || 'Duplicate umrn no in PM_MANDATE_MAST ',
--         '416');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                      'Insert Failed: Consistency Check No 416',
--                      2800);
--    END;
--  END IF;
  COMMIT;
  BEGIN
   SELECT count(1) INTO var_l_rec_count from
    (SELECT /*+ PARALLEL(4) */
     COUNT(1)
      from civ_ln_acct_attributes a
     where a.cod_umrn_no is NOT NULL
     group by a.cod_umrn_no
    having count(1) > 1);

  EXCEPTION
     WHEN NO_DATA_FOUND THEN
         var_l_rec_count :=0;
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE, 'Select Failed: Consistency Check 417', 2976);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0, --var_cod_cc_brn
         'LN_ACCT_ATTRIBUTES',
         'CRITICAL',
         var_l_rec_count || 'Duplicate umrn no in LN_ACCT_ATTRIBUTES ',
         '417');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'Insert Failed: Consistency Check No 417',
                      2825);
    END;
  END IF;

  COMMIT;
  RETURN 0;
EXCEPTION
  WHEN OTHERS THEN
    cbsfchost.ORA_RAISERROR(SQLCODE,
                  'Normal execution of the function - AP_CO_CONSIS_CHECK_LN_4 failed',
                  180);
    RETURN 95;
END;
/