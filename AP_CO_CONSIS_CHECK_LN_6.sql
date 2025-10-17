CREATE OR REPLACE FUNCTION AP_CO_CONSIS_CHECK_LN_6(var_cod_stream_id NUMBER)
  RETURN NUMBER AS

  var_l_rec_count      NUMBER := 0;
  MIG_DATE             DATE;
  var_l_count          NUMBER := 0;
  var_dat_process      DATE;
  var_dat_last_process DATE;
  var_cod_cc_brn       number := 0;
  --var_l_rec_count := 0;
  var_bank_mast_dt_to_use VARCHAR2(1) := 'N'; --nvl(ap_get_data_mig_param('BANK_MAST_DT_TO_USE'), 'Y');

begin
  BEGIN
    select param_value
      into MIG_DATE
      from BB_PARAMS
     where PARAM_NAME = 'MIG_DATE';
  END;
  BEGIN
    select dat_process, dat_last_process
      into var_dat_process, var_dat_last_process
      from cbsfchost.ba_bank_mast;
  END;

  /* IF ( var_bank_mast_dt_to_use = 'N' ) THEN
      var_dat_process := nvl(ap_get_data_mig_param('DAT_PROCESS'), var_dat_process);
      var_dat_last_process := nvl(ap_get_data_mig_param('DAT_LAST_PROCESS'), var_dat_last_process);
  END IF;*/

  Delete from co_warn_table
   where check_no in ('425', '484', '487', '488', '489', '490', '491', '492',
          '494', '457', '458', '1627', '1629', '1202', '1206',
          '1227', '1302', '1306', '1307', '1309', '1310', '1311',
          '1312', '1313', '1318', '1320', '1331', '1332', '456',
          '1319', '499', '2030', '2031', '2037', '2038');

  BEGIN
    SELECT /*+PARALLEL(4) */
     COUNT(DISTINCT(A.cod_acct_no))
      INTO var_l_count
      FROM civ_ln_acct_schedule     A,
           cbsfchost.ln_sched_types B,
           CiV_LN_ACCT_DTLS         C
     WHERE A.cod_acct_no = C.cod_acct_no
          --  AND C.cod_cc_brn = var_cod_cc_brn
       AND A.cod_sched_type = B.cod_sched_type
       AND A.cod_instal_rule = B.cod_instal_rule
       AND B.cod_instal_rule IN
           (SELECT cod_inst_rule
              FROM cbsfchost.ln_inst_rules
             WHERE cod_inst_calc_method = 'EPI'
               AND flg_mnt_status = 'A')
       AND A.frq_instal = A.frq_int
       AND A.dat_first_instal <> A.dat_first_int;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 425');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 425',
                              636);
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
         '  Loans Where DAT_FIRST_INSTAL <> DAT_FIRST_INT IN EPI Stage',
         '425');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 425');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 425',
                                648);
    END;
  END IF;
  commit;

  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE cbsfchost.DATEDIFF('DD', dat_stage_start, dat_stage_end) <= 0
       AND a.cod_acct_no = b.cod_acct_no;
    -- AND b.cod_cc_brn = var_cod_cc_brn;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 484');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 484',
                              1426);
  END;
  IF var_l_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         var_cod_cc_brn,
         'civ_LN_ACCT_schedule',
         'CRITICAL',
         var_l_count ||
         '  RECORDS WHERE (dat_stage_start - dat_stage_end)<=ZERO',
         '484');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 484');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 484',
                                1438);
    END;
  END IF;
  commit;

  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE a.dat_stage_start >= a.dat_first_int
       and a.nam_stage = 'PMI'
          -- and b.cod_cc_brn = var_cod_cc_brn
       and a.cod_acct_no = b.cod_acct_no;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 487',
                              1586);
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
         'RECORDS WHERE ln_acct_schedule.dat_stage_start (PMI stage) is less than or equal to ln_acct_schedule.dat_first_int=dat_stage_start plus one month',
         '487');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 489');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 487',
                                1686);
    END;
  END IF;
  commit;

  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE (a.dat_first_int IS NULL OR a.dat_first_comp IS NULL)
          -- and b.cod_cc_brn = var_cod_cc_brn
       and a.cod_acct_no = b.cod_acct_no;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 488');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 488',
                              1498);
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
         '  RECORDS WHERE dat_first_int IS NULL OR dat_first_comp IS NULL',
         '488');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 488');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 488',
                                1510);
    END;
  END IF;
  commit;
  --commit;
  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE (a.dat_stage_start IS NULL OR a.dat_stage_end IS NULL)
          -- and b.cod_cc_brn = var_cod_cc_brn
       and a.cod_acct_no = b.cod_acct_no;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 489');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 489',
                              1528);
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
         '  RECORDS WHERE dat_stage_start IS NULL OR dat_stage_end IS NULL',
         '489');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 489');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 489',
                                1540);
    END;
  END IF;
  commit;
  --commit;
  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE a.ctr_instal <> a.ctr_int
       AND a.cod_instal_rule in
           (SELECT b.cod_inst_rule
              FROM cbsfchost.ln_inst_rules b
             WHERE b.flg_mnt_status = 'A'
               AND b.cod_inst_calc_method = 'EPI')
          --AND                   ( a.ctr_instal = 0                OR          a.ctr_int =0 )
          -- and b.cod_cc_brn = var_cod_cc_brn
       and b.cod_acct_no = a.cod_acct_no;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 490');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 490',
                              1563);
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
         '  RECORDS FOR EPI STAGE WHERE (NO OF PRINC INSTALLMENTS) <> (NO OF INT INSTALLMENTS)',
         '490');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 490');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 490',
                                1575);
    END;
  END IF;
  commit;
  --commit;
  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE a.dat_first_instal < a.dat_stage_start
       AND a.ctr_instal > 0
          -- and b.cod_cc_brn = var_cod_cc_brn
       and a.cod_acct_no = b.cod_acct_no
          
       and a.dat_Stage_start <= var_dat_process
       and a.dat_Stage_end > var_dat_process
       and b.dat_of_maturity > var_dat_process;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 491');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 491',
                              1594);
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
         '  ACCOUNTS WHERE DAT_FIRST_INSTAL LESS THAN STAGE START',
         '491');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 491');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 491',
                                1606);
    END;
  END IF;
  commit;
  --commit;
  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule a, civ_LN_ACCT_DTLS b
     WHERE a.frq_int = 0
       AND a.dat_first_int <> a.dat_stage_end
          --  and b.Cod_Cc_Brn = var_cod_cc_brn
       and b.cod_acct_no = a.cod_acct_no
       AND A.COD_INSTAL_RULE NOT IN
           (select COD_INST_RULE
              from cbsfchost.ln_inst_rules
             where cod_inst_calc_method = 'MOR');
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 492');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 492',
                              1625);
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
         '  ACCOUNTS WHERE INTEREST FREQ IS NONE BUT DAT_FIRST_INT <> DAT_STAGE_END',
         '492');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 492');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 492',
                                1637);
    END;
  END IF;
  commit;
  --commit;

  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_schedule_detls a, civ_LN_ACCT_DTLS b
     WHERE (a.amt_princ_bal < 0)
          -- and b.Cod_Cc_Brn = var_cod_cc_brn
       and b.cod_acct_no = a.cod_acct_no;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 494');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 494',
                              1625);
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
         '  ACCOUNTS WHERE schedule principal balance is less than ZERO',
         '494');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 494');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 494',
                                1637);
    END;
  END IF;
  commit;
  --commit;

  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(DISTINCT A.COD_ACCT_NO)
      INTO var_l_count
      FROM civ_ln_acct_schedule_detls a, civ_LN_ACCT_DTLS b
     WHERE /*b.cod_cc_brn = var_cod_cc_brn
                         and */
     b.cod_acct_no = a.cod_acct_no
     and (a.ctr_instal < 1 OR amt_instal_outst < 0);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 457',
                              2250);
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
         'Loans Where ctr_instal should be greater than zero OR amt_instal_outst should not be negative in table civ_ln_acct_schedule_detls',
         '457');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 457',
                                2270);
    END;
  END IF;
  commit;
  BEGIN
    SELECT /*+PARALLEL(4)*/
     COUNT(DISTINCT A.COD_ACCT_NO)
      INTO var_l_count
      FROM civ_ln_acct_schedule_detls a, civ_LN_ACCT_DTLS b
     WHERE /* b.cod_cc_brn = var_cod_cc_brn
                         and */
     b.cod_acct_no = a.cod_acct_no
     and a.dat_start = a.date_instal
     and a.ctr_instal <> 1;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 458',
                              2286);
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
         'Loans Where dat_start = date_instal in table civ_ln_acct_schedule_detls',
         '458');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 458',
                                2301);
    END;
  END IF;
  commit;

  /*  BEGIN  -- duplicate of 1302
      SELECT \*+ parallel(4)*\
       COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE A.CTR_DISB > 0
            -- AND A.COD_CC_BRN = VAR_COD_CC_BRN
         AND DAT_OF_MATURITY > VAR_DAT_PROCESS
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_SCHEDULE_DETLS B
               WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
         AND COD_ACCT_STAT NOT IN (1, 5);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1625',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_SCHEDULE_DETLS',
           'CRITICAL',
           VAR_L_COUNT ||
           '  DISBURSED LOANS WHERE SCHEDULE DETAILS IS NOT PRESENT',
           '1625');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1625',
                              162);
      END;
    END IF;
  */
  COMMIT;

  BEGIN
    SELECT /*+ parallel(4)*/
     COUNT(1)
      INTO VAR_L_COUNT
      FROM CIV_LN_ACCT_DTLS B,
           (SELECT COD_ACCT_NO,
                   MIN(DAT_STAGE_START) MIN_SCHED_DAT,
                   MAX(DAT_STAGE_END) MAX_SCHED_DAT
              FROM CIV_LN_ACCT_SCHEDULE
             GROUP BY COD_ACCT_NO) C
     WHERE B.COD_ACCT_NO = C.COD_ACCT_NO
          -- AND B.COD_CC_BRN = VAR_COD_CC_BRN
       AND B.COD_ACCT_STAT <> 1
       AND B.CTR_DISB > 0
       AND NVL(LEAST(B.DAT_LAST_CHARGED, B.DAT_LAST_ACCRUAL, B.DAT_LAST_IOA),
               B.DAT_FIRST_DISB) < MIN_SCHED_DAT;
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'SELECT FAILED: CONSISTENCY CHECK 1627',
                              149);
  END;
  IF VAR_L_COUNT > 0 THEN
    BEGIN
      INSERT INTO CO_WARN_TABLE
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         VAR_COD_CC_BRN,
         'CIV_LN_ACCT_DTLS',
         'CRITICAL',
         VAR_L_COUNT ||
         '  LOANS WHERE CHARGING OR ACCRUAL DATES ARE NOT IN SCHEDULE SPAN',
         '1627');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'INSERT FAILED: CONSISTENCY CHECK NO 1627',
                                162);
    END;
  END IF;
  COMMIT;

  BEGIN
    SELECT /*+ parallel(4)*/
     COUNT(1)
      INTO VAR_L_COUNT
      FROM (SELECT A.COD_ACCT_NO,
                   A.AMT_PRINC_BALANCE,
                   A.AMT_ARREARS_PRINC,
                   B.AMT_FACE_VALUE,
                   A.AMT_DISBURSED,
                   SUM(C.AMT_PRINCIPAL - c.amt_capitalized),
                   D.NAM_STAGE
              FROM CIV_LN_ACCT_BALANCES       A,
                   CIV_LN_ACCT_DTLS           B,
                   CIV_LN_ACCT_SCHEDULE_DETLS C,
                   CIV_LN_ACCT_SCHEDULE       D
             WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
               AND A.COD_ACCT_NO = C.COD_ACCT_NO
               AND A.COD_ACCT_NO = D.COD_ACCT_NO
                  -- AND B.COD_CC_BRN = VAR_COD_CC_BRN
               AND A.COD_CC_BRN = B.COD_CC_BRN
               AND C.DATE_INSTAL >= VAR_DAT_PROCESS
               AND D.DAT_STAGE_START <= VAR_DAT_PROCESS
               AND D.DAT_STAGE_END > VAR_DAT_PROCESS
            --                 AND B.dat_of_maturity > VAR_DAT_PROCESS --To exclude PMI? TBD Akshay
             GROUP BY A.COD_ACCT_NO,
                      A.AMT_PRINC_BALANCE,
                      A.AMT_ARREARS_PRINC,
                      B.AMT_FACE_VALUE,
                      A.AMT_DISBURSED,
                      D.NAM_STAGE
            HAVING ABS((A.AMT_PRINC_BALANCE - A.AMT_ARREARS_PRINC) - /*(B.AMT_FACE_VALUE - A.AMT_DISBURSED) -*/
            SUM(C.AMT_PRINCIPAL - c.amt_capitalized)) > 0.01);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'SELECT FAILED: CONSISTENCY CHECK 1629',
                              149);
  END;
  IF VAR_L_COUNT > 0 THEN
    BEGIN
      INSERT INTO CO_WARN_TABLE
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         VAR_COD_CC_BRN,
         'CIV_LN_ACCT_SCHEDULE_DETLS',
         'CRITICAL',
         VAR_L_COUNT ||
         '  LOANS WHERE FUTURE DATED PRINCIPAL IN LN_ACCT_SCHEDULE IS NOT MATCHING WITH THE OUTSTANDING PRINCIPAL',
         '1629');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'INSERT FAILED: CONSISTENCY CHECK NO 1629',
                                162);
    END;
  END IF;
  COMMIT;

  BEGIN
    SELECT /*+parallel(128) nologging*/
     COUNT(1)
      INTO var_l_count
      FROM civ_ln_acct_dtls a,
           (SELECT cod_acct_no,
                   MIN(dat_stage_start) min_dat,
                   MAX(dat_stage_end) max_dat
              FROM civ_ln_acct_schedule
            --WHERE cod_cc_brn = var_cod_cc_brn
             GROUP BY cod_acct_no) b
     WHERE a.cod_acct_no = b.cod_acct_no
          -- AND a.cod_cc_brn = var_cod_cc_brn
       AND greatest(a.dat_last_charged, a.dat_last_restructure) NOT BETWEEN
           min_dat AND max_dat;
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1202');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 1202',
                              146);
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
         ' RECORDS WHERE greatest(a.dat_last_charged, a.dat_last_restructure) DOES NOT FALL IN ANY OF STAGES',
         '1202');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1202');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 1202',
                                159);
    END;
  END IF;
  commit;
  BEGIN
    SELECT /*+parallel(128) nologging*/
     COUNT(1)
      INTO var_l_count
      FROM (SELECT a.cod_acct_no, a.dat_last_charged
            --, a.dat_last_accrual
              FROM civ_ln_acct_dtls a,
                   (Select cod_acct_no, ctr_stage_no
                      from civ_ln_acct_schedule
                     where dat_stage_start <= var_dat_process
                       and (dat_stage_end - 1) >= var_dat_process) c,
                   civ_ln_acct_schedule d,
                   cbsfchost.ln_inst_rules e
             WHERE a.cod_acct_no = c.cod_acct_no
               AND a.cod_acct_no = d.cod_acct_no
               AND c.ctr_stage_no = d.ctr_stage_no
               AND d.cod_instal_rule = e.cod_inst_rule
                  --  AND a.cod_cc_brn = var_cod_cc_brn
                  --AND a.cod_cc_brn = d.cod_cc_brn
               AND e.COD_INST_CALC_METHOD = 'MOR'
               AND a.dat_last_charged IS NOT NULL
                  --         and a.dat_last_charged <> a.dat_last_restructure
               AND d.ctr_stage_no = 1
               AND e.flg_mnt_status = 'A');
  
  EXCEPTION
    WHEN OTHERS THEN
      --write_to_file(SQLCODE, 'Select Failed: Consistency Check 1206');
      cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 1206',
                              358);
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
         ' RECORDS WHERE ACCOUNT IS CURRENTLY MOR STAGE AND DAT_LAST_CHARGED IS NOT NULL and is not a differment case',
         '1206');
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Insert Failed: Consistency Check No 1206');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 1206',
                                371);
    END;
  END IF;
  commit;

  BEGIN
    BEGIN
      SELECT /*+parallel(128) nologging*/
       COUNT(1)
        INTO var_l_count
        FROM (SELECT cod_acct_no
                FROM civ_ln_acct_dtls a
               WHERE dat_of_maturity not in
                     (SELECT dat_stage_start
                        FROM civ_ln_acct_schedule b
                       WHERE a.cod_acct_no = b.cod_acct_no
                            --AND a.cod_cc_brn = b.cod_cc_brn
                         AND b.cod_instal_rule in
                             (SELECT cod_inst_rule
                                FROM cbsfchost.ln_inst_rules
                               WHERE cod_inst_calc_method = 'PMI'))
                    --AND b.flg_mnt_status = 'A')
                    --AND b.cod_entity_vpd = var_cod_entity_vpd)
                    --AND a.flg_mnt_status = 'A')
                    --AND a.cod_entity_vpd = var_cod_entity_vpd)
                    --   AND a.cod_cc_brn = var_cod_cc_brn
                 AND a.cod_acct_no in
                     (Select cod_acct_no from civ_ln_acct_schedule));
      --AND a.flg_mnt_status = 'A');
      --AND a.cod_entity_vpd = var_cod_entity_vpd);
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select distinct for Consistency Check 1227 failed. ');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1227 failed. ',
                                1454);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_dtls',
           'CRITICAL',
           var_l_count ||
           ' DATE OF MATURITY NOT EQUAL TO DATE START OF PMI STAGE',
           '1227');
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE, 'Insert for Consistency Check 1227 failed');
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1227 failed. ',
                                  1466);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      SELECT /*+parallel(128) nologging*/
       COUNT(1)
        INTO var_l_count
        from civ_ln_acct_dtls a
       where not exists (select 1
                from civ_ln_acct_schedule_detls b
               where a.cod_acct_no = b.cod_acct_no)
         and dat_of_maturity > MIG_DATE /*
                                 and a.cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1302 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_count || ' Records where no schedule details are provided',
           '1302');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1302 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      SELECT COUNT(A.cod_acct_no)
        INTO var_l_count
        from civ_ln_acct_dtls a,
             (Select cod_acct_no, dat_stage_start, count(1)
                from civ_ln_acct_schedule
               group by cod_acct_no, dat_stage_start
              having count(1) > 1) b
       where a.COD_aCCT_NO = b.COD_ACCT_NO /*
                                 AND A.cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1306 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           ' accounts where stage start is same for multiple stages ',
           '1306');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1306 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      SELECT COUNT(A.cod_acct_no)
        INTO var_l_count
        from civ_ln_acct_dtls a,
             (Select cod_acct_no, dat_stage_end, count(1)
                from civ_ln_acct_schedule
               group by cod_acct_no, dat_stage_end
              having count(1) > 1) b
       where a.COD_aCCT_NO = b.COD_ACCT_NO /*
                                 AND A.cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1307 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule',
           'CRITICAL',
           var_l_count ||
           ' accounts where stage end is same for multiple stages ',
           '1307');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1307 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;
  BEGIN
    BEGIN
      select /*+parallel(128) nologging*/
       COUNT(1)
        into var_l_count
        from civ_ln_acct_dtls
       WHERE cod_acct_no IN (Select cod_acct_no
                               from civ_ln_acct_schedule_detls
                              where amt_interest < 0) /*
                                 and cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check ` failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_count ||
           ' accounts where interest component is negative in schedule details ',
           '1309');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1309 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      select /*+parallel(128) nologging*/
       COUNT(1)
        into var_l_count
        from civ_ln_acct_dtls
       WHERE cod_acct_no IN
             (Select cod_acct_no
                from civ_ln_acct_schedule_detls
               where amt_principal < 0
                 and date_instal > var_dat_process) /*
                                 and cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1310 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_count ||
           'accounts where principal component is negative in schedule details ',
           '1310');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1310 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      select /*+parallel(128) nologging*/
       COUNT(1)
        into var_l_count
        from civ_ln_acct_dtls
       WHERE cod_acct_no IN
             (Select cod_acct_no
                from civ_ln_acct_schedule_detls
               where abs(amt_principal + amt_interest + amt_premium -
                         amt_instal_outst - amt_capitalized) > 0.01
                 and date_instal > var_dat_process - 1) /*
                                 and cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1311 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_count ||
           ' accounts where instalment amount is not matching with principal + interest in schedule details ',
           '1311');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1311 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      select /*+parallel(128) nologging*/
       COUNT(1)
        into var_l_count
        from civ_ln_acct_dtls A, civ_ln_acct_balances C
       where A.cod_acct_no = C.cod_acct_no
         AND C.amt_disbursed !=
            --         AND amt_face_value !=
             (Select sum(amt_principal)
                from civ_ln_acct_schedule_detls B
               where A.cod_acct_no = B.cod_acct_no) /*
                                 and cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1312 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'INFO-PRE', --PRE#12125
           var_l_count ||
           ' accounts where total principal repayment is not matching the sanctioned amount ',
           '1312');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1312 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;

  BEGIN
    BEGIN
      SELECT COUNT(distinct A.cod_acct_no)
        INTO var_l_count
        from civ_ln_acct_dtls a,
             (Select cod_acct_no, dat_start, count(1)
                from civ_ln_acct_schedule_detls
               where ctr_instal <> 1
                  or ctr_stage_no <> 1
               group by cod_acct_no, dat_start
              having count(1) > 1) b
       where a.COD_aCCT_NO = b.COD_ACCT_NO /*
                                 AND A.cod_cc_brn = var_cod_cc_brn*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1313 failed. ',
                                1702);
    END;
    IF (var_l_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_count ||
           ' accounts where stage start is same for multiple stages in stage details ',
           '1313');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1313 failed. ',
                                  1713);
      END;
    END IF;
    commit;
  END;
  BEGIN
    /*  BEGIN -- duplicate of 2031
        SELECT \*+parallel(128) nologging*\
         COUNT(1)
          INTO var_l_count
          FROM (SELECT cod_acct_no
                  FROM civ_ln_acct_dtls a
                 WHERE dat_of_maturity <>
                       (SELECT MAX(DATE_INSTAL)
                          FROM civ_ln_acct_schedule_detls b
                         WHERE a.cod_acct_no = b.cod_acct_no)
                      --  AND a.cod_cc_brn = var_cod_cc_brn
                   and A.DAT_OF_MATURITY > var_dat_process);
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                        'Select distinct for Consistency Check 1317 failed. ');
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select distinct for Consistency Check 1317 failed. ',
                              1454);
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
             ' records where DATE OF MATURITY NOT EQUAL MAX DATE IN SCHEDULE DETLS',
             '1317');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE,                          'Insert for Consistency Check 1317 failed');
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert for Consistency Check 1317 failed. ',
                                1466);
        END;
      END IF;
    */
    BEGIN
      BEGIN
        select /*+parallel(128) nologging*/
         COUNT(1)
          INTO var_l_count
          from (SELECT a.cod_acct_no,
                       b.ctr_stage_no,
                       c.ctr_stagE_no,
                       b.dat_stage_end,
                       c.dat_stage_start
                  FROM civ_ln_acct_dtls     a,
                       civ_ln_acct_schedule B,
                       civ_ln_acct_schedule C
                 WHERE a.cod_acct_no = b.cod_acct_no
                   AND a.cod_acct_no = c.cod_acct_no
                      --    AND a.cod_cc_brn = var_cod_cc_brn
                   and b.ctr_stage_no + 1 = c.ctr_stagE_no
                   and b.cod_instal_rule not in
                       (select cod_inst_rule
                          from cbsfchost.ln_inst_rules
                         where cod_inst_calc_method = 'PMI'
                           and flg_mnt_status = 'A')
                   and b.dat_stage_end != c.dat_stage_start);
      EXCEPTION
        WHEN OTHERS THEN
          --write_to_file(SQLCODE,                          'Select distinct for Consistency Check 1211 failed. ');
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Select distinct for Consistency Check 1318 failed. ',
                                  1454);
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
             ' records where start and end dates in schedule are not in sync',
             '1318');
        EXCEPTION
          WHEN OTHERS THEN
            --write_to_file(SQLCODE, 'Insert for Consistency Check 1318 failed');
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                    'Insert for Consistency Check 1318 failed. ',
                                    1466);
        END;
      END IF;
      commit;
    END;
    commit;
    BEGIN
      BEGIN
        select /*+parallel(128) nologging*/
         COUNT(distinct cod_acct_no)
          into var_l_count
          from (select b.cod_acct_no, date_instal, count(1)
                  from civ_ln_acct_dtls A, civ_ln_acct_schedule_detls b
                 where a.cod_acct_no = b.cod_acct_no
                --  and a.cod_cc_brn = var_cod_cc_brn
                 group by b.cod_acct_no, date_instal
                having count(1) > 1);
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Select distinct for Consistency Check 1320 failed. ',
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
             ' accounts where multiple records for same date are present in schedule details ',
             '1320');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                    'Insert for Consistency Check 1320 failed. ',
                                    1713);
        END;
      END IF;
      commit;
    END;
  
    BEGIN
      BEGIN
        select /*+parallel(128) nologging*/
         COUNT(distinct a.cod_acct_no)
          into var_l_count
          from civ_ln_arrears_table a, civ_ln_acct_schedule b
         where a.cod_acct_no = b.cod_acct_no
           and a.dat_arrears_due > b.dat_stage_start
           and a.dat_arrears_due <= b.dat_stage_end
           and a.cod_rule_id <> b.cod_int_rule
           and a.cod_arrear_type in ('I', 'N', 'T', 'U')
           AND EXISTS (Select 1
                  from civ_ln_acct_dtls c
                 WHERE c.cod_Acct_no = a.cod_acct_no /*
                                                                         and c.cod_cc_brn = var_cod_cc_brn*/
                );
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Select distinct for Consistency Check 1331 failed. ',
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
             ' accounts  where rule id is incorrect for regular/pmi interest type of arrears',
             '1331');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                    'Insert for Consistency Check 1331 failed. ',
                                    1713);
        END;
      END IF;
      commit;
    END;
  
    BEGIN
      BEGIN
        select /*+parallel(128) nologging*/
         COUNT(distinct a.cod_acct_no)
          into var_l_count
          from civ_ln_arrears_table a, civ_ln_acct_schedule b
         where a.cod_acct_no = b.cod_acct_no
           and a.dat_arrears_due > b.dat_stage_start
           and a.dat_arrears_due <= b.dat_stage_end
           and a.cod_rule_id <> b.cod_ioa_rule
           and a.cod_arrear_type in ('A', 'L')
           AND EXISTS (Select 1
                  from civ_ln_acct_dtls c
                 WHERE c.cod_Acct_no = a.cod_acct_no /*
                                                                         and c.cod_cc_brn = var_cod_cc_brn*/
                );
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Select distinct for Consistency Check 1332 failed. ',
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
             ' accounts  where rule id is incorrect for penalty interest type of arrears',
             '1332');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                    'Insert for Consistency Check 1332 failed. ',
                                    1713);
        END;
      END IF;
      commit;
    END;
    BEGIN
      SELECT COUNT(distinct cod_acct_no)
        INTO var_l_rec_count
        FROM (SELECT /*+PARALLEL(64) */
               cod_Acct_no
                FROM (select /*+PARALLEL(64) */
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
                       WHERE var_dat_last_process BETWEEN i.dat_stage_Start and
                             i.dat_stage_end
                            -- and i.cod_instal_rule in (select cod_inst_rule from ln_inst_rules where cod_inst_calc_method = 'EPI')
                         and j.cod_instal_rule in
                             (select cod_inst_rule
                                from cbsfchost.ln_inst_rules
                               where cod_inst_calc_method = 'EPI')
                            --and h.cod_Acct_no = i.cod_Acct_no
                         and j.cod_Acct_no = i.cod_Acct_no
                         and j.ctr_stage_no = (I.Ctr_Stage_No + 1)
                         and k.cod_acct_no = i.cod_Acct_no
                         and ((k.amt_princ_balance - k.amt_arrears_princ) -
                             j.amt_princ_repay) > 1));
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select Failed: Consistency Check 456',
                                42);
    END;
  
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
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
    BEGIN
      SELECT COUNT(1)
        INTO var_l_rec_count
        FROM (SELECT tab1.*,
                     tab2.*,
                     (amt_princ_balance - amt_arrears_princ) outbal_princ,
                     (((tab1.amt_princ_balance - tab1.amt_arrears_princ) +
                     /*NVL(tab1.amt_mor_int, 0)*/
                     0) - tab2.amt_principal) mismatch_amt
                FROM (select /*+PARALLEL(64) */
                       a.cod_Acct_no,
                       b.amt_princ_balance amt_princ_balance,
                       b.amt_arrears_princ amt_arrears_princ,
                       --NVL(d.amt_mor_int, 0) amt_mor_int,
                       a.amt_face_value amt_face_value,
                       b.amt_disbursed  amt_disbursed
                        from civ_ln_acct_dtls A, civ_ln_acct_balances b /*,
                                                                                                 (select h.cod_Acct_no cod_Acct_no,
                                                                                                         h.amt_mor_int amt_mor_int
                                                                                                    from civ_Ln_Acct_Mor_Dtls h,
                                                                                                         civ_ln_acct_Schedule i
                                                                                                   WHERE var_dat_process BETWEEN
                                                                                                         dat_stage_Start and dat_stage_end
                                                                                                     and cod_instal_rule = 5
                                                                                                     and h.cod_Acct_no = i.cod_Acct_no) d*/
                       where a.cod_acct_no = b.cod_acct_no
                      --AND a.cod_Acct_no = d.cod_Acct_no(+)
                      /*and NOT EXISTS
                                                                                           (Select 1
                                                                                                    from civ_ln_death_cases f
                                                                                                   WHERE f.cod_Acct_no = a.cod_Acct_no
                                                                                                   UNION
                                                                                                  Select 1
                                                                                                    from civ_ln_x_collat_seiz_mast t
                                                                                                   where t.cod_acct_no = a.cod_acct_no)*/
                      ) tab1,
                     (Select /*+PARALLEL(64) */
                       c.cod_Acct_no, sum(amt_principal) amt_principal
                        from civ_ln_acct_schedule_detls c
                       where NOT EXISTS
                       (select 1 from dual)
                         and date_instal > var_dat_process - 1
                       group by c.cod_Acct_no) tab2
               WHERE tab1.cod_Acct_no = tab2.cod_Acct_no
                 AND (((tab1.amt_princ_balance - tab1.amt_arrears_princ) +
                     /*NVL(tab1.amt_mor_int, 0)*/
                     0) - tab2.amt_principal) > 0.01);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select distinct for Consistency Check 1319 failed. ',
                                147);
      
    END;
    IF (var_l_rec_count > 0) THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_rec_count ||
           ' accounts where future principal repayment is not matching the principal amount ',
           '1319');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert for Consistency Check 1319 failed. ',
                                  171);
      END;
    END IF;
    BEGIN
      SELECT /*+ PARALLEL(64) */
       COUNT(1)
        INTO var_l_rec_count
        from civ_ln_acct_dtls d,
             civ_ln_acct_rates_detl a,
             (select cod_acct_no, max(ctr_from_dat_slab) max_rate_date
                from civ_ln_acct_rates_detl c
               where ctr_int_srl = 0
               group by cod_acct_no) c
       where a.cod_acct_no = c.cod_acct_no
         and a.cod_acct_no = d.cod_acct_no
         and d.dat_of_maturity > var_Dat_process
         and a.ctr_int_srl = 0
         and ctr_from_dat_slab = max_rate_date
         and a.rat_int_slab <>
             (select rat_int
                from civ_ln_acct_schedule_detls b
               where a.cod_acct_no = b.cod_acct_no
                 and date_instal = d.dat_of_maturity)
      /* and d.cod_acct_no NOT IN
                                 (Select cod_acct_no from ln_acct_fix_loat_rate)*/
      ;
    EXCEPTION
      WHEN OTHERS THEN
        --write_to_file(SQLCODE, 'Select Failed: Consistency Check 429');
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select Failed: Consistency Check 499',
                                2465);
    END;
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0, --var_cod_cc_brn
           'civ_LN_ACCT_SCHEDULE',
           'CRITICAL',
           var_l_rec_count ||
           ' Loans Where rat_int  is not matching with rat_int_slab of  ln_acct_rates_detl table',
           '499');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert Failed: Consistency Check No 498',
                                  2480);
      END;
    END IF;
  
    /*schedules with start date > process date*/
    var_l_rec_count := 0;
    BEGIN
      select count(1)
        into var_l_rec_count
        from (SELECT /*+parallel(64)*/
               cod_acct_no, MIN(dat_stage_start)
                FROM civ_ln_ACCT_schedule
               WHERE cod_acct_no IN
                     (SELECT cod_Acct_no
                        FROM civ_ln_acct_dtls
                       WHERE cod_Acct_stat = 8)
               GROUP BY cod_acct_no
              HAVING MIN(dat_stage_start) > MIG_DATE);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select Failed: Consistency Check 2030',
                                42);
    END;
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_ACCT_schedule',
           'CRITICAL',
           var_l_rec_count || ' schedules with start date > process date',
           '2030');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert Failed: Consistency Check No 2030',
                                  72);
      END;
    END IF;
  
    /*schedule vs schedule detls (dat_of_maturity)*/
    var_l_rec_count := 0;
    BEGIN
      select /*+parallel(64)*/
       count(1)
        into var_l_rec_count
        from (SELECT a.cod_Acct_no,
                     a.dat_stage_start,
                     b.max_date_instal,
                     c.dat_of_maturity
                FROM civ_ln_ACCT_SCHEDULE A,
                     (SELECT COD_ACCT_NO, MAX(DATE_INSTAL) MAX_DATE_INSTAL
                        FROM civ_ln_acct_schedule_detls
                       GROUP BY COD_ACCT_NO) B,
                     civ_ln_acct_dtls C,
                     civ_ln_acct_attributes D
               WHERE COD_INSTAL_RULE IN
                     (SELECT COD_INST_RULE
                        FROM cbsfchost.ln_INST_RULES
                       WHERE COD_INST_CALC_METHOD = 'PMI'
                         AND FLG_MNT_STATUS = 'A')
                 AND A.COD_ACCT_NO = B.COD_ACCT_NO
                 AND A.COD_ACCT_NO = C.COD_ACCT_NO
                 AND A.COD_ACCT_NO = D.COD_ACCT_NO
                 AND D.FLG_RECALLED = 'N'
                 AND (a.dat_stage_start <> b.max_date_instal or
                     b.max_date_instal <> c.dat_of_maturity));
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select Failed: Consistency Check 2031',
                                42);
    END;
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'ln_ACCT_schedule,ln_acct_schedule_detls',
           'CRITICAL',
           var_l_rec_count || ' schedules with start date > process date',
           '2031');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert Failed: Consistency Check No 2031',
                                  72);
      END;
    END IF;
  
    /*disb done no schedule detls*/
    var_l_rec_count := 0;
    /* BEGIN --duplicate of 1302
        SELECT \*+parallel(64)*\
         count(1)
          into var_l_rec_count
          FROM civ_ln_acct_dtls A
         WHERE A.CTR_DISB > 0
           AND dat_of_maturity > MIG_DATE
           AND NOT EXISTS (SELECT 1
                  FROM civ_ln_acct_schedule_detls B
                 WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
           AND COD_ACCT_STAT NOT IN (1, 5);
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 2035',
                              42);
      END;
      IF var_l_rec_count > 0 THEN
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
             var_l_rec_count || ' disb done no schedule detls',
             '2035');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 2035',
                                72);
        END;
      END IF;
    */
    /*schedule w/o schedule detls   */
    var_l_rec_count := 0;
    /*  BEGIN -- duplicate of 1302
        SELECT \*+parallel(64)*\
         count(1)
          into var_l_rec_count
          FROM civ_ln_acct_dtls A
         WHERE A.CTR_DISB > 0
           AND dat_of_maturity > MIG_DATE
           AND EXISTS (SELECT 1
                  FROM civ_ln_ACCT_SCHEDULE B
                 WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
           AND NOT EXISTS (SELECT 1
                  FROM civ_ln_acct_schedule_detls B
                 WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
           AND COD_ACCT_STAT NOT IN (1, 5);
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Select Failed: Consistency Check 2036',
                              42);
      END;
      IF var_l_rec_count > 0 THEN
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
             var_l_rec_count ||
             ' exist in ln_Acct_schedule but not exist in ln_Acct_schedule_detls',
             '2036');
        EXCEPTION
          WHEN OTHERS THEN
            cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Insert Failed: Consistency Check No 2036',
                                72);
        END;
      END IF;
    */
    /*stage start date,stage end date not in sync in schedule*/
    var_l_rec_count := 0;
    BEGIN
      SELECT /*+parallel(64)*/
       count(1)
        into var_l_rec_count
        from civ_ln_acct_dtls     a,
             civ_ln_acct_schedule b,
             civ_ln_acct_schedule c
       WHERE a.cod_acct_no = b.cod_acct_no
         AND a.cod_acct_no = c.cod_acct_no
         AND a.cod_acct_stat <> 1
         AND a.ctr_disb > 0
         AND b.cod_instal_rule NOT in
             (select cod_inst_rule
                from cbsfchost.ln_INST_RULES
               where cod_inst_calc_method = 'PMI')
         AND c.ctr_stage_no = b.ctr_stage_no + 1
         AND b.dat_stage_end <> c.dat_stage_start;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select Failed: Consistency Check 2037',
                                42);
    END;
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_acct_schedule_detls',
           'CRITICAL',
           var_l_rec_count ||
           ' stage start date,stage end date not in sync in schedule',
           '2037');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert Failed: Consistency Check No 2037',
                                  72);
      END;
    END IF;
    /* dates not in schedule span*/
    var_l_rec_count := 0;
    BEGIN
      SELECT /*+parallel(128)*/
       count(1)
        into var_l_rec_count
        FROM civ_ln_acct_attributes A,
             civ_ln_acct_dtls B,
             (SELECT COD_ACCT_NO,
                     MIN(DAT_STAGE_START) MIN_SCHED_DAT,
                     MAX(DAT_STAGE_END) MAX_SCHED_DAT
                FROM civ_ln_ACCT_SCHEDULE
               GROUP BY COD_ACCT_NO) C
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
         AND A.COD_ACCT_NO = C.COD_ACCT_NO
         AND b.cod_acct_stat <> 1
         AND b.ctr_disb > 0
         AND NVL(least(greatest(B.DAT_LAST_CHARGED, b.dat_last_restructure),
                       greatest(b.dat_last_accrual, b.dat_last_restructure),
                       NVL(b.dat_last_ioa, MIG_DATE),
                       NVL(b.dat_last_penalty_accrual, MIG_DATE)),
                 greatest(B.DAT_FIRST_DISB, b.dat_last_restructure)) <
             MIN_SCHED_DAT;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                                'Select Failed: Consistency Check 2038',
                                42);
    END;
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_ACCT_SCHEDULE',
           'CRITICAL',
           var_l_rec_count || ' dates not in schedule span',
           '2038');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                                  'Insert Failed: Consistency Check No 2038',
                                  72);
      END;
    END IF;
  
  end;
  return 0;
EXCEPTION
  WHEN OTHERS THEN
    cbsfchost.ORA_RAISERROR(SQLCODE,
                            'EXECUTION OF AP_CO_CONSIS_CHECK_LN_6 FAILED',
                            1782);
END;
/
