CREATE OR REPLACE FUNCTION "AP_CO_CONSIS_CHECK_LN_5" (var_cod_stream_id NUMBER)
  RETURN NUMBER AS

  var_l_rec_count NUMBER;
  MIG_DATE        DATE;
begin
  BEGIN
    select param_value
      into MIG_DATE
      from BB_PARAMS
     where PARAM_NAME = 'MIG_DATE';
  END;

  delete from co_warn_table where check_no between 2001 and 2041;
  commit;
  /*accrual date can not be default date*/
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      INTO var_l_rec_count
      FROM civ_ln_acct_dtls b
     WHERE dat_last_accrual = '1-jan-1950'
       AND cod_acct_stat <> 1; --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2001',
                          42);
  END;

  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count || ' accrual date can not be default date',
         '2001');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 1801',
                            72);
    END;
  END IF;

  /*ioa date can not be default date*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls b
     WHERE dat_last_ioa = '1-jan-1950'
       AND cod_acct_stat <> 1; --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2002',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count || ' ioa date can not be default date',
         '2002');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 1801',
                            72);
    END;
  END IF;

  /*penalty accrual  date can not be default date*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls b
     WHERE dat_last_penalty_accrual = '1-jan-1950'
       AND cod_acct_stat <> 1; --- count ot be 0

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2003',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' penalty accrual  date can not be default date',
         '2003');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 1801',
                            72);
    END;
  END IF;

  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      from civ_ln_acct_dtls b
     WHERE dat_first_disb > dat_last_disb
       AND cod_acct_stat <> 1; --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2004',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' First cannot be greater than the last disbursement date',
         '2004');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 1801',
                            72);
    END;
  END IF;

  /*non zero principal for accounts where disb date is null*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a, civ_ln_acct_balances b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.amt_princ_balance > 0
       AND (a.ctr_disb = 0 OR a.dat_first_disb IS NULL); --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2005',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls, civ_ln_acct_balances',
         'CRITICAL',
         var_l_rec_count ||
         ' non zero principal for accounts where disb date is null',
         '2005');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 1801',
                            72);
    END;
  END IF;

  /*non disb accounts where dat_last_charged is not null*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE a.ctr_disb = 0
       AND a.dat_last_charged IS NOT NULL
       AND cod_acct_stat <> 1
       AND EXISTS (SELECT 1
              FROM civ_ln_acct_schedule
             WHERE cod_acct_no = a.cod_acct_no); --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2006',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' non disb accounts where dat_last_charged is not null',
         '2006');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 1801',
                            72);
    END;
  END IF;

  /*non disb accounts where dat_last_accrual is not null*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE a.ctr_disb = 0
       AND a.dat_last_accrual IS NOT NULL
       AND cod_acct_stat <> 1
       AND EXISTS (SELECT 1
              FROM civ_ln_acct_schedule
             WHERE cod_acct_no = a.cod_acct_no); --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2007',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' non disb accounts where dat_last_accrual is not null',
         '2007');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2007',
                            72);
    END;
  END IF;

  /*non disb accounts where dat_last_ioa is not null*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE a.ctr_disb = 0
       AND a.dat_last_ioa IS NOT NULL
       AND cod_acct_stat <> 1 --0
       AND EXISTS (SELECT 1
              FROM civ_ln_acct_schedule
             WHERE cod_acct_no = a.cod_acct_no); --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2008',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' non disb accounts where dat_last_ioa is not null',
         '2008');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2008',
                            72);
    END;
  END IF;

  /*non disb accounts where dat_last_penalty_accrual is not null*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE a.ctr_disb = 0
       AND a.dat_last_penalty_accrual IS NOT NULL
       AND cod_acct_stat <> 1 --0
       AND EXISTS (SELECT 1
              FROM civ_ln_acct_schedule
             WHERE cod_acct_no = a.cod_acct_no); --- count ot be 0
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2009',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' non disb accounts where dat_last_penalty_accrual is not null',
         '2009');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2009',
                            72);
    END;
  END IF;

  /*checking regular rate records*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       AND ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_rates
             WHERE cod_acct_no = a.cod_acct_no
               AND ctr_int_srl = 0);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2010',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no record in civ_ln_acct_rates for disbursed accounts',
         '2010');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2010',
                            72);
    END;
  END IF;

  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       AND ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_rates_detl
             WHERE cod_acct_no = a.cod_acct_no
               AND ctr_int_srl = 0);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2011',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no record in civ_ln_acct_rates_detl for disbursed accounts',
         '2011');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2011',
                            72);
    END;
  END IF;

  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       AND ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_pricing_rate
             WHERE cod_acct_no = a.cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2012',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no record in civ_ln_acct_pricing_rate for disbursed accounts',
         '2012');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2012',
                            72);
    END;
  END IF;

  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       AND ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_pricing_rate_detl
             WHERE cod_acct_no = a.cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2013',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no record in civ_ln_acct_pricing_rate_detl for disbursed accounts',
         '2013');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2013',
                            72);
    END;
  END IF;

  /*checking penalty rate records*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     COUNT(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls a
     WHERE cod_acct_stat <> 1
       and ctr_disb > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_rates
             WHERE cod_acct_no = a.cod_acct_no
               AND cod_rate_typ = 1);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2014',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no penalty rates record in civ_ln_acct_rates for disbursed accounts',
         '2014');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2014',
                            72);
    END;
  END IF;

  /*checking penalty rate records*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
    /*  FROM civ_ln_acct_dtls A
     WHERE COD_ACCT_STAT <> 1
       AND CTR_DISB > 0
       AND NOT EXISTS
     (SELECT 1
              FROM civ_ln_acct_rates_detl
             WHERE COD_ACCT_NO = A.COD_ACCT_NO
               AND ctr_int_srl in
                   (select cod_ioa_rate
                      from civ_ln_acct_schedule
                     where COD_ACCT_NO = A.COD_ACCT_NO))*/
                     
   FROM CIV_LN_ACCT_DTLS A
 WHERE COD_ACCT_STAT <> 1
   AND CTR_DISB > 0
   AND NOT EXISTS (SELECT 1
          FROM CIV_LN_ACCT_RATES_DETL B, CIV_LN_ACCT_SCHEDULE C
         WHERE b.COD_ACCT_NO = A.COD_ACCT_NO
         AND B.cod_acct_no = c.cod_acct_no
         and ctr_int_Srl = cod_ioa_rate);

  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2015',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no penalty rates record in civ_ln_acct_rates_detl for disbursed accounts',
         '2015');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2015',
                            72);
    END;
  END IF;

  /*checking efs/ppf rate records*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls A
     WHERE COD_ACCT_STAT <> 1
       AND CTR_DISB > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_rates
             WHERE COD_ACCT_NO = A.COD_ACCT_NO
               AND cod_rate_typ = 3);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2016',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no efs rates record in civ_ln_acct_rates for disbursed accounts',
         '2016');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2016',
                            72);
    END;
  END IF;
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls A
     WHERE COD_ACCT_STAT <> 1
       AND CTR_DISB > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_acct_rates
             WHERE COD_ACCT_NO = A.COD_ACCT_NO
               AND cod_rate_typ = 2);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2017',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_acct_Dtls',
         'CRITICAL',
         var_l_rec_count ||
         ' no ppf rates record in civ_ln_acct_rates for disbursed accounts',
         '2017');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2017',
                            72);
    END;
  END IF;
  /*ctr_amd_no mismatch*/

  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(DISTINCT b.COD_ACCT_NO)
      into var_l_rec_count
      FROM civ_ln_Acct_Rates A, civ_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.cod_acct_stat <> 1
       AND CTR_DISB > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_ACCT_RATES_DETL
             WHERE COD_ACCT_NO = A.COD_ACCT_NO
               AND CTR_INT_SRL = A.CTR_INT_SRL
               AND A.CTR_AMD_NO = CTR_AMD_NO)
       AND a.cod_rate_typ IN (0, 1);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2018',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_Rates,civ_ln_ACCT_RATES_DETL',
         'CRITICAL',
         var_l_rec_count ||
         ' ctr_amd_no mismatch in civ_ln_Acct_Rates,civ_ln_ACCT_RATES_DETL for disbursed accounts',
         '2018');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2018',
                            72);
    END;
  END IF;
  /*ctr_amd_no mismatch*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(DISTINCT a.COD_ACCT_NO)
      into var_l_rec_count
      FROM civ_ln_Acct_Pricing_Rate A, civ_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.cod_acct_stat <> 1
       AND CTR_DISB > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_ACCT_Pricing_Rate_Detl
             WHERE COD_ACCT_NO = A.COD_ACCT_NO
               AND A.CTR_AMD_NO = CTR_AMD_NO);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2019',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'ln_Act_Prcng_Rt,ln_ACT_Prcng_Rt_Dtl',
         'CRITICAL',
         var_l_rec_count ||
         ' ctr_amd_no mismatch in civ_ln_Acct_Pricing_Rate,civ_ln_ACCT_Pricing_Rate_Detl for disbursed accounts',
         '2019');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2019',
                            72);
    END;
  END IF;
  /*ctr_amd_no mismatch*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/count(DISTINCT a.COD_ACCT_NO)
      into var_l_rec_count
      FROM civ_ln_Acct_Variance A, civ_ln_acct_dtls b
     WHERE a.cod_acct_no = b.cod_acct_no
       AND b.cod_acct_stat <> 1
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_Acct_Variance_Detl
             WHERE COD_ACCT_NO = A.COD_ACCT_NO
               AND A.CTR_AMD_NO = CTR_AMD_NO
               AND a.cod_rate_id = cod_rate_id);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2020',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_Variance,civ_ln_Acct_Variance_Detl',
         'CRITICAL',
         var_l_rec_count ||
         ' ctr_amd_no mismatch in civ_ln_Acct_Variance,civ_ln_Acct_Variance_Detl for disbursed accounts',
         '2020');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2020',
                            72);
    END;
  END IF;
  /*rate not found for accrual*/
  var_l_rec_count := 0;
  /*duplicate issue as this is already present in consis_2*/
  /*BEGIN
    SELECT \*+parallel(8)*\
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
              FROM civ_ln_ACCT_RATES A
             WHERE A.COD_RATE_TYP = 0
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND MIN_RATE > NVL(B.DAT_LAST_accrual, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2021',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_ACCT_RATES',
         'CRITICAL',
         var_l_rec_count || ' rate not found for accrual',
         '2021');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2021',
                            72);
    END;
  END IF;*/
  /*rate detls not found for accrual*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(ctr_from_dat_slab) MIN_RATE
              FROM civ_ln_Acct_Rates_Detl A
             WHERE A.ctr_int_srl = 0
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND MIN_RATE > NVL(B.DAT_LAST_accrual, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2022',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_Rates_Detl',
         'CRITICAL',
         var_l_rec_count || ' rate detls not found for accrual',
         '2022');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2022',
                            72);
    END;
  END IF;

  /*rate not found for charging*/
  var_l_rec_count := 0;
  /*duplicate consis - it was present in check_ln_2*/
  /* BEGIN
      SELECT \*+parallel(8)*\
       count(1)
        into var_l_rec_count
        FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
                FROM civ_ln_ACCT_RATES A
               WHERE A.COD_RATE_TYP = 0
               GROUP BY A.COD_ACCT_NO) A,
             civ_ln_acct_dtls B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
         AND b.cod_acct_stat <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Select Failed: Consistency Check 2023',
                            42);
    END;
    IF var_l_rec_count > 0 THEN
      BEGIN
        INSERT INTO co_warn_table
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           0,
           'civ_ln_ACCT_RATES',
           'CRITICAL',
           var_l_rec_count || ' rate not found for charging',
           '2023');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'Insert Failed: Consistency Check No 2023',
                              72);
      END;
    END IF;
  */
  /*rate detls not found for charging*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(ctr_from_dat_slab) MIN_RATE
              FROM civ_ln_Acct_Rates_Detl A
             WHERE A.ctr_int_srl = 0
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND MIN_RATE > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2024',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_Rates_Detl',
         'CRITICAL',
         var_l_rec_count || ' rate detls not found for charging',
         '2024');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2024',
                            72);
    END;
  END IF;

  /*int base not found for charging*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(dat_baschg_eff) MIN_base
              FROM civ_ln_int_base_hist A
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND b.ctr_disb > 0
       AND MIN_base > NVL(B.DAT_LAST_charged, B.DAT_FIRST_DISB)
       AND EXISTS (SELECT 1
              FROM civ_ln_acct_schedule
             WHERE cod_acct_no = b.cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2025',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_int_base_hist',
         'CRITICAL',
         var_l_rec_count || ' int base not found for charging',
         '2025');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2025',
                            72);
    END;
  END IF;
  /*int base not found for accrual*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(dat_baschg_eff) MIN_base
              FROM civ_ln_int_base_hist A
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B,
           civ_ln_acct_attributes c
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND A.COD_ACCT_NO = c.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND b.ctr_disb > 0
       AND MIN_base > NVL(B.DAT_LAST_accrual, B.DAT_FIRST_DISB)
       AND EXISTS (SELECT 1
              FROM civ_ln_acct_schedule
             WHERE cod_acct_no = b.cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2026',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_int_base_hist',
         'CRITICAL',
         var_l_rec_count || ' int base not found for accrual',
         '2026');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2026',
                            72);
    END;
  END IF;

  /*min penalty rate vs dat_lasT_ioa*/
  var_l_rec_count := 0;
  /*duplicate consis. already exist in consis_2*/
  /*BEGIN
    SELECT \*+parallel(8)*\
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
              FROM civ_ln_ACCT_RATES A
             WHERE A.COD_RATE_TYP = 1
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND ctr_disb > 0
       AND MIN_RATE > NVL(B.DAT_LAST_ioa, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2027',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_ACCT_RATES',
         'CRITICAL',
         var_l_rec_count || ' min penalty rate vs dat_lasT_ioa',
         '2027');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2027',
                            72);
    END;
  END IF;*/

  /*min penalty rate detls vs dat_lasT_ioa*/
  var_l_rec_count := 0;
  /*duplicate consis. Already present in consis 2*/
  /*BEGIN
    SELECT \*+parallel(8)*\
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(ctr_from_dat_slab) MIN_RATE
              FROM civ_ln_Acct_Rates_Detl A
             WHERE A.ctr_int_srl in
                   (select cod_ioa_rate
                      from civ_ln_acct_schedule
                     where cod_acct_no = a.cod_acct_no)
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND ctr_disb > 0
       AND MIN_RATE > NVL(B.DAT_LAST_ioa, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2028',
                          42);
  END;
  SELECT  COUNT(1) FROM CIV_LN_ACCT_DTLS A WHERE COD_ACCT_STAT <> 1 AND CTR_DISB > 0 AND NOT EXISTS (SELECT 1 FROM CIV_LN_ACCT_RATES_DETL WHERE COD_ACCT_NO = A.COD_ACCT_NO AND CTR_INT_SRL IN (SELECT COD_IOA_RATE FROM CIV_LN_ACCT_SCHEDULE WHERE COD_ACCT_NO = A.COD_ACCT_NO))
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_Rates_Detl',
         'CRITICAL',
         var_l_rec_count ||
         ' min penalty rate detls greater than dat_lasT_ioa',
         '2028');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2028',
                            72);
    END;
  END IF;*/

  /*min penalty rate vs dat_lasT_penalty accrual*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
              FROM civ_ln_ACCT_RATES A
             WHERE A.COD_RATE_TYP = 1
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND ctr_disb > 0
       AND MIN_RATE > NVL(B.DAT_LAST_penalty_accrual, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2029',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_ACCT_RATES',
         'CRITICAL',
         var_l_rec_count ||
         ' min penalty rate greater than dat_lasT_penalty accrual',
         '2029');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2029',
                            72);
    END;
  END IF;

  /*min penalty rate detls vs dat_lasT_penalty accrual*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM (SELECT A.COD_ACCT_NO, MIN(ctr_from_dat_slab) MIN_RATE
              FROM civ_ln_Acct_Rates_Detl A
             WHERE A.ctr_int_srl = 707
             GROUP BY A.COD_ACCT_NO) A,
           civ_ln_acct_dtls B
     WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       AND b.cod_acct_stat <> 1
       AND ctr_disb > 0
       AND MIN_RATE > NVL(B.DAT_LAST_penalty_accrual, B.DAT_FIRST_DISB);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2029',
                          42);
  END;
  IF var_l_rec_count > 0 THEN
    BEGIN
      INSERT INTO co_warn_table
        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
      VALUES
        ('LN',
         0,
         'civ_ln_Acct_Rates_Detl',
         'CRITICAL',
         var_l_rec_count ||
         ' min penalty rate detls greater than dat_lasT_penalty accrual',
         '2029');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2029',
                            72);
    END;
  END IF;
  /*To check if account has only one stage in schedule*/
  var_l_rec_count := 0;
  BEGIN
    select count(1)
      into var_l_rec_count
      from (SELECT /*+parallel(4)*/
             cod_acct_no, COUNT(1)
              FROM civ_ln_acct_schedule a
             WHERE a.cod_acct_no IN (SELECT cod_Acct_no
                                       FROM civ_ln_acct_dtls
                                      WHERE cod_Acct_stat <> 1
                                        AND ctr_disb > 0) HAVING
             COUNT(1) = 1
             GROUP BY cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2032',
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
         var_l_rec_count ||
         ' To check if account has only one stage in schedule',
         '2032');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2032',
                            72);
    END;
  END IF;

  --To check if account has pmi stage in schedule
  var_l_rec_count := 0;
  BEGIN
    select count(1)
      into var_l_rec_count
      from (SELECT /*+parallel(4)*/
             cod_acct_no, COUNT(1)
              FROM civ_ln_acct_schedule a
             WHERE a.cod_acct_no IN (SELECT cod_Acct_no
                                       FROM civ_ln_acct_dtls
                                      WHERE cod_Acct_stat <> 1
                                        AND ctr_disb > 0) --685
               AND cod_instal_rule in
                   (select cod_inst_rule
                      from cbsfchost.ln_INST_RULES
                     where cod_inst_calc_method = 'PMI') HAVING
             COUNT(1) <> 1
             GROUP BY cod_acct_no);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2033',
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
         ' To check if account has pmi stage in schedule',
         '2033');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2033',
                            72);
    END;
  END IF;
  /*disb done no schedule*/
  var_l_rec_count := 0;
  BEGIN
    SELECT /*+parallel(4)*/
     count(1)
      into var_l_rec_count
      FROM civ_ln_acct_dtls A
     WHERE A.CTR_DISB > 0
       AND NOT EXISTS (SELECT 1
              FROM civ_ln_ACCT_SCHEDULE B
             WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
       AND COD_ACCT_STAT NOT IN (1, 5);
  EXCEPTION
    WHEN OTHERS THEN
      cbsfchost.ORA_RAISERROR(SQLCODE,
                          'Select Failed: Consistency Check 2034',
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
         var_l_rec_count || ' disb done no schedule',
         '2034');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2034',
                            72);
    END;
  END IF;

  /* Regular accounts whose CRR code is updated as Suspended*/
  var_l_rec_count := 0;
  BEGIN

    select /*+parallel(128)*/
     count(1)
      into var_l_rec_count
      from Civ_Ln_Acct_Dtls a, civ_ln_acct_balances b
     WHERE a.COD_aCCT_NO in
           (select cod_accT_no
              from civ_ac_acct_crr_code
             where cod_crr_from in
                   (select cod_crr
                      from cbsfchost.ac_crr_codes
                     where flg_accr_status = 'S'))
       and flg_accr_status != 'S'
       and a.cod_Acct_no = b.cod_Acct_no
       and (b.amt_princ_balance > 0 or b.amt_arrears_regular_int > 0 or
           b.amt_arrears_princ > 0 or b.amt_arrears_fees > 0);
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
         'Civ_Ln_Acct_Dtls',
         'INFO',
         var_l_rec_count ||
         ' Regular accounts whose CRR code is updated as Suspended. INFO As per Iteration_3.3_Post_Consis.xls',
         '2039');
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'Insert Failed: Consistency Check No 2038',
                            72);
    END;
  END IF;

--TBC : Akshay : 2040 Commented this as civ_ln_acct_schedule doesn't have amt_princ_arrears. Could be Bandhan specific
--  BEGIN
--    SELECT /*+parallel(128)*/
--     COUNT(1)
--      INTO var_l_rec_count
--      FROM (select a.cod_acct_no,
--                   min(dat_arrears_due),
--                   (MIG_DATE + 1) - min(dat_arrears_due),
--                   c.current_os_days
--              from civ_ln_arrears_table  a,
--                   civ_NPA_PROVISION_DTLS c, --cv_NPA_PROVISION_DTLS
--                   civ_ln_acct_dtls       d --cv_ln_acct_dtls?
--             where amt_Arrears_due > 0
--               and a.cod_acct_no = c.cod_acct_No
--               and a.cod_acct_no = d.cod_acct_no
--              /* and d.cod_prod in (select e.cod_prod
--                                    from bb_map_repay_mode e
--                                   where e.prod_flag = 'GB')*/
--             group by a.cod_Acct_no, c.current_os_days
--            having((MIG_DATE + 1) - min(dat_arrears_due)) <> c.current_os_days);
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE,
--                          'Select Failed: Consistency Check 2040',
--                          42);
--  END;
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'Civ_Ln_Acct_Dtls',
--         'CRITICAL',
--         var_l_rec_count ||
--         ' Arrears wise DPD and Bank Provided DPD is not matching',
--         '2040');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                            'Insert Failed: Consistency Check No 2040',
--                            72);
--    END;
--  END IF;

--TBC : Akshay : 2041 Commented this as civ_ln_acct_schedule doesn't have amt_princ_arrears. Could be Bandhan specific
--  BEGIN
--    select /*+parallel(256)*/
--     count(1)
--      INTO var_l_rec_count
--      from civ_ln_acct_balances a, civ_ln_acct_schedule b --cv_ln_acct_schedule
--     where a.cod_acct_No = b.cod_acct_No
--       and a.amt_arrears_princ <> b.amt_princ_arrears;
--  EXCEPTION
--    WHEN OTHERS THEN
--      cbsfchost.ORA_RAISERROR(SQLCODE,
--                          'Select Failed: Consistency Check 2041',
--                          42);
--  END;
--  IF var_l_rec_count > 0 THEN
--    BEGIN
--      INSERT INTO co_warn_table
--        (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
--      VALUES
--        ('LN',
--         0,
--         'civ_ln_acct_balances',
--         'INFO',
--         var_l_rec_count ||
--         ' where amt_arrears_princ is not matching with source and target table. INFO As per Iteration_3.3_Post_Consis.xls',
--         '2041');
--    EXCEPTION
--      WHEN OTHERS THEN
--        cbsfchost.ORA_RAISERROR(SQLCODE,
--                            'Insert Failed: Consistency Check No 2040',
--                            72);
--    END;
--  END IF;
  return 0;

end;
/
