CREATE OR REPLACE FUNCTION "AP_CO_CONSIS_CHECK_LN_2_NO_STREAM" (VAR_COD_STREAM_ID NUMBER)
  RETURN NUMBER AS
  VAR_DAT_PROCESS        DATE;
  VAR_DAT_LAST_PROCESS   DATE;
  VAR_L_COUNT            NUMBER;
  var_makerid            CHAR(12) := 'CONVTELLER';
  var_bank_mast_dt_to_use VARCHAR2(1) := 'N';
  VAR_COD_CC_BRN         NUMBER:=0;
  VAR_L_COD_PROD_LN_FROM NUMBER;
  VAR_L_COD_PROD_LN_TO   NUMBER;
  CURSOR CONSIS_BRN IS
    SELECT COD_CC_BRN
      FROM CONV_BRN_STREAM_PROC_XREF A
     WHERE A.COD_PROC_NAM = 'AP_CO_CONSIS_CHECK_LN_2'
       AND A.COD_STREAM_ID = VAR_COD_STREAM_ID
       AND A.FLG_PROCESSED = 'N'; 
BEGIN
  
  
  SELECT DAT_PROCESS, DAT_LAST_PROCESS
    INTO VAR_DAT_PROCESS, VAR_DAT_LAST_PROCESS
    FROM cbsfchost.BA_BANK_MAST;

   

  










  
   
    
    BEGIN
      DELETE FROM CO_WARN_TABLE
       WHERE  CHECK_NO BETWEEN 1601 AND 1700;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                      'DELETE FAILED IN AP_CO_CONSIS_CHECK_LN.',
                      124);
    END;
    COMMIT;
    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A, CIV_LN_ACCT_BALANCES B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
         AND B.AMT_PRINC_BALANCE > 0
         AND (A.CTR_DISB = 0 OR A.DAT_FIRST_DISB IS NULL)
         and a.flg_mnt_status ='A'
        ;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1601',
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
           '  LOANS WHERE PRINCIPAL BALANCE > ZERO AND CTR_DISB = ZERO',
           '1601');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1601',
                              162);
      END;
    END IF;
    COMMIT;
    BEGIN
      SELECT /*+parallel(4)*/
       COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE COD_ACCT_STAT <> 1
        
         AND CTR_DISB > 0
         and a.flg_mnt_status ='A'
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_RATES
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND CTR_INT_SRL = 0);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1602',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT || '  LOANS WHERE REGULAR RATE IS NOT PRESENT',
           '1602');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1602',
                              162);
      END;
    END IF;
    COMMIT;
    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE COD_ACCT_STAT <> 1
       and a.flg_mnt_status ='A'
        
         AND CTR_DISB > 0
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_RATES_DETL
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND CTR_INT_SRL = 0);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1603',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE REGULAR RATE DETAILS IS NOT PRESENT',
           '1603');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1603',
                              162);
      END;
    END IF;
        COMMIT;
    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE COD_ACCT_STAT <> 1
       and a.flg_mnt_status ='A'
        
         AND CTR_DISB > 0
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_RATES
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND COD_RATE_TYP = 1);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1604',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT || '  LOANS WHERE PENALTY RATE  IS NOT PRESENT',
           '1604');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1604',
                              162);
      END;
    END IF;
        COMMIT;
    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_RATES B, CIV_LN_ACCT_DTLS A
       WHERE B.COD_ACCT_NO = A.COD_ACCT_NO
         AND COD_ACCT_STAT <> 1
         AND CTR_DISB > 0
         and a.flg_mnt_status ='A'
       
         AND COD_RATE_TYP = 0
         AND CTR_INT_SRL <> 0;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1605',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE CTR_INT_SRL IS INCORRECT FOR REGULAR RATE',
           '1605');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1605',
                              162);
      END;
    END IF;
        COMMIT;
    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE COD_ACCT_STAT <> 1
       and a.flg_mnt_status ='A'
        
         AND CTR_DISB > 0
         AND NOT EXISTS
       (SELECT 1
                FROM CIV_LN_ACCT_RATES_DETL
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND CTR_INT_SRL IN
                     (SELECT COD_IOA_RATE
                        FROM cbsfchost.LN_SCHED_TYPES
                       WHERE COD_PROD = A.COD_PROD
                         AND COD_SCHED_TYPE = A.COD_SCHED_TYPE));
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1606',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE PENALTY RATE DETAILS IS NOT PRESENT',
           '1606');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1606',
                              162);
      END;
    END IF;
        COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE COD_ACCT_STAT <> 1
         and a.flg_mnt_status ='A'
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_RATES
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND COD_RATE_TYP = 3);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1607',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT || '  LOANS WHERE EFS RATE DETAILS IS NOT PRESENT',
           '1607');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1607',
                              162);
      END;
    END IF;
        COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERECOD_ACCT_STAT <> 1
         and a.flg_mnt_status ='A'
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_RATES
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND COD_RATE_TYP = 2);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1608',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT || '  LOANS WHERE PPF RATE DETAILS IS NOT PRESENT',
           '1608');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1608',
                              162);
      END;
    END IF;
        COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT A.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_RATES A, CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
         AND B.COD_ACCT_STAT <> 1
         AND CTR_DISB > 0
         and a.flg_mnt_status ='A'
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_RATES_DETL
               WHERE COD_ACCT_NO = A.COD_ACCT_NO
                 AND CTR_INT_SRL = A.CTR_INT_SRL
                 AND A.CTR_AMD_NO = CTR_AMD_NO)
         AND A.COD_RATE_TYP IN (0, 1);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1609',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE AMD NUMBER IS NOT IN SYNC WITHIN RATES TABLES',
           '1609');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1609',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT A.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_VARIANCE A, CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and a.flg_mnt_status ='A'
         AND B.COD_ACCT_STAT <> 1
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_VARIANCE_DETL
               WHERE COD_ACCT_NO = A.COD_ACCT_NO);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1610',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_VARIANCE',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE RATE VARAINCE TABLES ARE NOT IN SYNC',
           '1610');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1610',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_RATES A, CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
        
         AND A.COD_RATE_TYP IN (1, 0)
         AND FLG_INCR_CUM <> 'C'
         and a.flg_mnt_status ='A';
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1611',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT || '  LOANS WHERE RATES ARE NOT CUMULATIVE TYPE',
           '1611');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1611',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_RATES A, CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
        
         AND A.COD_RATE_TYP IN (1, 0)
         and a.flg_mnt_status ='A'
         AND COD_SLAB_TYP <> 1;

    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1612',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT || '  LOANS WHERE RATES WITH SLAB TYPE <> ONE',
           '1612');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1612',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
                FROM CIV_LN_ACCT_RATES A
               WHERE A.COD_RATE_TYP = 0
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_ACCRUAL, B.DAT_FIRST_DISB); 
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1613',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN RATE DATE IS GREATER TO LSAT ACCRUAL DATE',
           '1613');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1613',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
                FROM CIV_LN_ACCT_RATES A
               WHERE A.COD_RATE_TYP = 0
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
         
         AND B.COD_ACCT_STAT <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_CHARGED, B.DAT_FIRST_DISB); 
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1614',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN RATE DATE IS GREATER TO LAST CHARGING DATE',
           '1614');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1614',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT/*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(CTR_FROM_DAT_SLAB) MIN_RATE
                FROM CIV_LN_ACCT_RATES_DETL A
               WHERE A.CTR_INT_SRL = 0
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_ACCRUAL, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1615',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN RATE DETLS DATE IS GREATER TO LAST ACCRUAL DATE',
           '1615');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1615',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT/*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(CTR_FROM_DAT_SLAB) MIN_RATE
                FROM CIV_LN_ACCT_RATES_DETL A
               WHERE A.CTR_INT_SRL = 0
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_CHARGED, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1616',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN RATE DETLS DATE IS GREATER TO LAST CHARGING DATE',
           '1616');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1616',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT/*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(CTR_FROM_DAT_SLAB) MIN_RATE
                FROM CIV_LN_ACCT_RATES_DETL A
               WHERE A.CTR_INT_SRL = 0
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_CHARGED, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1617',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN RATE DATE IS GREATER TO LAST CHARGED  DATE',
           '1617');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1617',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT/*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(CTR_FROM_DAT_SLAB) MIN_RATE
                FROM CIV_LN_ACCT_RATES_DETL A
               WHERE A.CTR_INT_SRL = 0
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND MIN_RATE > NVL(B.DAT_LAST_CHARGED, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1618',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES_DETL',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN RATE DETLS DATE IS GREATER TO LAST CHARGING DATE',
           '1618');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1618',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(DAT_BASCHG_EFF) MIN_BASE
                FROM CIV_LN_INT_BASE_HIST A
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND B.CTR_DISB > 0
         AND MIN_BASE > NVL(B.DAT_LAST_CHARGED, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1619',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_INT_BASE_HIST',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN INTEREST BASE DATE IS GREATER TO LAST CHARGING DATE',
           '1619');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1619',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(DAT_BASCHG_EFF) MIN_BASE
                FROM CIV_LN_INT_BASE_HIST A
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND B.CTR_DISB > 0
         AND MIN_BASE > NVL(B.DAT_LAST_ACCRUAL, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1620',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_INT_BASE_HIST',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN INTEREST BASE DATE IS GREATER TO LAST ACCRUAL DATE',
           '1620');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1620',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT/*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(DAT_EFF_INT_INDX) MIN_RATE
                FROM CIV_LN_ACCT_RATES A 
               WHERE A.COD_RATE_TYP = 1
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND B.COD_ACCT_STAT <> 1
         AND CTR_DISB > 0
         AND MIN_RATE > NVL(B.DAT_LAST_IOA, B.DAT_FIRST_DISB);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1621',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN PENALTY INTEREST RATE DATE IS GREATER TO LAST PENALTY APPLICATION DATE',
           '1621');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1621',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO, MIN(CTR_FROM_DAT_SLAB) MIN_RATE
                FROM CIV_LN_ACCT_RATES_DETL A, CIV_LN_ACCT_DTLS C
               WHERE A.COD_ACCT_NO = C.COD_ACCT_NO
               and c.flg_mnt_status ='A'
                
                 AND A.CTR_INT_SRL IN
                     (SELECT COD_IOA_RATE
                        FROM cbsfchost.LN_SCHED_TYPES
                       WHERE COD_PROD = C.COD_PROD
                         AND COD_SCHED_TYPE = C.COD_SCHED_TYPE)
               GROUP BY A.COD_ACCT_NO) A,
             CIV_LN_ACCT_DTLS B
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
        
         AND B.COD_ACCT_STAT <> 1
         AND CTR_DISB > 0
         AND MIN_RATE > NVL(B.DAT_LAST_IOA, B.DAT_FIRST_DISB); 
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1622',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_RATES',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE MIN PENALTY INTEREST RATE DATE DETAIL IS GREATER TO LAST PENALTY APPLICATION DATE',
           '1622');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1622',
                              162);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS A
       WHERE A.CTR_DISB > 0
       and a.flg_mnt_status ='A'
        
         AND NOT EXISTS (SELECT 1
                FROM CIV_LN_ACCT_SCHEDULE B
               WHERE B.COD_ACCT_NO = A.COD_ACCT_NO)
         AND COD_ACCT_STAT NOT IN (1, 5);
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1624',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_SCHEDULE',
           'CRITICAL',
           VAR_L_COUNT || '  DISBURSED LOANS WHERE SCHEDULE IS NOT PRESENT',
           '1624');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1624',
                              162);
      END;
    END IF;
    COMMIT;


    BEGIN
      SELECT/*+ parallel(4)*/ COUNT(1)
        INTO VAR_L_COUNT
        FROM CIV_LN_ACCT_DTLS     A,
             CIV_LN_ACCT_SCHEDULE B,
             CIV_LN_ACCT_SCHEDULE C
       WHERE A.COD_ACCT_NO = B.COD_ACCT_NO
       and a.flg_mnt_status ='A'
        
         AND A.COD_ACCT_NO = C.COD_ACCT_NO
         AND A.COD_ACCT_STAT <> 1
         AND A.CTR_DISB > 0
         AND B.COD_INSTAL_RULE NOT IN
             (SELECT COD_INST_RULE
                FROM cbsfchost.LN_INST_RULES
               WHERE COD_INST_CALC_METHOD = 'PMI')
         AND C.CTR_STAGE_NO = B.CTR_STAGE_NO + 1
         AND B.DAT_STAGE_END <> C.DAT_STAGE_START;
    EXCEPTION
      WHEN OTHERS THEN
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1626',
                            149);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_SCHEDULE',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE SCHEDULE SUMMARY DATES ARE NOT IN SYNC',
           '1626');
      EXCEPTION
        WHEN OTHERS THEN
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1626',
                              162);
      END;
    END IF;
    COMMIT;


    
    COMMIT;
    
    COMMIT;
    
    BEGIN
      SELECT /*+ parallel(4)*/ COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO,
                     SUM(A.BAL_INT_ARREARS_SUSP) BAL_INT_ARREARS_SUSP_TOT
                FROM CIV_LN_ACCT_INT_BALANCES A
               GROUP BY A.COD_ACCT_NO) K,
             CIV_LN_ACCT_DTLS B,
             (SELECT C.COD_ACCT_NO, SUM(AMT_ARREARS_DUE) ARREARS
                FROM CIV_LN_ARREARS_TABLE C
               WHERE C.COD_ARREAR_TYPE IN ('N', 'L', 'U')
               GROUP BY C.COD_ACCT_NO) E
       WHERE K.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND K.COD_ACCT_NO = E.COD_ACCT_NO
         AND K.BAL_INT_ARREARS_SUSP_TOT <> E.ARREARS;
    EXCEPTION
      WHEN OTHERS THEN
        
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1632',
                            1226);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_INT_BALANCE_DTLS',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE BAL_INT_ARREARS_SUSP <> IN_ARREARS_TABLE (N, L, U)',
           '1632');
      EXCEPTION
        WHEN OTHERS THEN
          
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1632',
                              1238);
      END;
    END IF;
    COMMIT;

    BEGIN
      SELECT /*+ parallel(4)*/COUNT(DISTINCT B.COD_ACCT_NO)
        INTO VAR_L_COUNT
        FROM (SELECT A.COD_ACCT_NO,
                     SUM(A.BAL_INT_ARREARS) BAL_INT_ARREARS_TOT
                FROM CIV_LN_ACCT_INT_BALANCES A
               GROUP BY A.COD_ACCT_NO) K,
             CIV_LN_ACCT_DTLS B,
             (SELECT C.COD_ACCT_NO, SUM(AMT_ARREARS_DUE) ARREARS
                FROM CIV_LN_ARREARS_TABLE C
               WHERE C.COD_ARREAR_TYPE IN ('I', 'A', 'T')
               GROUP BY C.COD_ACCT_NO) E
       WHERE K.COD_ACCT_NO = B.COD_ACCT_NO
       and b.flg_mnt_status ='A'
        
         AND K.COD_ACCT_NO = E.COD_ACCT_NO
         AND K.BAL_INT_ARREARS_TOT <> E.ARREARS;
    EXCEPTION
      WHEN OTHERS THEN
        
        cbsfchost.ORA_RAISERROR(SQLCODE,
                            'SELECT FAILED: CONSISTENCY CHECK 1633',
                            1226);
    END;
    IF VAR_L_COUNT > 0 THEN
      BEGIN
        INSERT INTO CO_WARN_TABLE
          (COD_MODULE, COD_CC_BRN, TABLE_NAME, SEVERITY, REMARKS, CHECK_NO)
        VALUES
          ('LN',
           VAR_COD_CC_BRN,
           'CIV_LN_ACCT_INT_BALANCE_DTLS',
           'CRITICAL',
           VAR_L_COUNT ||
           '  LOANS WHERE BAL_INT_ARREARS <> IN_ARREARS_TABLE (I,A,T)',
           '1633');
      EXCEPTION
        WHEN OTHERS THEN
          
          cbsfchost.ORA_RAISERROR(SQLCODE,
                              'INSERT FAILED: CONSISTENCY CHECK NO 1632',
                              1238);
      END;
    END IF;
commit;
    
    

    
    COMMIT;
    
    
  
  
  
  COMMIT;
  RETURN 0;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

    cbsfchost.ORA_RAISERROR(SQLCODE,
                        'EXECUTION OF AP_CO_CONSIS_CHECK_LN_2 FAILED',
                        1655);
    RETURN 95;
END;
/
