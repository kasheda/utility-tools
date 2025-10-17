CREATE OR REPLACE FUNCTION npa_run_consis_checks(p_date_run IN DATE DEFAULT SYSDATE)
  RETURN NUMBER AS
  v_step VARCHAR2(200);
BEGIN
  BEGIN
    v_step := 'INSERT #3';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO npa_consis_check
      (consis_no, date_run, cod_Acct_no_pri, cod_acct_no_sec, npa_date)
      SELECT /*+parallel(32) FULL(LAD) FULL(B)*/
      DISTINCT 3, p_date_run, '', a.cod_Acct_No, a.dat_npa
        FROM CO_ac_Acct_crr_code   a,
             CO_ln_acct_attributes b,
             CO_LN_ACCT_DTLS       LAD
       WHERE a.cod_acct_no = b.cod_acct_no
         AND a.cod_acct_no = LAD.cod_acct_no
         AND a.flg_mnt_status = 'A'
         AND b.flg_mnt_status = 'A'
         AND LAD.flg_mnt_status = 'A'
         AND LAD.COD_ACCT_STAT NOT IN (1, 5, 11)
         AND DECODE(a.Cod_Crr_Mvmt_Reason, ' ', 'X', a.Cod_Crr_Mvmt_Reason) <> 'X'
         AND NVL(a.dat_npa, DATE '1950-01-01') <>
             NVL(b.dat_npl, DATE '1950-01-01');
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #4';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO npa_consis_check
      (consis_no, date_run, cod_Acct_no_pri, cod_acct_no_sec, npa_date)
      SELECT /*+parallel(32) FULL(B)*/
      DISTINCT 4, p_date_run, '', a.cod_Acct_No, a.dat_npa
        FROM CO_ac_Acct_crr_code a, CO_ch_acct_mast b
       WHERE a.cod_acct_no = b.cod_acct_no
         AND a.flg_mnt_status = 'A'
         AND b.flg_mnt_status = 'A'
         AND B.COD_ACCT_STAT NOT IN (1, 5)
         AND DECODE(a.Cod_Crr_Mvmt_Reason, ' ', 'X', a.Cod_Crr_Mvmt_Reason) <> 'X'
         AND NVL(a.dat_npa, DATE '1950-01-01') <>
             NVL(b.dat_npl, DATE '1950-01-01');
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #5';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO npa_consis_check
      (consis_no,
       date_run,
       cod_Acct_no_pri,
       cod_acct_no_sec,
       cod_crr_cust,
       flg_accr_status)
      SELECT /*+parallel(32)*/
      DISTINCT 5,
               p_date_run,
               '',
               a.cod_Acct_No,
               a.cod_crr_cust,
               b.flg_accr_status
        FROM CO_ac_Acct_crr_code    a,
             CO_ln_acct_dtls        b,
             cbsfchost.ac_crr_codes c
       WHERE a.cod_acct_no = b.cod_acct_no
         AND a.cod_crr_cust = c.cod_crr
         AND a.flg_mnt_status = 'A'
         AND b.flg_mnt_status = 'A'
         AND c.flg_mnt_status = 'A'
         AND b.cod_Acct_stat NOT IN (1, 5, 11)
         AND b.flg_accr_status <> c.flg_accr_status
         AND DECODE(a.Cod_Crr_Mvmt_Reason, ' ', 'X', a.Cod_Crr_Mvmt_Reason) <> 'X';
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #6';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO npa_consis_check
      (consis_no,
       date_run,
       cod_Acct_no_pri,
       cod_acct_no_sec,
       cod_crr_cust,
       flg_accr_status)
      SELECT /*+parallel(32) FULL(B) FULL(C)*/
      DISTINCT 6,
               p_date_run,
               '',
               a.cod_Acct_No,
               a.cod_crr_cust,
               b.flg_accr_status
        FROM CO_ac_Acct_crr_code    a,
             CO_ch_acct_mast        b,
             cbsfchost.ac_crr_codes c
       WHERE a.cod_acct_no = b.cod_acct_no
         AND a.cod_crr_cust = c.cod_crr
         AND a.flg_mnt_status = 'A'
         AND b.flg_mnt_status = 'A'
         AND c.flg_mnt_status = 'A'
         AND b.cod_Acct_stat NOT IN (1, 5)
         AND b.flg_accr_status <> c.flg_accr_status
         AND DECODE(a.Cod_Crr_Mvmt_Reason, ' ', 'X', a.Cod_Crr_Mvmt_Reason) <> 'X';
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIn
    v_step := 'INSERT #7';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO npa_consis_check
      (consis_no,
       date_run,
       cod_Acct_no_pri,
       cod_acct_no_sec,
       cod_crr_cust,
       npa_date)
      SELECT /*+parallel(32)*/
      DISTINCT 7, p_date_run, '', a.cod_Acct_No, a.cod_crr_cust, a.dat_npa
        FROM CO_ac_Acct_crr_code a, CO_ch_acct_cust_xref b
       WHERE a.cod_Acct_no = b.cod_Acct_no
         AND a.flg_mnt_status = 'A'
         AND b.flg_mnt_status = 'A'
         AND cod_crr_cust >= 1050
         AND NOT EXISTS (SELECT 1
                FROM CO_LN_ACCT_DTLS LAD
               WHERE LAD.COD_ACCT_NO = B.COD_ACCT_NO
                 AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                 AND FLG_MNT_STATUS = 'A')
         AND NOT EXISTS
       (SELECT 1
                FROM CO_CH_ACCT_MAST CAM
               WHERE CAM.COD_ACCT_NO = B.COD_ACCT_NO
                 AND CAM.COD_ACCT_STAT IN (1, 5)
                 AND FLG_MNT_STATUS = 'A'
                 AND COD_PROD IN (SELECT COD_PROD
                                    FROM CBSFCHOST.CH_PROD_MAST CPM
                                   WHERE CPM.COD_TYP_PROD = 'A'
                                     AND FLG_MNT_STATUS = 'A'))
         AND NVL(dat_npa, DATE '1950-01-01') = DATE
       '1950-01-01'
         AND DECODE(a.Cod_Crr_Mvmt_Reason, ' ', 'X', a.Cod_Crr_Mvmt_Reason) <> 'X';
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #8';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO npa_consis_check
      (consis_no,
       date_run,
       cod_Acct_no_pri,
       cod_acct_no_sec,
       cod_crr_cust,
       npa_date)
      SELECT /*+parallel(32)*/
      DISTINCT 8, p_date_run, '', a.cod_Acct_No, a.cod_crr_cust, a.dat_npa
        FROM CO_ac_Acct_crr_code a, CO_ch_acct_cust_xref b
       WHERE a.cod_Acct_no = b.cod_Acct_no
         AND a.flg_mnt_status = 'A'
         AND b.flg_mnt_status = 'A'
         AND cod_crr_cust < 1050
         AND NOT EXISTS (SELECT 1
                FROM CO_LN_ACCT_DTLS LAD
               WHERE LAD.COD_ACCT_NO = B.COD_ACCT_NO
                 AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                 AND FLG_MNT_STATUS = 'A')
         AND NOT EXISTS
       (SELECT 1
                FROM CO_CH_ACCT_MAST CAM
               WHERE CAM.COD_ACCT_NO = B.COD_ACCT_NO
                 AND CAM.COD_ACCT_STAT IN (1, 5)
                 AND FLG_MNT_STATUS = 'A'
                 AND COD_PROD IN (SELECT COD_PROD
                                    FROM CBSFCHOST.CH_PROD_MAST CPM
                                   WHERE CPM.COD_TYP_PROD = 'A'
                                     AND FLG_MNT_STATUS = 'A'))
         AND NVL(dat_npa, DATE '1950-01-01') <> DATE
       '1950-01-01'
         AND DECODE(a.Cod_Crr_Mvmt_Reason, ' ', 'X', a.Cod_Crr_Mvmt_Reason) <> 'X';
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #9';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO NPA_CONSIS_CHECK
      (CONSIS_NO,
       DATE_RUN,
       COD_ACCT_NO_PRI,
       COD_CUST_ID,
       COD_ACCT_NO_SEC,
       NPA_DATE,
       COD_CRR_CUST,
       COD_CRR_TO,
       FLG_ACCR_STATUS,
       DAT_COOLING_PRD_END)
      SELECT /*+parallel(32)*/
       9,
       p_date_run,
       NULL,
       COD_CUST_ID,
       COD_ACCT_NO,
       NULL,
       NULL,
       NULL,
       FLG_ACCR_STATUS,
       NULL
        FROM CO_LN_ACCT_DTLS LAD
       WHERE COD_ACCT_STAT = 10
         AND FLG_ACCR_STATUS = 'N'
         AND FLG_MNT_STATUS = 'A';
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #10';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO NPA_CONSIS_CHECK
      (CONSIS_NO,
       DATE_RUN,
       COD_ACCT_NO_PRI,
       COD_CUST_ID,
       COD_ACCT_NO_SEC,
       NPA_DATE,
       COD_CRR_CUST,
       COD_CRR_TO,
       FLG_ACCR_STATUS,
       DAT_COOLING_PRD_END,
       COD_ACCT_STAT)
      SELECT /*+parallel(32) FULL(CAM)*/
       10,
       p_date_run,
       NULL,
       CAM.COD_CUST,
       CAM.COD_ACCT_NO,
       NULL,
       NULL,
       NULL,
       CAM.FLG_ACCR_STATUS,
       NULL,
       CAM.COD_ACCT_STAT
        FROM CO_CH_ACCT_MAST CAM, CO_AC_ACCT_CRR_CODE AC
       WHERE COD_ACCT_STAT NOT IN (2, 3, 10, 11, 1, 5)
         AND FLG_ACCR_STATUS = 'S'
         AND AC.COD_ACCT_NO = CAM.COD_ACCT_NO
         AND AC.FLG_MNT_STATUS = 'A'
         AND CAM.FLG_MNT_STATUS = 'A';
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  /* v_step := 'INSERT #14';
  INSERT \*+enable_parallel_dml parallel(32)*\
  INTO NPA_CONSIS_CHECK
    (CONSIS_NO,
     DATE_RUN,
     COD_ACCT_NO_PRI,
     COD_CUST_ID,
     COD_ACCT_NO_SEC,
     NPA_DATE,
     COD_CRR_CUST,
     COD_CRR_TO,
     FLG_ACCR_STATUS,
     DAT_COOLING_PRD_END)
    SELECT \*+PARALLEL(32)*\
     14,
     p_date_run,
     NULL,
     COD_CUST_ID,
     COD_ACCT_NO,
     DAT_NPA,
     COD_CRR_CUST,
     COD_CRR_TO,
     NULL,
     NULL
      FROM CO_AC_ACCT_CRR_CODE AC
     WHERE AC.COD_CRR_CUST >= 1050
       AND AC.DPD_ARREARS = 0
       AND AC.COD_CRR_MVMT_REASON = 'E'
       AND cod_Acct_no NOT IN
           (SELECT cod_acct_no
              FROM Civ_ac_acct_preferences
             WHERE cod_plan_id IN
                   (SELECT cod_pref_id
                      FROM CBSFCHOST.ac_preferences
                     WHERE cod_rev_mvmnt <> 2
                       AND flg_mnt_status = 'A')
               AND flg_mnt_status = 'A');*/
  BEGIN
    v_step := 'INSERT #15';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO NPA_CONSIS_CHECK
      (CONSIS_NO,
       DATE_RUN,
       COD_ACCT_NO_PRI,
       COD_CUST_ID,
       COD_ACCT_NO_SEC,
       NPA_DATE,
       COD_CRR_CUST,
       COD_CRR_TO,
       FLG_ACCR_STATUS,
       DAT_COOLING_PRD_END)
      SELECT /*+PARALLEL(32)*/
       15,
       p_date_run,
       NULL,
       COD_CUST_ID,
       COD_ACCT_NO,
       DAT_NPA,
       COD_CRR_FROM,
       COD_CRR_TO,
       NULL,
       NULL
        FROM CBSFCHOST.AC_ACCT_CRR_CODE AC
       WHERE AC.COD_CRR_CUST >= 1050
         AND COD_CRR_TO >= 1050
         AND COD_CRR_TO < COD_CRR_FROM;
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #16';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO NPA_CONSIS_CHECK
      (CONSIS_NO,
       DATE_RUN,
       COD_ACCT_NO_PRI,
       COD_CUST_ID,
       COD_ACCT_NO_SEC,
       NPA_DATE,
       COD_CRR_CUST,
       COD_CRR_TO,
       FLG_ACCR_STATUS,
       DAT_COOLING_PRD_END)
      SELECT /*+PARALLEL(32)*/
      DISTINCT 16,
               p_date_run,
               NULL,
               AC1.COD_CUST_ID,
               AC1.COD_ACCT_NO,
               DAT_NPA,
               NULL,
               NULL,
               NULL,
               NULL
        FROM CO_AC_ACCT_CRR_CODE AC1
       WHERE COD_CUST_ID IN
             (SELECT COD_CUST_ID
                FROM (SELECT /*+INDEX(CAC IN_CH_ACCT_CUST_XREF_1)*/
                      DISTINCT AC.COD_CUST_ID,
                               NVL(AC.DAT_NPA, DATE '1950-01-01')
                        FROM CO_AC_ACCT_CRR_CODE AC, CO_CH_ACCT_CUST_XREF CAC
                       WHERE NVL(AC.COD_CRR_MVMT_REASON, 'X') <> 'X'
                         AND NOT EXISTS
                       (SELECT 1
                                FROM CO_LN_ACCT_DTLS LAD
                               WHERE LAD.COD_ACCT_NO = AC.COD_ACCT_NO
                                 AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                                 AND FLG_MNT_STATUS = 'A')
                         AND NOT EXISTS
                       (SELECT 1
                                FROM CO_CH_ACCT_MAST CAM
                               WHERE CAM.COD_ACCT_NO = AC.COD_ACCT_NO
                                 AND CAM.COD_ACCT_STAT IN (1, 5)
                                 AND FLG_MNT_STATUS = 'A'
                                 AND COD_PROD IN
                                     (SELECT COD_PROD
                                        FROM CBSFCHOST.CH_PROD_MAST CPM
                                       WHERE CPM.COD_TYP_PROD = 'A'
                                         AND FLG_MNT_STATUS = 'A'))
                         AND AC.FLG_MNT_STATUS = 'A'
                         AND CAC.FLG_MNT_STATUS = 'A'
                         AND AC.COD_ACCT_NO = CAC.COD_ACCT_NO)
               GROUP BY COD_CUST_ID
              HAVING COUNT(1) > 1)
         AND NVL(COD_CRR_MVMT_REASON, 'X') <> 'X'
         AND AC1.FLG_MNT_STATUS = 'A'
       ORDER BY COD_CUST_ID;
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  BEGIN
    v_step := 'INSERT #17';
    INSERT /*+enable_parallel_dml parallel(32)*/
    INTO NPA_CONSIS_CHECK
      (CONSIS_NO,
       DATE_RUN,
       COD_ACCT_NO_PRI,
       COD_CUST_ID,
       COD_ACCT_NO_SEC,
       NPA_DATE,
       COD_CRR_CUST,
       COD_CRR_TO,
       FLG_ACCR_STATUS,
       DAT_COOLING_PRD_END)
      SELECT /*+PARALLEL(32)*/
      DISTINCT 17,
               p_date_run,
               NULL,
               COD_CUST_ID,
               AC1.COD_ACCT_NO,
               NULL,
               COD_CRR_CUST,
               NULL,
               NULL,
               NULL
        FROM CO_AC_ACCT_CRR_CODE AC1
       WHERE COD_CUST_ID IN
             (SELECT COD_CUST_ID
                FROM (SELECT /*+INDEX(CAC IN_CH_ACCT_CUST_XREF_1)*/
                      DISTINCT AC.COD_CUST_ID, AC.COD_CRR_CUST
                        FROM CO_AC_ACCT_CRR_CODE AC, CO_CH_ACCT_CUST_XREF CAC
                       WHERE NVL(AC.COD_CRR_MVMT_REASON, 'X') <> 'X'
                         AND CAC.FLG_MNT_STATUS = 'A'
                         AND AC.COD_ACCT_NO = CAC.COD_ACCT_NO
                         AND AC.FLG_MNT_STATUS = 'A'
                         AND NOT EXISTS
                       (SELECT 1
                                FROM CO_LN_ACCT_DTLS LAD
                               WHERE LAD.COD_ACCT_NO = AC.COD_ACCT_NO
                                 AND LAD.COD_ACCT_STAT IN (1, 5, 11)
                                 AND FLG_MNT_STATUS = 'A')
                         AND NOT EXISTS
                       (SELECT 1
                                FROM CO_CH_ACCT_MAST CAM
                               WHERE CAM.COD_ACCT_NO = AC.COD_ACCT_NO
                                 AND CAM.COD_ACCT_STAT IN (1, 5)
                                 AND FLG_MNT_STATUS = 'A'
                                 AND COD_PROD IN
                                     (SELECT COD_PROD
                                        FROM CBSFCHOST.CH_PROD_MAST CPM
                                       WHERE CPM.COD_TYP_PROD = 'A'
                                         AND FLG_MNT_STATUS = 'A')))
               GROUP BY COD_CUST_ID
              HAVING COUNT(1) > 1)
         AND NVL(COD_CRR_MVMT_REASON, 'X') <> 'X'
         AND AC1.FLG_MNT_STATUS = 'A'
       ORDER BY COD_CUST_ID;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
                              'npa_run_consis_checks failed at ' || v_step ||
                              ' : ' || SQLERRM);
  END;
  COMMIT;
  RETURN 1;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE_APPLICATION_ERROR(-20001,
                            'npa_run_consis_checks failed at ' || v_step ||
                            ' : ' || SQLERRM);
END;
/
