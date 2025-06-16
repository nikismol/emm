/* ****************************************************************************
 * Шаг 1. Создание временных таблиц и последовательностей,
 * предназначенных для тестирования работы SQL-скрипта
 * Примечание: таблицы, содержат только колонки, изменяемые SQL-скриптом.
 * ************************************************************************* */

CREATE TABLE схема.wo_header_test
   (
        event_perfno_i NUMBER(12),
        ata_chapter VARCHAR2(12),
        state VARCHAR2(2),
        ac_registr VARCHAR2(6),
        type VARCHAR2(2),
        est_groundtime NUMBER(12),
        event_type VARCHAR2(2),
        prio VARCHAR2(4),
        template_revisionno_i NUMBER(12)
   );

CREATE TABLE схема.wo_event_link_test
   (
        event_perfno_i NUMBER(12),
        mevt_headerno_i NUMBER(12),
        planable_status NUMBER(12),
        pending_status NUMBER(12),
        event_type VARCHAR2(8),
        event_key NUMBER(12),
        event_key_parent NUMBER(12),
        event_key_root NUMBER(12),
        event_status VARCHAR2(1),
        auto_report_back VARCHAR2(1),
        revision VARCHAR2(8),
        revision_status VARCHAR2(2),
        next_revision VARCHAR2(8),
        next_revision_status VARCHAR2(2),
        event_name VARCHAR2(100),
        event_display VARCHAR2(300),
        taskcard_type VARCHAR2(1)
   );

CREATE TABLE схема.wo_transfer_test
   (
        event_perfno_i NUMBER(12),
        event_transferno_i NUMBER(12),
        recno NUMBER(12),
        is_last_transfer VARCHAR2(1),
        transfer_type VARCHAR2(1),
        treq_dimension_groupno_i NUMBER(12),
        treq_interval_groupno_i NUMBER(12),
        first_later_logic VARCHAR2(1),
        transfer_time NUMBER(12),
        date_transfer NUMBER(12),
        transfer_hours NUMBER(12),
        transfer_cycles NUMBER(12),
        transfer_days NUMBER(12),
        doc_ref VARCHAR2(36),
        legno_i NUMBER(12),
        limit_type VARCHAR2(2),
        dc_use_case NUMBER(12),
        transfer_context VARCHAR2(2),
        init_option VARCHAR2(2),
        absolute_due_date NUMBER(12),
        absolute_due_time NUMBER(12),
        defer VARCHAR2(1)
   );

CREATE TABLE схема.wo_transfer_dimension_test
   (
        event_transferno_i NUMBER(12),
        wo_transfer_dimensionno_i NUMBER(12),
        counterno_i NUMBER(12),
        treq_intervalno_i NUMBER(12),
        togo_interval BINARY_DOUBLE,
        due_at BINARY_DOUBLE
   );

/* Временная таблица, необходимая для хранения промежуточных расчётов для таблиц wo_transfer_test и wo_transfer_dimension_test */
CREATE GLOBAL TEMPORARY TABLE схема.wo_transfer_temp (
    event_transferno_i NUMBER(12),
    wo_transfer_dimensionno_i NUMBER(12),
    counterno_i NUMBER(12),
    treq_intervalno_i NUMBER(12),
    togo_interval BINARY_DOUBLE,
    event_perfno_i NUMBER(12),
    recno NUMBER(12),
    is_last_transfer VARCHAR2(1),
    transfer_type VARCHAR2(1),
    treq_dimension_groupno_i NUMBER(12),
    treq_interval_groupno_i NUMBER(12),
    first_later_logic VARCHAR2(1),
    transfer_time NUMBER(12),
    date_transfer NUMBER(12),
    transfer_hours NUMBER(12),
    transfer_cycles NUMBER(12),
    transfer_days NUMBER(12),
    doc_ref VARCHAR2(36),
    legno_i NUMBER(12),
    limit_type VARCHAR2(2),
    dc_use_case NUMBER(12),
    transfer_context VARCHAR2(1),
    init_option VARCHAR2(2),
    absolute_due_date NUMBER(12),
    absolute_due_time NUMBER(12),
    defer VARCHAR2(1),
    code VARCHAR2(2),
    mfg_date NUMBER(12),
    due_at BINARY_DOUBLE,
    taskcard_type VARCHAR2(2),
    treq_interval_group_status NUMBER(12)
    ) ON COMMIT DELETE ROWS;


/* Шаг 1.2. Последовательности */
CREATE SEQUENCE схема.wo_header_test_seq
  START WITH 100000000
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;

CREATE SEQUENCE схема.wo_transfer_test_seq
  START WITH 100000000
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;

CREATE SEQUENCE схема.wo_transfer_dimension_test_seq
  START WITH 100000000
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;



/* ****************************************************************************
 * Шаг 2. Создание временных процедур, предназначенных для реализации
 * алгоритмов реализации задач ТЗ в виде SQL кода
 * ************************************************************************* */

/* Шаг 2.1. Подготовка тестовой среды
 * Входные параметры:
 * - p_docno_i - уникальный идентификатор документа
 * - p_aircraftno_i - уникальный идентификатор воздушного судна
 */

CREATE OR REPLACE PROCEDURE схема.prepare_test_area (
        p_docno_i IN VARCHAR2,
        p_aircraftno_i IN VARCHAR2
)
IS
BEGIN
    /* Очистка временных таблиц */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE схема.wo_header_test';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE схема.wo_event_link_test';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE схема.wo_transfer_test';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE схема.wo_transfer_dimension_test';

  /* Заполнение временной таблиц wo_event_link_test */
  INSERT INTO схема.wo_event_link_test
  SELECT
      wo_event_link.event_perfno_i,
      wo_event_link.MEVT_HEADERNO_I,
      wo_event_link.PLANABLE_STATUS,
      wo_event_link.PENDING_STATUS,
      wo_event_link.EVENT_TYPE,
      wo_event_link.EVENT_KEY,
      wo_event_link.EVENT_KEY_PARENT,
      wo_event_link.EVENT_KEY_ROOT,
      wo_event_link.EVENT_STATUS,
      wo_event_link.AUTO_REPORT_BACK,
      wo_event_link.REVISION,
      wo_event_link.REVISION_STATUS,
      wo_event_link.NEXT_REVISION,
      wo_event_link.NEXT_REVISION_STATUS,
      wo_event_link.EVENT_NAME,
      wo_event_link.EVENT_DISPLAY,
      wo_event_link.TASKCARD_TYPE
  FROM схема.doc_header
  LEFT JOIN схема.event_effectivity_link ON doc_header.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
  LEFT JOIN схема.event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
  LEFT JOIN схема.applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i AND applicability.ref_type = 'AC_INT'
  LEFT JOIN схема.aircraft ON applicability.ref_key = aircraft.aircraftno_i
  LEFT JOIN схема.mevt_header ON applicability.ref_key = mevt_header.ref_key
                                               AND applicability.ref_type = mevt_header.ref_type
                                               AND doc_header.docno_i = mevt_header.mevt_key
                                               AND mevt_header.mevt_type = 'DO'
  LEFT JOIN схема.mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                               AND mevt_effectivity.revision_type = 'DO'
  LEFT JOIN схема.wo_event_link ON wo_event_link.mevt_headerno_i = mevt_header.mevt_headerno_i
  WHERE doc_header.docno_i = p_docno_i
    AND applicability.ref_key = p_aircraftno_i
    AND wo_event_link.pending_status < 0;

/* Заполнение временной таблиц wo_header_test */
    INSERT INTO схема.wo_header_test
    SELECT
        r.event_perfno_i,
           r.ATA_CHAPTER,
           r.STATE,
           r.AC_REGISTR,
           r.TYPE,
           r.EST_GROUNDTIME,
           r.EVENT_TYPE,
           r.PRIO,
           r.template_revisionno_i
      FROM схема.wo_header r
      JOIN схема.wo_event_link_test l
        ON r.event_perfno_i = l.event_perfno_i;

    /* Заполнение временной таблиц wo_transfer_test */
    INSERT INTO схема.wo_transfer_test
     SELECT r.EVENT_PERFNO_I,
            r.EVENT_TRANSFERNO_I,
            r.RECNO,
            r.IS_LAST_TRANSFER,
            r.TRANSFER_TYPE,
            r.TREQ_DIMENSION_GROUPNO_I,
            r.TREQ_INTERVAL_GROUPNO_I,
            r.FIRST_LATER_LOGIC,
            r.TRANSFER_TIME,
            r.DATE_TRANSFER,
            r.TRANSFER_HOURS,
            r.TRANSFER_CYCLES,
            r.TRANSFER_DAYS,
            r.DOC_REF,
            r.LEGNO_I,
            r.LIMIT_TYPE,
            r.DC_USE_CASE,
            r.TRANSFER_CONTEXT,
            r.INIT_OPTION,
            r.ABSOLUTE_DUE_DATE,
            r.ABSOLUTE_DUE_TIME,
            r.DEFER
      FROM схема.wo_transfer r
      JOIN схема.wo_event_link_test l
        ON r.event_perfno_i = l.event_perfno_i;

    /* Заполнение временной таблиц wo_transfer_dimension_test */
    INSERT INTO схема.wo_transfer_dimension_test
     SELECT r.event_transferno_i,
            r.wo_transfer_dimensionno_i,
            r.counterno_i,
            r.treq_intervalno_i,
            r.togo_interval,
            r.due_at
      FROM схема.wo_transfer_dimension r
      JOIN схема.wo_transfer_test l
        ON r.event_transferno_i = l.event_transferno_i;

END;

/* Шаг 2.2. Создание новой записи в таблице wo_header для планируемого события ТО.
 * Входные параметры:
 * Входные параметры:
 * - p_docno_i - уникальный идентификатор документа
 * - p_aircraftno_i - уникальный идентификатор воздушного судна
 * Выходные параметры:
 * - p_event_perfno_i - уникальный идентификатор планирования события ТО
 */

CREATE OR REPLACE PROCEDURE схема.create_wo_header_row (
        p_docno_i IN NUMBER,
        p_aircraftno_i IN NUMBER,
        p_event_perfno_i OUT NUMBER
)
IS
    v_ata_chapter VARCHAR2(12);
    v_est_groundtime NUMBER(12);
    v_ac_registr VARCHAR2(12);
    v_prio VARCHAR2(4);
    v_template_revisionno_i NUMBER(12);

    CURSOR с_new_wo_header (cp_docno_i NUMBER, cp_aircraftno_i NUMBER) IS
    select
        doc_header.ata_chapter,
        wo_header.est_groundtime,
        COALESCE(wo_header.prio, 'M') prio,
        aircraft.ac_registr,
        mevt_effectivity.template_revisionno_i
    FROM схема.doc_header
    JOIN схема.event_effectivity_link ON doc_header.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
    JOIN схема.event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
    JOIN схема.applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i AND applicability.ref_type = 'AC_INT'
    JOIN схема.mevt_header ON applicability.ref_key = mevt_header.ref_key
                                                     AND mevt_header.ref_type='AC_INT'
                                                     AND doc_header.docno_i = mevt_header.mevt_key
                                                     AND mevt_header.mevt_type = 'DO'
    JOIN схема.mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                      AND mevt_effectivity.revision_type = 'DO'
    JOIN схема.AIRCRAFT ON MEVT_HEADER.REF_TYPE  = 'AC_INT' AND MEVT_HEADER.REF_KEY = AIRCRAFT.AIRCRAFTNO_I
    JOIN схема.wo_header ON wo_header.template_revisionno_i= mevt_effectivity.template_revisionno_i
                                          AND wo_header.TYPE = 'T'
    WHERE doc_header.docno_i = cp_docno_i
      AND applicability.ref_key = cp_aircraftno_i;

BEGIN
    /* Генерация уникального ключа event_perfno_i для планирования события ТО */
    p_event_perfno_i := wo_header_test_seq.NEXTVAL;

    /* Вычисляем данные для создания новой записи в таблице wo_header */
    OPEN с_new_wo_header(p_docno_i, p_aircraftno_i);
    FETCH с_new_wo_header INTO v_ata_chapter, v_est_groundtime, v_prio, v_ac_registr, v_template_revisionno_i;
    IF с_new_wo_header%NOTFOUND THEN
        CLOSE с_new_wo_header;
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: В соответствии с заданными параметрами, событие ТО не найдено');
    END IF;
    CLOSE с_new_wo_header;

    /* Записываем строку в БД */
    insert into схема.wo_header_test
    (
        event_perfno_i,
        ata_chapter,
        state,
        ac_registr,
        type,
        est_groundtime,
        event_type,
        prio,
        template_revisionno_i
    ) values (
        p_event_perfno_i,
        v_ata_chapter,
        'O',
        v_ac_registr,
        'PD',
        v_est_groundtime,
        'D',
        v_prio,
        v_template_revisionno_i
    );
    commit;
END;

/* Шаг 2.3. Создание новой записи в таблице wo_event_link
 *          для текущего планирования события ТО.
 * Входные параметры:
 * - p_event_perfno_i - уникальный идентификатор планирования события ТО
 * - p_docno_i - уникальный идентификатор документа
 * - p_aircraftno_i - уникальный идентификатор воздушного судна
 * - p_pending_status - идентификатор планирования.
 * Выходные параметры:
 * - p_init_future_events - флаг, указывающий следуюет ли создавать будущие события ТО, для данного планируемого события ТО.
 */

CREATE OR REPLACE PROCEDURE схема.create_wo_event_link_row (
        p_event_perfno_i IN NUMBER,
        p_docno_i IN NUMBER,
        p_aircraftno_i IN NUMBER,
        p_pending_status IN NUMBER,
        p_init_future_events OUT VARCHAR2
)
IS
    v_mevt_headerno_i NUMBER(12);
    v_event_key NUMBER(12);
    v_event_key_parent NUMBER(12);
    v_event_key_root NUMBER(12);
    v_event_status VARCHAR2(1);
    v_auto_report_back VARCHAR2(1);
    v_revision VARCHAR2(8);
    v_revision_status VARCHAR2(2);
    v_next_revision VARCHAR2(8);
    v_next_revision_status VARCHAR2(2);
    v_event_name VARCHAR2(100);
    v_event_display VARCHAR2(300);
    v_taskcard_type VARCHAR2(1);

    CURSOR с_new_wo_event_link (cp_docno_i NUMBER, cp_aircraftno_i IN NUMBER) IS
    SELECT
        mevt_header.mevt_headerno_i,
        mevt_effectivity.effectivity_linkno_i event_key,
        doc_header.docno_i event_key_parent,
        doc_header.docno_i event_key_root,
        case
            when p_pending_status > 0
            then 'R'
            when exists
              (select 1
                 from схема.wo_event_link wel
                WHERE wel.event_type = 'DOC_EFF'
                  AND wel.event_key_root = doc_header.docno_i
                  AND wel.mevt_headerno_i = mevt_header.mevt_headerno_i
                  AND wel.pending_status < 0)
              AND exists
              (select 1
                 from схема.wo_event_link wel
                WHERE wel.event_type = 'DOC_EFF'
                  AND wel.mevt_headerno_i = mevt_header.mevt_headerno_i
                  AND wel.pending_status > 0)
            THEN 'R' ELSE 'O' END event_status,
        treq_time_requirement.auto_reportback auto_report_back,
        doc_header.revision revision,
        NULL revision_status,
        NULL next_revision,
        NULL next_revision_status,
        doc_header.docno|| ' ('||doc_header.doc_type || ')' event_name,
        doc_header.docno|| '/' || doc_header.text event_display,
        NULL taskcard_type,
        CASE
            WHEN
                EXISTS
                (SELECT 1
                  FROM схема.treq_interval_group
                 WHERE treq_interval_group.timerequirementno_i= mevt_effectivity.timerequirementno_i
                   AND treq_interval_group.status is NOT NULL)
            THEN 'N'
            ELSE 'Y'
        END init_future_events
    FROM doc_header
    JOIN doc_revision ON doc_header.docno_i=doc_revision.docno_i
    JOIN event_effectivity_link ON doc_header.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
    JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
    JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i AND applicability.ref_type = 'AC_INT'
    JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                         AND applicability.ref_type = mevt_header.ref_type
                                         AND doc_header.docno_i = mevt_header.mevt_key
                                         AND mevt_header.mevt_type = 'DO'
    JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
    JOIN схема.AIRCRAFT ON MEVT_HEADER.REF_TYPE  = 'AC_INT'
                                         AND MEVT_HEADER.REF_KEY = AIRCRAFT.AIRCRAFTNO_I
    JOIN схема.TREQ_TIME_REQUIREMENT ON TREQ_TIME_REQUIREMENT.timerequirementno_i = mevt_effectivity.timerequirementno_i
    WHERE doc_header.docno_i = cp_docno_i
      AND applicability.ref_key= cp_aircraftno_i;

BEGIN

    /* Вычисляем данные для создания новой записи в таблице wo_event_link */
    OPEN с_new_wo_event_link(p_docno_i, p_aircraftno_i);
    FETCH с_new_wo_event_link INTO
        v_mevt_headerno_i,
        v_event_key,
        v_event_key_parent,
        v_event_key_root,
        v_event_status,
        v_auto_report_back,
        v_revision,
        v_revision_status,
        v_next_revision,
        v_next_revision_status,
        v_event_name,
        v_event_display,
        v_taskcard_type,
        p_init_future_events;
    IF с_new_wo_event_link%NOTFOUND THEN
        CLOSE с_new_wo_event_link;
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: В соответствии с заданными параметрами, событие ТО не найдено');
    END IF;
    CLOSE с_new_wo_event_link;

    /* Записываем строку в БД */
    insert into схема.wo_event_link_test
    (
        event_perfno_i,
        mevt_headerno_i,
        planable_status,
        pending_status,
        event_type,
        event_key,
        event_key_parent,
        event_key_root,
        event_status,
        auto_report_back,
        revision,
        revision_status,
        next_revision,
        next_revision_status,
        event_name,
        event_display,
        taskcard_type
    ) values (
        p_event_perfno_i,
        v_mevt_headerno_i,
        0,
        p_pending_status,
        'DOC_EFF',
        v_event_key,
        v_event_key_parent,
        v_event_key_root,
        v_event_status,
        v_auto_report_back,
        v_revision,
        v_revision_status,
        v_next_revision,
        v_next_revision_status,
        v_event_name,
        v_event_display,
        v_taskcard_type
    );
    commit;

END;

/* Шаг 2.4. Создание фуникции расчета даты "baseline" от которой будет расчитываться Threshold
 * (необходимо, если размерность точки отсчета Baseline и размерность порога отличаются).
 * Функция вызывается только внутри create_wo_transfer_and_wo_transfer_dim_row
 * Входные параметры:
 * - p_based_on - тип точки отсчета Baseline
 * - p_mfg_date - дата производства ВС
 * - p_del_date - дата поставки ВС
 * - p_dimension - размерность точки отсчета Baseline
 * - p_amount - заданное количественное зачение точки отсчета Baseline
 * - p_first_flight_date - дата первого полета ВС
 * - p_aircraftno_i - уникальный номер ВС в системе
 * Результат выполнения функиии:
 * - расчитанная дата Baseline
 */
CREATE OR REPLACE FUNCTION схема.get_baseline_date (
    p_based_on          IN VARCHAR, -- treq_baseline.based_on
    p_mfg_date          IN NUMBER,  -- aircraft.mfg_date
    p_del_date          IN NUMBER,  -- aircraft.del_date
    p_dimension         IN VARCHAR, -- treq_baseline_threshold.dimension
    p_amount            IN NUMBER,  -- treq_baseline_threshold.amount
    p_first_flight_date IN NUMBER,  -- aircraft.first_flight_date
    p_aircraftno_i      IN NUMBER   -- aircraft.aircraftno_i
    )
     RETURN NUMBER
  as
  baseline_date NUMBER;
  BEGIN

  SELECT DECODE(p_based_on,
                'M', p_mfg_date,
                'D', NVL(NULLIF(p_del_date, 0), p_mfg_date),
                'V', CASE WHEN p_dimension = 'D'  THEN p_amount
                          WHEN p_dimension IN ('H', 'C')
                          THEN (
                                SELECT MAX(counter_value.readout_date)
                                FROM схема.counter
                                JOIN схема.counter_template ON counter.counter_templateno_i = counter_template.counter_templateno_i
                                JOIN схема.counter_definition ON counter_template.counter_defno_i = counter_definition.counter_defno_i
                                LEFT JOIN схема.counter_value ON counter_value.counterno_i=counter.counterno_i AND NVL(counter_value.is_minor, 'N') != 'Y'
                                WHERE 1=1
                                      AND counter.ref_type = 'AC_INT'
                                      AND counter_definition.code = p_dimension
                                      AND counter.ref_key = p_aircraftno_i
                                      AND counter_value.life_value <= p_amount * (DECODE(p_dimension, 'H', 60, 1))
                               )
                          WHEN p_dimension IN ('AH', 'AC')
                          THEN -- получаю дату, в которую счетчик AH/AC на ВС был равен заданному значению
                               (SELECT MAX (counter_value_m_h.readout_date) as max_readout_date
                                FROM (
                                      SELECT counter.counterno_i,
                                             counter.master_counterno_i,
                                             nvl(counter_value.life_value, 0) as life_value,
                                             nvl(counter_value_on.life_value, 0) as life_value_on
                                      FROM схема.counter
                                      JOIN схема.counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
                                      JOIN схема.counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
                                      LEFT JOIN схема.counter_value ON counter_value.counterno_i=counter.counterno_i AND NVL(counter_value.is_minor, 'N') != 'Y'
                                      LEFT JOIN схема.counter_value counter_value_on on counter_value.on_counter_valueno_i=counter_value_on.counter_valueno_i AND NVL(counter_value_on.is_minor, 'N') != 'Y'
                                      WHERE 1=1
                                             AND counter.ref_key = p_aircraftno_i
                                             AND counter.ref_type = 'AC_INT'
                                             AND counter_definition.code = p_dimension
                                             AND counter_value.on_counter_valueno_i IS NOT NULL
                                      ) counter_h
                                 LEFT JOIN схема.counter counter_m_h ON counter_h.master_counterno_i=counter_m_h.counterno_i
                                 LEFT JOIN схема.counter_value counter_value_m_h ON counter_value_m_h.counterno_i=counter_m_h.counterno_i
                                                                                              AND NVL(counter_value_m_h.is_minor, 'N') != 'Y'
                                 WHERE 1=1
                                       AND TO_NUMBER(counter_h.life_value + counter_value_m_h.life_value - counter_h.life_value_on) <= p_amount * (DECODE(p_dimension, 'H', 60, 1))
                                )
                          ELSE 0 END,
                'F', NVL(NULLIF(p_first_flight_date, 0), p_mfg_date)
                , p_mfg_date
               ) INTO baseline_date
  FROM DUAL;

  RETURN baseline_date;
  END;

/* Шаг 2.4.1. Создание новых записей в таблицах wo_transfer и wo_transfer_dimension для текущего планирования события ТО.
 * Входные параметры:
 * - p_event_perfno_i - уникальный идентификатор планирования события ТО
 * - p_curr_plan_event_perfno_i - уникальный идентификатор текущего планирования события ТО
 * - p_pending_status - идентификатор планирования.
 * - p_init_option - основа инициализации
 *   Возможные значения: Unknown/Auto Calculation (EMPTY_STRING or NULL), Last Performed (L), Never Performed (P)
 * - p_recno - порядковый номер версии wo_transfer* для заданного планирования события ТО
 * - p_transfer_context - триггер инициализации.
 *   Возможные значения: Unknown (EMPTY_STRING or NULL), Initialistaion (I), Reporting Back (R), Sync. Time Requirement (S),
 *                       MP Activation (A), Event Next Due Changed (C), Event Extension (E), Component Receiving (R)
 * - p_ground_time - При Report Back может задаваться "ground time" в днях (используется для TR с типом Next Due Calculation Strategy = Groundtime Extension).
 *   В БД заданный "ground time" не сохраняется, поэтому due_date расчитывается в момент выполнения Report Back c учетом внесенного "ground time".
 *   При этом, если сделать deassign/assign события, то новый DL не будет учитывать ground time, т.к. он нигде не сохранен.
 *   Сохранение введенного "ground time" и его использование для расчета DL можно рассматривать как модификацию и улучшение функционала системы.
 *
 * Выходные параметры:
 * - p_init_future_events - флаг, указывающий следуюет ли создавать будущие события ТО, для данного планируемого события ТО.
 * - p_timerequirementno_i - уникальный идентификатор time-requirement-а, применимого к данному событию ТО.
 */
CREATE OR REPLACE PROCEDURE схема.create_wo_transfer_and_wo_transfer_dim_row (
    p_event_perfno_i IN NUMBER,
    p_curr_plan_event_perfno_i IN NUMBER,
    p_pending_status IN NUMBER,
    p_init_option IN VARCHAR2,
    p_recno IN NUMBER,
    p_transfer_context IN VARCHAR2,
    p_ground_time IN NUMBER
)
IS
    v_event_transferno_i number(12);
BEGIN

/* Генерация уникального ключа event_transferno_i */
v_event_transferno_i := wo_transfer_test_seq.NEXTVAL;

/* Заполнение временной таблицы c расчитанными данными */
INSERT INTO wo_transfer_temp (
           event_transferno_i,
           wo_transfer_dimensionno_i,
           counterno_i,
           treq_intervalno_i,
           togo_interval,
           event_perfno_i,
           recno,
           is_last_transfer,
           transfer_type,
           treq_dimension_groupno_i,
           treq_interval_groupno_i,
           first_later_logic,
           transfer_time,
           date_transfer,
           transfer_hours,
           transfer_cycles,
           transfer_days,
           doc_ref,
           legno_i,
           limit_type,
           dc_use_case,
           transfer_context,
           init_option,
           absolute_due_date,
           absolute_due_time,
           defer,
           code,
           mfg_date,
           due_at,
           taskcard_type,
           treq_interval_group_status)

    WITH wo_transfer_data as
    (
    SELECT v_event_transferno_i as event_transferno_i,
           (SELECT counter.counterno_i
            FROM схема.counter
            JOIN схема.counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
            JOIN схема.counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
                                       AND counter_definition.counter_defno_i = treq_interval.counter_defno_i --определяю размерность счетчика
            WHERE 1=1
                  AND counter.ref_type = 'AC_INT'
                  AND counter.ref_key = aircraft.aircraftno_i
           ) as counterno_i,
           treq_interval.intervalno_i as treq_intervalno_i,
           null as togo_interval,
           p_event_perfno_i event_perfno_i,
           p_recno as recno,
           'Y' as is_last_transfer,
           'T' as transfer_type,
           treq_dimension_group.dimension_groupno_i as treq_dimension_groupno_i,
           treq_interval_group.interval_groupno_i as treq_interval_groupno_i,
           CASE WHEN wo_event_link_test.taskcard_type = 'O' THEN NULL
                WHEN treq_interval_group.first_later IN ('F','L') THEN treq_interval_group.first_later
                ELSE NULL
           END as first_later_logic,
           (SYSDATE - TRUNC(SYSDATE)) * 24 * 60 as transfer_time,
           FLOOR(SYSDATE - TO_DATE('1971-12-31 00:00', 'YYYY-MM-DD HH24:MI')) as date_transfer,
           NULL as transfer_hours,
           NULL as transfer_cycles,
           NULL as transfer_days,
           NULL as doc_ref,
           NULL as legno_i,
           NULL as limit_type,
           NULL as dc_use_case,
           p_transfer_context as transfer_context,
           p_init_option as init_option,
           NULL as absolute_due_date,
           0 as absolute_due_time,
           NULL as defer,

            CASE -- Next Due Calculation strategy = Standart Calculation
                 WHEN treq_dimension_group.next_due_rule_id = -1 OR treq_dimension_group.next_due_rule_id = 0 AND (SELECT amount_int from схема.parameters where parameter = 898) = 0
                 THEN CASE WHEN last_due_at.last_due_value > 0 THEN LEAST(last_perf.last_perf_value, last_due_at.last_due_value) ELSE last_perf.last_perf_value END --если просрочен dl - берем last due at

                 -- Next Due Calculation strategy = Last Performed
                 WHEN treq_dimension_group.next_due_rule_id = 3 OR treq_dimension_group.next_due_rule_id = 0 AND (SELECT amount_int from схема.parameters where parameter = 898) = 3
                 THEN last_perf.last_perf_value

                 -- Next Due Calculation strategy = Groundtime Extension
                 WHEN treq_dimension_group.next_due_rule_id = 1 OR treq_dimension_group.next_due_rule_id = 0 AND (SELECT amount_int from схема.parameters where parameter = 898) = 1
                 THEN CASE WHEN counter_definition.code = 'D'
                           THEN CASE WHEN last_perf.last_perf_value <= last_due_at.last_due_value + p_ground_time*24*60
                                     THEN last_perf.last_perf_value
                                     ELSE last_due_at.last_due_value + p_ground_time*24*60
                                END
                           ELSE CASE WHEN last_due_at.last_due_value > 0 THEN LEAST(last_perf.last_perf_value, last_due_at.last_due_value) ELSE last_perf.last_perf_value END
                      END

                 -- Next Due Calculation strategy = Tolerance Extension
                 WHEN treq_dimension_group.next_due_rule_id = 2 OR treq_dimension_group.next_due_rule_id = 0 AND (SELECT amount_int from схема.parameters where parameter = 898) = 2
                 THEN CASE WHEN last_perf.last_perf_value <= CASE WHEN counter_definition.code = 'D'
                                                                  THEN CASE WHEN treq_interval.unit = 'H' THEN last_due_at.last_due_value + treq_interval.pos_tolerance*60
                                                                            WHEN treq_interval.unit = 'D' THEN last_due_at.last_due_value + treq_interval.pos_tolerance*24*60
                                                                            WHEN treq_interval.unit = 'MT' THEN (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                                                                                            + aircraft.mfg_date
                                                                                                                            + TRUNC(last_due_at.last_due_value/24/60)
                                                                                                                            , treq_interval.pos_tolerance
                                                                                                                 )
                                                                                                                 - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + aircraft.mfg_date)
                                                                                                                ) * 24 * 60
                                                                            WHEN treq_interval.unit = 'YR' THEN (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                                                                                            + aircraft.mfg_date
                                                                                                                            + TRUNC(last_due_at.last_due_value/24/60)
                                                                                                                            , treq_interval.pos_tolerance * 12
                                                                                                                           )
                                                                                                                 - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + aircraft.mfg_date)
                                                                                                                ) * 24 * 60
                                                                            ELSE 0
                                                                       END
                                                                  WHEN counter_definition.code IN ('H', 'C', 'AH', 'AC')
                                                                  THEN last_due_at.last_due_value + treq_interval.pos_tolerance * DECODE (counter_definition.code, 'H', 60, 'AH', 60, 1)
                                                                  ELSE 0
                                                             END
                           THEN last_perf.last_perf_value
                           ELSE last_due_at.last_due_value + CASE WHEN counter_definition.code = 'D'
                                                                  THEN CASE WHEN treq_interval.unit = 'H' THEN last_due_at.last_due_value + treq_interval.pos_tolerance*60
                                                                            WHEN treq_interval.unit = 'D' THEN last_due_at.last_due_value + treq_interval.pos_tolerance*24*60
                                                                            WHEN treq_interval.unit = 'MT' THEN (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                                                                                            + aircraft.mfg_date
                                                                                                                            + TRUNC(last_due_at.last_due_value/24/60)
                                                                                                                            , treq_interval.pos_tolerance
                                                                                                                 )
                                                                                                                 - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + aircraft.mfg_date)
                                                                                                                ) * 24 * 60
                                                                            WHEN treq_interval.unit = 'YR' THEN (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                                                                                            + aircraft.mfg_date
                                                                                                                            + TRUNC(last_due_at.last_due_value/24/60)
                                                                                                                            , treq_interval.pos_tolerance * 12
                                                                                                                           )
                                                                                                                 - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + aircraft.mfg_date)
                                                                                                                ) * 24 * 60
                                                                            ELSE 0
                                                                       END
                                                                  WHEN counter_definition.code IN ('H', 'C', 'AH', 'AC')
                                                                  THEN last_due_at.last_due_value + treq_interval.pos_tolerance * DECODE (counter_definition.code, 'H', 60, 'AH', 60, 1)
                                                                  ELSE 0
                                                             END
                       END

                 -- Next Due Calculation strategy = Flexible Calculation
                 WHEN treq_dimension_group.next_due_rule_id = 100 OR treq_dimension_group.next_due_rule_id = 0 AND (SELECT amount_int from схема.parameters where parameter = 898) = 100
                 THEN CASE WHEN last_perf.last_perf_value < CASE WHEN counter_definition.code = 'D'
                                                                 THEN CASE WHEN treq_interval.unit = 'H' THEN last_due_at.last_due_value - treq_interval.neg_tolerance*60
                                                                           WHEN treq_interval.unit = 'D' THEN last_due_at.last_due_value - treq_interval.neg_tolerance*24*60
                                                                           WHEN treq_interval.unit = 'MT' THEN (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                                                                                           + aircraft.mfg_date
                                                                                                                           + TRUNC(last_due_at.last_due_value/24/60)
                                                                                                                           , - treq_interval.neg_tolerance
                                                                                                                )
                                                                                                                - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + aircraft.mfg_date)
                                                                                                               ) * 24 * 60
                                                                           WHEN treq_interval.unit = 'YR' THEN (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                                                                                           + aircraft.mfg_date
                                                                                                                           + TRUNC(last_due_at.last_due_value/24/60)
                                                                                                                           , - treq_interval.neg_tolerance * 12
                                                                                                                          )
                                                                                                                - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + aircraft.mfg_date)
                                                                                                               ) * 24 * 60
                                                                           ELSE 0
                                                                      END
                                                                 WHEN counter_definition.code IN ('H', 'C', 'AH', 'AC')
                                                                 THEN last_due_at.last_due_value - treq_interval.neg_tolerance * DECODE (counter_definition.code, 'H', 60, 'AH', 60, 1)
                                                                 ELSE 0
                                                             END
                           THEN last_perf.last_perf_value
                           ELSE last_due_at.last_due_value
                       END
            END as last_perf_value,

            counter_definition.code,
            CASE WHEN counter_definition.code = 'D' THEN NVL(treq_interval.accuracy, 'D') ELSE treq_interval.accuracy END as accuracy,
            treq_interval.unit,
            treq_baseline.based_on,
            treq_baseline_threshold.dimension,
            treq_baseline_threshold.amount,
            aircraft.del_date,
            aircraft.mfg_date,
            aircraft.aircraftno_i,
            treq_interval.amount_interval,
            treq_interval.dimension_type,
            treq_interval.due_at * CASE WHEN counter_definition.code IN ('H', 'AH') THEN  60 ELSE 1 END as treq_interval_due_at,

            CASE WHEN counter_definition.code = 'D'
                 THEN (SELECT схема.get_baseline_date (treq_baseline.based_on,
                                                                 aircraft.mfg_date,
                                                                 aircraft.del_date,
                                                                 treq_baseline_threshold.dimension,
                                                                 treq_baseline_threshold.amount,
                                                                 aircraft.first_flight_date,
                                                                 aircraft.aircraftno_i
                                                                )
                       FROM DUAL)
                 WHEN counter_definition.code IN ('C', 'H')
                 THEN NVL(
                          (SELECT life_value
                           FROM (SELECT DISTINCT
                                        cv.life_value,
                                        max (cv.readout_date) over (partition by cv.counterno_i) as max_readout_date,
                                        cv.readout_date as readout_date,
                                        min (cv.readout_time) over (partition by cv.readout_date) as min_readout_time,
                                        cv.readout_time
                                 FROM схема.counter c
                                 JOIN схема.counter_template ct ON c.counter_templateno_i = ct.counter_templateno_i
                                 JOIN схема.counter_definition cd ON ct.counter_defno_i = cd.counter_defno_i
                                 JOIN схема.counter_value cv ON cv.counterno_i=c.counterno_i AND NVL(cv.is_minor, 'N') != 'Y'
                                 WHERE c.ref_type = 'AC_INT'
                                       AND cd.code = counter_definition.code
                                       AND c.ref_key = aircraft.aircraftno_i
                                       AND cv.readout_date < (SELECT схема.get_baseline_date (treq_baseline.based_on,
                                                                                                        aircraft.mfg_date,
                                                                                                        aircraft.del_date,
                                                                                                        treq_baseline_threshold.dimension,
                                                                                                        treq_baseline_threshold.amount,
                                                                                                        aircraft.first_flight_date,
                                                                                                        aircraft.aircraftno_i
                                                                                                       )
                                                               FROM DUAL)
                                 )
                           WHERE 1=1
                                 AND max_readout_date = readout_date
                                 AND min_readout_time = readout_time
                          )
                          , 0
                         )
                 WHEN counter_definition.code IN ('AC', 'AH')
                 THEN NVL(
                          (SELECT apu_hist_life_value
                           FROM (
                                 SELECT DISTINCT
                                        TO_NUMBER(counter_h.life_value + counter_value_m_h.life_value - counter_h.life_value_on) as apu_hist_life_value,
                                        MAX (counter_value_m_h.readout_date) over (partition by counter_value_m_h.counterno_i) as max_readout_date,
                                        counter_value_m_h.readout_date as readout_date,
                                        MAX (counter_value_m_h.readout_time) over (partition by counter_value_m_h.readout_date) as max_readout_time,
                                        counter_value_m_h.readout_time
                                 FROM (
                                       SELECT --c.ref_key,
                                              c.counterno_i,
                                              --counter.is_unknown,
                                              c.master_counterno_i,
                                              --counter_value.on_counter_valueno_i,
                                              --counter_definition.code,
                                              nvl(cv.life_value, 0) as life_value,
                                              nvl(counter_value_on.life_value, 0) as life_value_on,
                                              max (cv.readout_date*86400000+cv.readout_time) over (partition by cv.counterno_i) as max_readout,
                                              cv.readout_date*86400000+cv.readout_time as readout
                                       FROM схема.counter c
                                       JOIN схема.counter_template ct ON c.counter_templateno_i=ct.counter_templateno_i
                                       JOIN схема.counter_definition cd ON ct.counter_defno_i=cd.counter_defno_i
                                       LEFT JOIN схема.counter_value cv ON cv.counterno_i=c.counterno_i
                                       LEFT JOIN схема.counter_value counter_value_on on cv.on_counter_valueno_i=counter_value_on.counter_valueno_i
                                       WHERE 1=1
                                             AND c.ref_key = aircraft.aircraftno_i
                                             AND c.ref_type = 'AC_INT'
                                             AND cd.code = counter_definition.code
                                             AND cv.on_counter_valueno_i IS NOT NULL
                                             AND cv.readout_date < (SELECT схема.get_baseline_date (treq_baseline.based_on,
                                                                                                                         aircraft.mfg_date,
                                                                                                                         aircraft.del_date,
                                                                                                                         treq_baseline_threshold.dimension,
                                                                                                                         treq_baseline_threshold.amount,
                                                                                                                         aircraft.first_flight_date,
                                                                                                                         aircraft.aircraftno_i
                                                                                                                        )
                                                                                                    FROM DUAL)
                                      ) counter_h
                                 LEFT JOIN схема.counter counter_m_h ON counter_h.master_counterno_i=counter_m_h.counterno_i
                                 LEFT JOIN схема.counter_value counter_value_m_h ON counter_value_m_h.counterno_i=counter_m_h.counterno_i
                                                                              AND NVL(counter_value_m_h.is_minor, 'N') != 'Y'

                                 WHERE 1=1
                                       AND counter_h.readout=counter_h.max_readout
                                       AND counter_value_m_h.readout_date < (SELECT схема.get_baseline_date (treq_baseline.based_on,
                                                                                                                       aircraft.mfg_date,
                                                                                                                       aircraft.del_date,
                                                                                                                       treq_baseline_threshold.dimension,
                                                                                                                       treq_baseline_threshold.amount,
                                                                                                                       aircraft.first_flight_date,
                                                                                                                       aircraft.aircraftno_i
                                                                                                                      )
                                                                                                                    FROM DUAL)
                                )
                           WHERE 1=1
                                 AND max_readout_date = readout_date
                                 AND max_readout_time = readout_time)
                          , 0
                         )
                 ELSE 0
            END as baseline_value,
            CASE WHEN counter_definition.code IN ('H', 'C') AND treq_baseline_threshold.amount > 0
                 THEN (SELECT life_value
                       FROM (
                             SELECT DISTINCT
                                    cv.life_value,
                                    max (cv.readout_date) over (partition by cv.counterno_i) as max_readout_date,
                                    cv.readout_date as readout_date,
                                    min (cv.readout_time) over (partition by cv.readout_date) as min_readout_time,
                                    cv.readout_time
                             FROM схема.counter c
                             LEFT JOIN схема.counter_template ct ON c.counter_templateno_i = ct.counter_templateno_i
                             LEFT JOIN схема.counter_definition cd ON ct.counter_defno_i = cd.counter_defno_i
                             LEFT JOIN схема.counter_value cv ON cv.counterno_i=c.counterno_i AND NVL(cv.is_minor, 'N') != 'Y'
                             WHERE c.ref_type = 'AC_INT'
                                   AND cd.code = counter_definition.code
                                   AND c.ref_key = aircraft.aircraftno_i
                                   AND cv.readout_date < treq_baseline_threshold.amount
                                   AND p_pending_status = 0
                            )
                       WHERE 1=1
                             AND max_readout_date = readout_date
                             AND min_readout_time = readout_time
                      )
                WHEN counter_definition.code IN ('AH', 'AC') AND treq_baseline_threshold.amount > 0
                THEN (
                      SELECT apu_hist_life_value
                      FROM (
                            SELECT DISTINCT
                                   TO_NUMBER(counter_h.life_value + counter_value_m_h.life_value - counter_h.life_value_on) as apu_hist_life_value,
                                   MAX (counter_value_m_h.readout_date) over (partition by counter_value_m_h.counterno_i) as max_readout_date,
                                   counter_value_m_h.readout_date as readout_date,
                                   MAX (counter_value_m_h.readout_time) over (partition by counter_value_m_h.readout_date) as max_readout_time,
                                   counter_value_m_h.readout_time
                            FROM (
                                  SELECT --c.ref_key,
                                         c.counterno_i,
                                         --counter.is_unknown,
                                         c.master_counterno_i,
                                         --counter_value.on_counter_valueno_i,
                                         --counter_definition.code,
                                         nvl(cv.life_value, 0) as life_value,
                                         nvl(counter_value_on.life_value, 0) as life_value_on,
                                         max (cv.readout_date*86400000+cv.readout_time) over (partition by cv.counterno_i) as max_readout,
                                         cv.readout_date*86400000+cv.readout_time as readout
                                  FROM схема.counter c
                                  JOIN схема.counter_template ct ON c.counter_templateno_i=ct.counter_templateno_i
                                  JOIN схема.counter_definition cd ON ct.counter_defno_i=cd.counter_defno_i
                                  LEFT JOIN схема.counter_value cv ON cv.counterno_i=c.counterno_i
                                  LEFT JOIN схема.counter_value counter_value_on on cv.on_counter_valueno_i=counter_value_on.counter_valueno_i
                                  WHERE 1=1
                                        AND c.ref_key = aircraft.aircraftno_i
                                        AND c.ref_type = 'AC_INT'
                                        AND cd.code = counter_definition.code
                                        AND cv.on_counter_valueno_i IS NOT NULL
                                        AND cv.readout_date < treq_baseline_threshold.amount
                                 ) counter_h
                            LEFT JOIN схема.counter counter_m_h ON counter_h.master_counterno_i=counter_m_h.counterno_i
                            LEFT JOIN схема.counter_value counter_value_m_h ON counter_value_m_h.counterno_i=counter_m_h.counterno_i
                                                                         AND NVL(counter_value_m_h.is_minor, 'N') != 'Y'

                            WHERE 1=1
                                  AND counter_h.readout=counter_h.max_readout
                                  AND counter_value_m_h.readout_date < treq_baseline_threshold.amount
                           )
                      WHERE 1=1
                            AND max_readout_date = readout_date
                            AND max_readout_time = readout_time
                     )
                ELSE 0
            END as counter_hist_value,
            wo_event_link_test.taskcard_type,
            treq_interval_group.status as treq_interval_group_status
    FROM схема.wo_event_link_test
    JOIN схема.mevt_header ON wo_event_link_test.mevt_headerno_i = mevt_header.mevt_headerno_i
                        AND mevt_header.mevt_type = 'DO'
    JOIN схема.mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i AND mevt_effectivity.applicable_status = 'Y'
    JOIN схема.aircraft ON mevt_header.ref_key = aircraft.aircraftno_i AND mevt_header.ref_type = 'AC_INT'
    JOIN схема.wo_header_test ON wo_event_link_test.event_perfno_i = wo_header_test.event_perfno_i
    JOIN doc_header ON mevt_header.mevt_key=doc_header.docno_i
    JOIN event_effectivity_link ON doc_header.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
--     JOIN схема.requirement_header ON requirement_header.event_key = doc_header.docno_i AND requirement_header.event_type = 'DO'
    JOIN схема.treq_time_requirement ON mevt_effectivity.timerequirementno_i = treq_time_requirement.timerequirementno_i
                                  AND treq_time_requirement.event_type IN ('REQH', 'EFL')
                                  AND treq_time_requirement.status = 0
                                  AND treq_time_requirement.type = 'OP'
    JOIN схема.treq_interval_group ON treq_time_requirement.timerequirementno_i=treq_interval_group.timerequirementno_i
                                AND ( p_init_option NOT IN ('E','D','R','S','T','O','C','G','N','L') OR p_init_option IS NULL) --  "Только Autocalculate/NeverPerformed инициализация"
                                AND treq_interval_group.threshold = CASE --1. "Если были выполнения TC - использовать интервал"
                                                                         --   "Если не задан порог - использовать интервал"
                                                                         WHEN (SELECT 1 FROM DUAL WHERE EXISTS (
                                                                                             SELECT 1
                                                                                             FROM схема.wo_event_link_test wel_hist
                                                                                             JOIN схема.wo_header_test woh_hist ON wel_hist.event_perfno_i = woh_hist.event_perfno_i
                                                                                             WHERE 1=1
                                                                                                   AND wel_hist.event_type = 'DOC_EFF'
                                                                                                   AND wel_hist.event_key_root = wo_event_link_test.event_key_root
                                                                                                   AND woh_hist.ac_registr = wo_header_test.ac_registr
                                                                                                   AND wel_hist.pending_status < 0
                                                                                            )
                                                                              ) = 1
                                                                              AND p_init_option NOT IN ('P')
                                                                              OR
                                                                              (SELECT 1 FROM DUAL WHERE NOT EXISTS (
                                                                                            SELECT 1
                                                                                            FROM схема.treq_interval_group tig
                                                                                            WHERE tig.timerequirementno_i = treq_time_requirement.timerequirementno_i
                                                                                                  AND tig.threshold = 'Y'
                                                                                           )

                                                                              ) = 1
                                                                              OR
                                                                              p_pending_status > 0
                                                                         THEN 'N'
                                                                         --2. "Если не было выполнений TC - использовать порог"
                                                                         WHEN (SELECT 1 FROM DUAL WHERE NOT EXISTS (
                                                                                        SELECT 1
                                                                                        FROM схема.wo_event_link_test wel_hist
                                                                                        JOIN схема.wo_header_test woh_hist ON wel_hist.event_perfno_i = woh_hist.event_perfno_i
                                                                                        WHERE 1=1
                                                                                              AND wel_hist.event_type = 'DOC_EFF'
                                                                                              AND wel_hist.event_key_root = wo_event_link_test.event_key_root
                                                                                              AND woh_hist.ac_registr = wo_header_test.ac_registr
                                                                                              AND wel_hist.pending_status < 0
                                                                                       )
                                                                              ) = 1
                                                                              AND p_pending_status = 0
                                                                              OR
                                                                              p_init_option IN ('P')
                                                                              AND p_pending_status = 0
                                                                         THEN 'Y'
                                                                         ELSE 'N'
                                                                    END


    LEFT JOIN схема.treq_dimension_group ON treq_interval_group.interval_groupno_i=treq_dimension_group.interval_groupno_i
    LEFT JOIN схема.treq_interval ON treq_interval.dimension_groupno_i=treq_dimension_group.dimension_groupno_i
    LEFT JOIN схема.counter_definition ON counter_definition.counter_defno_i = treq_interval.counter_defno_i

    LEFT JOIN схема.treq_baseline ON treq_interval.intervalno_i = treq_baseline.intervalno_i
    LEFT JOIN схема.treq_baseline_threshold ON treq_baseline.baselineno_i = treq_baseline_threshold.baselineno_i
    LEFT JOIN (
           SELECT MIN (wo_event_link_test.pending_status) OVER (PARTITION BY wo_event_link_test.event_key_root, wo_header_test.ac_registr) as min_pending_status,
                  wo_event_link_test.pending_status,
                  wo_transfer_dimension_test.due_at as last_perf_value,
                  counter_definition.code
           FROM схема.wo_event_link_test
           JOIN схема.wo_header_test ON wo_event_link_test.event_perfno_i = wo_header_test.event_perfno_i
           JOIN схема.wo_transfer_test on wo_transfer_test.event_perfno_i = wo_event_link_test.event_perfno_i and wo_transfer_test.is_last_transfer='Y' AND wo_transfer_test.transfer_type IN ('R', 'C')
           JOIN схема.wo_transfer_dimension_test on wo_transfer_test.event_transferno_i = wo_transfer_dimension_test.event_transferno_i
           JOIN схема.counter ON wo_transfer_dimension_test.counterno_i=counter.counterno_i
           JOIN схема.counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
           JOIN схема.counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
           WHERE 1=1
                 AND wo_event_link_test.event_key_root = (SELECT e.event_key_root FROM схема.wo_event_link_test e WHERE e.event_perfno_i = p_curr_plan_event_perfno_i)
                 AND wo_header_test.ac_registr = (SELECT w.ac_registr FROM схема.wo_header_test w WHERE w.event_perfno_i = p_curr_plan_event_perfno_i)
                 AND wo_event_link_test.pending_status < 0
                 AND wo_event_link_test.event_type = 'DOC_EFF'
                 AND p_pending_status = 0
          ) last_perf ON last_perf.min_pending_status = last_perf.pending_status
                         AND last_perf.code = counter_definition.code
                         AND treq_interval.dimension_type = 'I'


    LEFT JOIN (
           SELECT MIN (wo_event_link_test.pending_status) OVER (PARTITION BY wo_event_link_test.event_key_root, wo_header_test.ac_registr) as min_pending_status,
                  MAX (wo_event_link_test.pending_status) OVER (PARTITION BY wo_event_link_test.event_key_root, wo_header_test.ac_registr) as max_pending_status,
                  wo_event_link_test.pending_status,
                  wo_transfer_dimension_test.due_at as last_due_value,
                  counter_definition.code
           FROM схема.wo_event_link_test
           JOIN схема.wo_header_test ON wo_event_link_test.event_perfno_i = wo_header_test.event_perfno_i
           JOIN схема.wo_transfer_test ON wo_transfer_test.event_perfno_i = wo_event_link_test.event_perfno_i and wo_transfer_test.is_last_transfer='Y' AND wo_transfer_test.transfer_type IN ('T')
           JOIN схема.wo_transfer_dimension_test ON wo_transfer_test.event_transferno_i = wo_transfer_dimension_test.event_transferno_i
           JOIN схема.counter ON wo_transfer_dimension_test.counterno_i=counter.counterno_i
           JOIN схема.counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
           JOIN схема.counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
           WHERE 1=1
                 AND wo_event_link_test.event_key_root = (SELECT e.event_key_root FROM схема.wo_event_link_test e WHERE e.event_perfno_i = p_curr_plan_event_perfno_i)
                 AND wo_header_test.ac_registr = (SELECT w.ac_registr FROM схема.wo_header_test w WHERE w.event_perfno_i = p_curr_plan_event_perfno_i)
                 AND (-- pending events
                      wo_event_link_test.pending_status < 0
                      AND p_pending_status = 0
                      OR
                      -- future events
                      p_pending_status > 0
                      AND wo_event_link_test.pending_status < p_pending_status
                      AND wo_event_link_test.pending_status >= 0
                     )

                 AND wo_event_link_test.event_type = 'DOC_EFF'
          ) last_due_at ON (last_due_at.min_pending_status = last_due_at.pending_status -- pending events
                            AND p_pending_status = 0
                            OR
                            last_due_at.max_pending_status = last_due_at.pending_status -- future events
                            AND p_pending_status > 0
                           )
                         AND last_due_at.code = counter_definition.code
                         AND treq_interval.dimension_type = 'I'
    WHERE 1=1
          AND wo_event_link_test.event_perfno_i = p_curr_plan_event_perfno_i

   )


    SELECT event_transferno_i,
           wo_transfer_dimension_test_seq.NEXTVAL as wo_transfer_dimensionno_i,
           counterno_i,
           treq_intervalno_i,
           togo_interval,
           event_perfno_i,
           recno,
           is_last_transfer,
           transfer_type,
           treq_dimension_groupno_i,
           treq_interval_groupno_i,
           first_later_logic,
           transfer_time,
           date_transfer,
           transfer_hours,
           transfer_cycles,
           transfer_days,
           doc_ref,
           legno_i,
           limit_type,
           dc_use_case,
           transfer_context,
           init_option,
           absolute_due_date,
           absolute_due_time,
           defer,
           code,
           mfg_date,
     CASE WHEN wo_transfer_data.dimension_type = 'B' AND wo_transfer_data.code = 'D'
          THEN (wo_transfer_data.treq_interval_due_at - wo_transfer_data.mfg_date)*24*60 + 1439
          WHEN wo_transfer_data.dimension_type = 'B' AND wo_transfer_data.code <> 'D'
          THEN wo_transfer_data.treq_interval_due_at
     ELSE
     CASE WHEN wo_transfer_data.code = 'D'
     THEN CASE WHEN wo_transfer_data.accuracy = 'D' AND wo_transfer_data.unit = 'H'
               THEN DECODE (wo_transfer_data.dimension_type,
                            'W', (TRUNC(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value + NUMTODSINTERVAL(wo_transfer_data.amount_interval, 'HOUR'))
                                  - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                 ),
                            'I', TRUNC(wo_transfer_data.last_perf_value/60/24)*60*24 + wo_transfer_data.amount_interval*60
                           ) * 24 * 60

               WHEN wo_transfer_data.accuracy IN ('F', 'H', 'D')
               THEN CASE WHEN wo_transfer_data.unit = 'H' AND wo_transfer_data.accuracy <> 'D'
                         THEN DECODE(wo_transfer_data.dimension_type,
                                     -- THRESHOLD
                                     'W', (wo_transfer_data.baseline_value - wo_transfer_data.mfg_date) * 24 * 60 + wo_transfer_data.amount_interval * 60,
                                     -- INTERVAL
                                     'I', NVL(DECODE(wo_transfer_data.accuracy,
                                                     'F', wo_transfer_data.last_perf_value,
                                                     'H', TRUNC(wo_transfer_data.last_perf_value/60)*60
                                                    )
                                              , 0
                                             ) + wo_transfer_data.amount_interval * 60

                                    )
                         WHEN NVL(wo_transfer_data.unit, 'D') = 'D'
                         THEN DECODE(wo_transfer_data.dimension_type,
                                     -- THRESHOLD
                                     'W', (wo_transfer_data.baseline_value - wo_transfer_data.mfg_date) * 24 * 60 + wo_transfer_data.amount_interval * 1440,
                                     -- INTERVAL
                                     'I', NVL(DECODE(wo_transfer_data.accuracy,
                                                     'F', wo_transfer_data.last_perf_value,
                                                     'H', TRUNC(wo_transfer_data.last_perf_value/60)*60,
                                                     'D', TRUNC(wo_transfer_data.last_perf_value/24/60)*24*60
                                                    )
                                               , 0
                                              ) + wo_transfer_data.amount_interval * 1440
                                    )
                         WHEN wo_transfer_data.unit = 'MT'
                         THEN DECODE(wo_transfer_data.dimension_type,
                                     -- THRESHOLD
                                     'W', (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value, wo_transfer_data.amount_interval)
                                           - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                          ) * 24 * 60,
                                     -- INTERVAL
                                     'I', (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                      + wo_transfer_data.mfg_date
                                                      + NVL(DECODE(wo_transfer_data.accuracy,
                                                                   'F', wo_transfer_data.last_perf_value/24/60,
                                                                   'H', TRUNC(wo_transfer_data.last_perf_value/60)/24,
                                                                   'D', TRUNC(wo_transfer_data.last_perf_value/24/60)
                                                                   )
                                                            , 0
                                                           )
                                                      , wo_transfer_data.amount_interval
                                                     )
                                           - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                          ) * 24 * 60
                                    )
                         WHEN wo_transfer_data.unit = 'YR'
                         THEN DECODE(wo_transfer_data.dimension_type,
                                     -- THRESHOLD
                                     'W', (
                                           ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value, wo_transfer_data.amount_interval * 12)
                                           - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                          ) * 24 * 60,
                                     -- INTERVAL
                                     'I', (ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                      + wo_transfer_data.mfg_date
                                                      + NVL(DECODE(wo_transfer_data.accuracy,
                                                                   'F', wo_transfer_data.last_perf_value/24/60,
                                                                   'H', TRUNC(wo_transfer_data.last_perf_value/60)/24,
                                                                   'D', TRUNC(wo_transfer_data.last_perf_value/24/60)
                                                                  )
                                                            , 0
                                                           )
                                                      , wo_transfer_data.amount_interval * 12
                                                     )
                                           - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                          ) * 24 * 60
                                    )
                         ELSE 0
                    END
                    + DECODE (wo_transfer_data.accuracy, 'H', 59, 'D', 1439, 0)

               WHEN wo_transfer_data.accuracy = 'M'
               THEN DECODE(wo_transfer_data.dimension_type,
                           -- THRESHOLD
                           'W', (ADD_MONTHS(TRUNC(
                                                  CASE WHEN wo_transfer_data.unit = 'H'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value + wo_transfer_data.amount_interval/24
                                                       WHEN NVL(wo_transfer_data.unit, 'D') = 'D'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value + wo_transfer_data.amount_interval
                                                       WHEN wo_transfer_data.unit = 'MT'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value, wo_transfer_data.amount_interval)
                                                       WHEN wo_transfer_data.unit = 'YR'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value, wo_transfer_data.amount_interval * 12)
                                                       ELSE TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                  END
                                                  , 'MONTH'
                                                 )
                                            , 1
                                           )
                                  - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                 ),
                           -- INTERVAL
                           'I', (ADD_MONTHS(TRUNC(
                                                  CASE WHEN wo_transfer_data.unit = 'H'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60) + wo_transfer_data.amount_interval/24
                                                       WHEN NVL(wo_transfer_data.unit, 'D') = 'D'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60) + wo_transfer_data.amount_interval
                                                       WHEN wo_transfer_data.unit = 'MT'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60), wo_transfer_data.amount_interval)
                                                       WHEN wo_transfer_data.unit = 'YR'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60), wo_transfer_data.amount_interval * 12)
                                                       ELSE TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                  END
                                                  , 'MONTH'
                                                 )
                                            , 1
                                           )
                                  - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                 )
                         ) * 24 * 60 - 1
               WHEN wo_transfer_data.accuracy = 'Y'
               THEN DECODE(wo_transfer_data.dimension_type,
                           -- THRESHOLD
                           'W', (ADD_MONTHS(TRUNC(
                                                  CASE WHEN wo_transfer_data.unit = 'H'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value + wo_transfer_data.amount_interval/24
                                                       WHEN NVL(wo_transfer_data.unit, 'D') = 'D'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value + wo_transfer_data.amount_interval
                                                       WHEN wo_transfer_data.unit = 'MT'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value, wo_transfer_data.amount_interval)
                                                       WHEN wo_transfer_data.unit = 'YR'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.baseline_value, wo_transfer_data.amount_interval * 12)
                                                       ELSE TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                  END
                                                  , 'YEAR'
                                                 )
                                            , 12
                                           )
                                 - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                               ),
                           -- INTERVAL
                           'I', (ADD_MONTHS(TRUNC(
                                                  CASE WHEN wo_transfer_data.unit = 'H'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60) + wo_transfer_data.amount_interval/24
                                                       WHEN NVL(wo_transfer_data.unit, 'D') = 'D'
                                                       THEN TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60) + wo_transfer_data.amount_interval
                                                       WHEN wo_transfer_data.unit = 'MT'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60), wo_transfer_data.amount_interval)
                                                       WHEN wo_transfer_data.unit = 'YR'
                                                       THEN ADD_MONTHS(TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date + TRUNC(NVL(wo_transfer_data.last_perf_value, 0)/24/60), wo_transfer_data.amount_interval * 12)
                                                       ELSE TO_DATE('31.12.1971', 'dd.mm.yyyy')
                                                  END
                                                  , 'YEAR'
                                                 )
                                            , 12
                                           )
                                  - (TO_DATE('31.12.1971', 'dd.mm.yyyy') + wo_transfer_data.mfg_date)
                                 )
                         ) * 24 * 60 - 1
               ELSE 0
          END
     WHEN wo_transfer_data.code IN ('C', 'H', 'AC', 'AH')
     THEN CASE --THRESHOLD
               WHEN wo_transfer_data.based_on IN ('M','D','F')
               THEN wo_transfer_data.baseline_value + wo_transfer_data.amount_interval * DECODE(wo_transfer_data.code, 'H', 60, 'AH', 60, 1)
               WHEN wo_transfer_data.based_on IN ('V')
               THEN CASE WHEN wo_transfer_data.dimension IN ('C','H','AH','AC') AND wo_transfer_data.dimension = wo_transfer_data.code
                         THEN (wo_transfer_data.amount + wo_transfer_data.amount_interval) * DECODE(wo_transfer_data.code, 'H', 60, 'AH', 60, 1)
                         WHEN wo_transfer_data.dimension IN ('D')
                         THEN wo_transfer_data.counter_hist_value + (wo_transfer_data.amount_interval) * DECODE(wo_transfer_data.code, 'H', 60, 'AH', 60, 1)
                         ELSE 0
                    END
               -- INTERVAL
               ELSE NVL(wo_transfer_data.last_perf_value, 0) + wo_transfer_data.amount_interval * DECODE(wo_transfer_data.code, 'H', 60, 'AH', 60, 1)
          END
     ELSE 0
     END
     END
     as due_at,
     taskcard_type,
     treq_interval_group_status

    FROM wo_transfer_data
    WHERE 1=1
          AND (treq_interval_group_status <> 2 OR treq_interval_group_status IS NULL) -- исключаю Terminated Time Requirement
    ;

        INSERT INTO схема.wo_transfer_dimension_test (
                event_transferno_i,
                wo_transfer_dimensionno_i,
                counterno_i,
                treq_intervalno_i,
                togo_interval,
                due_at)
        SELECT event_transferno_i,
               wo_transfer_dimensionno_i,
               counterno_i,
               treq_intervalno_i,
               togo_interval,
               due_at
        FROM wo_transfer_temp

        ;

        INSERT INTO схема.wo_transfer_test (
                event_transferno_i,
                event_perfno_i,
                recno,
                is_last_transfer,
                transfer_type,
                treq_dimension_groupno_i,
                treq_interval_groupno_i,
                first_later_logic,
                transfer_time,
                date_transfer,
                transfer_hours,
                transfer_cycles,
                transfer_days,
                doc_ref,
                legno_i,
                limit_type,
                dc_use_case,
                transfer_context,
                init_option,
                absolute_due_date,
                absolute_due_time,
                defer
                )


        SELECT DISTINCT event_transferno_i,
                event_perfno_i,
                recno,
                is_last_transfer,
                transfer_type,
                treq_dimension_groupno_i,
                treq_interval_groupno_i,
                first_later_logic,
                transfer_time,
                date_transfer,
                transfer_hours,
                transfer_cycles,
                transfer_days,
                doc_ref,
                legno_i,
                limit_type,
                dc_use_case,
                transfer_context,
                init_option,
                (SELECT MAX(t.mfg_date + TRUNC(t.due_at/24/60)) FROM wo_transfer_temp t WHERE t.code = 'D' ) as absolute_due_date,
                (SELECT MAX(MOD(t.due_at, 24*60)) FROM wo_transfer_temp t WHERE t.code = 'D') as absolute_due_time,
                defer
        FROM wo_transfer_temp
        ;

		-- Очистка записей временной таблицы
        DELETE FROM wo_transfer_temp;

		-- Завершение транзакции с сохранением данных
        COMMIT;
END;


/* ****************************************************************************
 * Шаг 3. Создание временных процедур, предназначенных для тестирования задач ТЗ
 * Прмечание: для каждой задачи ТЗ создана отдельная процедура
 * ************************************************************************* */

/* Шаг 3.1. Задача: Инициализация (планирование) события ТО (Assign): Auto Calculation
 * Входные параметры:
 * - p_docno_i - уникальный номер документа
 * - p_aircraftno_i - идентификатор воздушного судна
 * Output:
 * - event_perfno_i - уникальный идентификатор созданного планирования события ТО
*/
CREATE OR REPLACE PROCEDURE схема.maint_init_mpt_assign_auto_calculated (
        p_docno_i IN NUMBER,
        p_aircraftno_i IN NUMBER
)
IS
    v_event_perfno_i NUMBER(12);
    v_curr_plan_event_perfno_i NUMBER(12);
    v_init_future_events VARCHAR2 (1);
    v_pending_status NUMBER(12) := 0;
BEGIN

    /* 1. Подготовка тестовой среды (копируем историю планирования искомого события ТО) */
    схема.prepare_test_area(
        p_docno_i,
        p_aircraftno_i);

    /* 2. Инициализация текущего планирования события ТО */

    /* Добавляем новую запись в таблицу wo_header */
    схема.create_wo_header_row (
        p_docno_i,
        p_aircraftno_i,
        v_event_perfno_i);
    v_curr_plan_event_perfno_i := v_event_perfno_i;

    /* Добавляем новую запись в таблицу wo_event_link */
    схема.create_wo_event_link_row (
        v_event_perfno_i,
        p_docno_i,
        p_aircraftno_i,
        v_pending_status,
        v_init_future_events);

    /* Добавляем новые записи в таблицы wo_transfer и wo_transfer_dimension */
    схема.create_wo_transfer_and_wo_transfer_dim_row (
        v_event_perfno_i,
        v_curr_plan_event_perfno_i,
        v_pending_status,
        null,
        1,
        'I',
        0);

    /* 2. Инициализация будущх планирований события ТО
     * Если значение v_init_future_events = N,
     * тогда для заданого события ТО будущих событий планирования не создаётся.
     * Если же значение равно Y, тогда в соответствии с настройками системы
     * для заданного события ТО создаётся 5 будущих событий планирования.
     */
     if v_init_future_events NOT IN ('N', ' ')
     THEN

        WHILE v_pending_status < 5 LOOP
          v_pending_status := v_pending_status + 1;
          /* Добавляем новую запись в таблицу wo_header */
          схема.create_wo_header_row (
                p_docno_i,
                p_aircraftno_i,
                v_event_perfno_i);
          /* Добавляем новую запись в таблицу wo_event_link */
          схема.create_wo_event_link_row (
                v_event_perfno_i,
                p_docno_i,
                p_aircraftno_i,
                v_pending_status,
                v_init_future_events);
          /* Добавляем новые записи в таблицы wo_transfer и wo_transfer_dimension */
          схема.create_wo_transfer_and_wo_transfer_dim_row (
                v_event_perfno_i,
                v_curr_plan_event_perfno_i,
                v_pending_status,
                null,
                1,
                'I',
                0);

        END LOOP;
     end if;

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
END;


/* Шаг 3.2. Задача: Корректировка планирования события ТО (Change Next Due): Synchronize Time Requirement.
 * Примечание: задача не имеет входных параметров, так как перед её выполнением необходимо заполнить тестовые таблицы,
 * путём выполнения инициализации заданного события ТО, как показано на шаге 3.1 или другим методом инициализаци.
*/
CREATE OR REPLACE PROCEDURE схема.maint_init_mpt_change_next_due_sync_time_req
IS
    v_event_perfno_i NUMBER(12);
    v_curr_plan_event_perfno_i NUMBER(12);
    v_init_future_events VARCHAR2 (1);
    v_pending_status NUMBER(12) := 0;
    v_recno NUMBER(12);
BEGIN

    /* Вычисляем параметры события ТО и расчитывает значения нового recno */
    select event_perfno_i into v_curr_plan_event_perfno_i
      from схема.wo_event_link_test
     where pending_status = v_pending_status;

    select case when count(1) > 0 then 'Y' else 'N' end into v_init_future_events
      from схема.wo_event_link_test
     where pending_status > 0;

    select max(recno) into v_recno
      from схема.wo_transfer_test
     where event_perfno_i = v_curr_plan_event_perfno_i;
     v_recno := v_recno + 1;

    /* Устанавливаем значение is_last_transfer = N для действующих значений планирования */
    update схема.wo_transfer_test
       set is_last_transfer = 'N'
     where event_perfno_i in (select event_perfno_i from схема.wo_event_link_test where pending_status >= 0);

    /* Добавляем новые записи (recno) в таблицы wo_transfer и wo_transfer_dimension */
    схема.create_wo_transfer_and_wo_transfer_dim_row (
        v_curr_plan_event_perfno_i,
        v_curr_plan_event_perfno_i,
        v_pending_status,
        null,
        v_recno,
        'S', /* transfer_context = Sync. Time Requirement (S) */
        0);

    /* 2. Инициализация будущх планирований события ТО
     * Если значение v_init_future_events = N,
     * тогда для заданого события ТО будущих событий планирования не создаётся.
     * Если же значение равно Y, тогда в соответствии с настройками системы
     * для заданного события ТО создаётся 5 будущих событий планирования.
     */
     if v_init_future_events != 'N'
     THEN

        WHILE v_pending_status < 5 LOOP
          v_pending_status := v_pending_status + 1;
          /* Получаем значение event_perfno_i для будущего планирования с индексом v_pending_status */
          select event_perfno_i into v_event_perfno_i
           from схема.wo_event_link_test
          where pending_status = v_pending_status;

          /* Добавляем новые записи в таблицы wo_transfer и wo_transfer_dimension */
          схема.create_wo_transfer_and_wo_transfer_dim_row (
                v_event_perfno_i,
                v_curr_plan_event_perfno_i,
                v_pending_status,
                null,
                v_recno,
                'S', /* transfer_context = Sync. Time Requirement (S) */
                0);

        END LOOP;
     end if;

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
END;


/* ****************************************************************************
 * Шаг 4. Примеры запуска тестовых процедур для задач ТЗ и методика проверки
 * ************************************************************************* */

/* Шаг 4.1 Представления, предназначенные для тестирования работы алгоритмов */

/* Шаг 4.1.1 Скрипты создания представлений */
/* Проверка значений таблицы wo_event_header */
create or replace view схема.wo_header_test_check as
 SELECT r.*
   FROM
     (SELECT  'Test row' rtype,
        r.EVENT_PERFNO_I,
        r.ATA_CHAPTER,
        r.STATE,
        r.AC_REGISTR,
        r."TYPE",
        r.EST_GROUNDTIME,
        r.EVENT_TYPE,
        r.PRIO,
        l.PENDING_STATUS
  FROM схема.wo_header_test r
  JOIN схема.wo_event_link_test l
    ON r.EVENT_PERFNO_I = l.EVENT_PERFNO_I
 WHERE l.pending_status >= 0
UNION
 SELECT 'Original row' rtype,
        r.EVENT_PERFNO_I,
        r.ATA_CHAPTER,
        r.STATE,
        r.AC_REGISTR,
        r."TYPE",
        r.EST_GROUNDTIME,
        r.EVENT_TYPE,
        r.PRIO,
        l.PENDING_STATUS
  FROM схема.wo_header r
  JOIN схема.wo_event_link l
    ON r.EVENT_PERFNO_I = l.EVENT_PERFNO_I
 WHERE l.MEVT_HEADERNO_I IN (SELECT p.MEVT_HEADERNO_I FROM схема.wo_event_link_test p WHERE p.PENDING_STATUS = 0)
   AND l.pending_status >= 0) r
ORDER BY r.pending_status, r.rtype;

/* Проверка значений таблицы wo_event_link */
create or replace view схема.wo_event_link_test_check as
 SELECT r.*
   FROM
     (SELECT 'Test row' rtype,
            r.MEVT_HEADERNO_I,
            r.PLANABLE_STATUS,
            r.PENDING_STATUS,
            r.EVENT_TYPE,
            r.EVENT_KEY,
            r.EVENT_KEY_PARENT,
            r.EVENT_KEY_ROOT,
            r.EVENT_STATUS,
            r.AUTO_REPORT_BACK,
            r.REVISION,
            r.REVISION_STATUS,
            r.NEXT_REVISION,
            r.NEXT_REVISION_STATUS,
            r.EVENT_NAME,
            r.EVENT_DISPLAY,
            r.TASKCARD_TYPE
      FROM схема.wo_event_link_test r
     WHERE r.pending_status >=0
     UNION
     SELECT 'Original row' rtype,
            r.MEVT_HEADERNO_I,
            r.PLANABLE_STATUS,
            r.PENDING_STATUS,
            r.EVENT_TYPE,
            r.EVENT_KEY,
            r.EVENT_KEY_PARENT,
            r.EVENT_KEY_ROOT,
            r.EVENT_STATUS,
            r.AUTO_REPORT_BACK,
            r.REVISION,
            r.REVISION_STATUS,
            r.NEXT_REVISION,
            r.NEXT_REVISION_STATUS,
            r.EVENT_NAME,
            r.EVENT_DISPLAY,
            r.TASKCARD_TYPE
      FROM схема.wo_event_link r
     WHERE r.MEVT_HEADERNO_I IN (SELECT p.MEVT_HEADERNO_I FROM схема.wo_event_link_test p WHERE p.PENDING_STATUS = 0)
       AND r.pending_status >=0) r
ORDER BY r.pending_status, r.rtype;

/* Проверка значений таблицы wo_transfer */
create or replace view схема.wo_transfer_test_check as
 SELECT r.*
   FROM
     (
      SELECT 'Test row' rtype,
        r.event_perfno_i,
        r.event_transferno_i,
        r.RECNO,
        r.IS_LAST_TRANSFER,
        r.TRANSFER_TYPE,
        r.TREQ_DIMENSION_GROUPNO_I,
        r.TREQ_INTERVAL_GROUPNO_I,
        r.FIRST_LATER_LOGIC,
        r.TRANSFER_TIME,
        r.DATE_TRANSFER,
        r.TRANSFER_HOURS,
        r.TRANSFER_CYCLES,
        r.TRANSFER_DAYS,
        r.DOC_REF,
        r.LEGNO_I,
        r.LIMIT_TYPE,
        r.DC_USE_CASE,
        r.TRANSFER_CONTEXT,
        r.INIT_OPTION,
        r.ABSOLUTE_DUE_DATE,
        r.ABSOLUTE_DUE_TIME,
        r.DEFER,
        l.PENDING_STATUS
  FROM схема.wo_transfer_test r
  JOIN схема.wo_event_link_test l
    ON r.event_perfno_i = l.event_perfno_i
 where l.pending_status >=0
   UNION
 SELECT 'Original row' rtype,
        r.event_perfno_i,
        r.event_transferno_i,
        r.RECNO,
        r.IS_LAST_TRANSFER,
        r.TRANSFER_TYPE,
        r.TREQ_DIMENSION_GROUPNO_I,
        r.TREQ_INTERVAL_GROUPNO_I,
        r.FIRST_LATER_LOGIC,
        r.TRANSFER_TIME,
        r.DATE_TRANSFER,
        r.TRANSFER_HOURS,
        r.TRANSFER_CYCLES,
        r.TRANSFER_DAYS,
        r.DOC_REF,
        r.LEGNO_I,
        r.LIMIT_TYPE,
        r.DC_USE_CASE,
        r.TRANSFER_CONTEXT,
        r.INIT_OPTION,
        r.ABSOLUTE_DUE_DATE,
        r.ABSOLUTE_DUE_TIME,
        r.DEFER,
        l.PENDING_STATUS
  FROM схема.wo_transfer r
  JOIN схема.wo_event_link l
    ON r.event_perfno_i = l.event_perfno_i
 WHERE l.MEVT_HEADERNO_I IN (SELECT p.MEVT_HEADERNO_I FROM схема.wo_event_link_test p WHERE p.PENDING_STATUS = 0)
   AND l.pending_status >= 0) r
 ORDER BY r.pending_status, r.rtype;

/* Проверка значений таблицы wo_transfer_dimension */
 create or replace view схема.wo_transfer_dimension_test_check as
 SELECT r.*
   FROM
     (
     SELECT 'Test row' rtype,
            r.event_transferno_i,
            r.wo_transfer_dimensionno_i,
            r.counterno_i,
            r.treq_intervalno_i,
            r.togo_interval,
            r.due_at,
            t.event_perfno_i,
            l.PENDING_STATUS
      FROM схема.wo_transfer_dimension_test r
      JOIN схема.wo_transfer_test t
        ON t.event_transferno_i = r.event_transferno_i
      JOIN схема.wo_event_link_test l
        ON t.event_perfno_i = l.event_perfno_i
       AND l.pending_status >= 0
   UNION
    SELECT 'Original row' rtype,
            r.event_transferno_i,
            r.wo_transfer_dimensionno_i,
            r.counterno_i,
            r.treq_intervalno_i,
            r.togo_interval,
            r.due_at,
            t.event_perfno_i,
            l.PENDING_STATUS
      FROM схема.wo_transfer_dimension r
      JOIN схема.wo_transfer t
        ON t.event_transferno_i = r.event_transferno_i
      JOIN схема.wo_event_link l
        ON t.event_perfno_i = l.event_perfno_i
 WHERE l.MEVT_HEADERNO_I IN (SELECT p.MEVT_HEADERNO_I FROM схема.wo_event_link_test p WHERE p.PENDING_STATUS = 0)
   and l.pending_status >= 0) r
 ORDER BY r.pending_status, r.counterno_i, r.rtype;

/* Шаг 4.1.2 Пример запуска проверки после выполнения процедур 4.2-4.3, представленных далее в этом скрипте */
SELECT *
  FROM схема.wo_header_test_check;
SELECT *
  FROM схема.wo_event_link_test_check;
SELECT *
  FROM схема.wo_transfer_test_check;
SELECT *
  FROM схема.wo_transfer_dimension_test_check;


/* Шаг 4.2 Задача: Инициализация (планирование) события ТО (Assign): Auto Calculation
 */
BEGIN
    схема.maint_init_mpt_assign_auto_calculated(
            p_docno_i =>44696,
            p_aircraftno_i => 178
    );
END;

/* Шаг 4.3 Задача: Корректировка планирования события ТО (Change Next Due): Sync Time Requirement
 * Примечание: перед выполнение данного тестирования следует запустить процедуру 4.2, чтобы
 *             инициализировать планирование события ТО, по которому будет выполнена операция Sync Time Requirements.
 */
BEGIN
    схема.maint_init_mpt_change_next_due_sync_time_req;
END;