/* *******************************************************
 * *** ТЗ: Инициализация заполнение таблицы event_effectiviry_mapping
 * *** SQL - скрипт	S1
******************************************************* */


/* ****************************************************************************
 * Шаг 1. Создание временных таблиц и последовательностей,
 * предназначенных для тестирования работы SQL-скрипта
 * Примечание: таблицы, содержат только колонки, изменяемые SQL-скриптом.
 * ************************************************************************* */

CREATE TABLE схема.EVENT_EFFECTIVITY_MAPPING_TEST
(
    EVENT_TYPE           VARCHAR2(20),
    EVENT_KEY_PARENT     NUMBER(12),
    AFFECTED             VARCHAR2(1),
    NEEDS_RECALCULATION  VARCHAR2(1),
    AC_REGISTR           VARCHAR2(6),
    AIRCRAFTNO_I         NUMBER(12),
    PSN                  NUMBER(12),
    EVENT_STATUS         VARCHAR2(1),
    EVENT_STATUS_SINGLE  VARCHAR2(1),
    LAST_EVENT_PERFNO_I  NUMBER(12),
    EVENT_PERFNO_I       NUMBER(12),
    STATUS               NUMBER(12),
    EFFECTIVITY_LINKNO_I NUMBER(12)
);

/* Добавляется новая таблица статусов документов, для обработки статусов документов */
CREATE TABLE схема.document_status (
    code VARCHAR2(5),
    name VARCHAR2(100),
    description CLOB,
    status_group VARCHAR2(20),
    pre_post VARCHAR2(10)
);

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
    EXECUTE IMMEDIATE 'TRUNCATE TABLE схема.event_effectivity_mapping_test';

    /* Таблица event_effectivity_mapping_tes заполняется данными при активации новой ревизии в effectivity */
  INSERT INTO схема.EVENT_EFFECTIVITY_MAPPING_TEST
  SELECT
    'EFL',
    doc_header.docno_i,
    CASE WHEN applicability.applicable IN ('X', 'N') THEN 'N' ELSE 'Y' END,
    'N',
    aircraft.ac_registr,
    aircraft.aircraftno_i,
    0,
    '',
    '',
    0,
    0,
    0,
    event_effectivity_link.effectivity_linkno_i
  FROM схема.doc_header
  LEFT JOIN схема.event_effectivity_link ON doc_header.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
  LEFT JOIN схема.event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
  LEFT JOIN схема.applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i AND applicability.ref_type = 'AC_INT'
  LEFT JOIN схема.aircraft ON applicability.ref_key = aircraft.aircraftno_i
  WHERE doc_header.docno_i=p_docno_i
    AND applicability.ref_key=p_aircraftno_i
    AND event_effectivity.status = 0;



  /* заполнение новой таблица статусов document_status */
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES (' ', ' ', ' ', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('!', 'Unknown', 'Unknown', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('-', 'Not Assigned', '-', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('/', 'No status / Not installed', 'Component not installed into this Aircraft', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('0', 'Not performed', '-', 'Open', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('1', 'Aircraft group changed', '-', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('2', 'Aircraft/Rotable deactivated', '-', 'N/A', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('3', 'Effectivity Historized', '-', 'N/A', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('?', 'No status', '-', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('A', 'On Attrition', 'Indicates that the modification of the rotable is only done in the course of a repair when the unit is removed and sent off, comparable to a maintenance or soft-time limit of a rotable requirement. You would not remove a rotable just for the performance of this document. Status: open. Before reporting back the document, it is possible to change the assignment code to "open" again. Reporting back a document with the status On Attrition, the document can be initialised in Maintenance Event Initialiser and in View/Edit Modifications.', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('B', 'Postponed (open)', 'The performance of the document has been postponed. Status: ope', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('C', 'Closed', 'Closed by your company. The normal closing procedure is done in Reporting Back Maintenance Event', 'Closed', 'Post');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('D', 'Delete assignment!', 'When you need to change the assignment status of an aircraft or component you must delete the assignment first before you can assign a new status.', '-', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('E', 'Event-Triggered', 'Reserved for special cases: Event Triggered Modifications. Indicates that a document is pending and acts as closed until a specific event, for example a hard-landing, occurs. Then you can change the status of that aircraft or rotable to open. This can be done by clicking Start Event Triggered in the Status for Document window.', 'Closed', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('F', 'Ex Factory', 'The modification has already been performed by the manufacturer. Status: closed.', 'Closed', 'Post');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('G', 'Term.Action open', '-', 'Open', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('H', 'Rotable Driven - Accomplished', '-', 'Closed', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('I', 'Information Only', 'No status.', '-', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('J', 'Rejected by Operator', 'Status: open.', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('K', 'Rotable Driven - Not Accomplished', '-', 'Open', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('L', 'Logbook Controlled', 'Status: open. This document does not appear in the forecast.', 'Open', 'N/A');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('M', 'Taskcard Controlled', 'A repetitive document might be controlled by a taskcard. Status: open. This document does not appear in the forecast.', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('N', 'Not Applicable', 'Can also be set manually. You must enter a reason for this status in the Initialise Documents window. This reason will also be printed on the Modification Status report.', 'N/A', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('O', 'Open', 'Open.', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('P', 'Partly Performed', '-', 'Open', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('Q', 'Part-Requirem.Contr.', 'A repetitive document might be controlled by a part requirement. Status: open. This document does not appear in the forecast.', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('R', 'Repetitive', '-', 'Open', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('S', 'DOC.IN PREP.', '-', 'Open', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('T', 'Terminated by third party', 'The document has been performed by a third party. Status: closed.', 'Closed', 'Post');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('U', 'Cancelled', 'This status is automatically set for documents which were cancelled or superseded - see Replacement Information section.' ||
                                                                                                        'It is also possible to assign this status manually to individual serial numbers. You must enter a reason for this status in the Initialise Documents window. This reason will also be printed on the Modification Status report.', 'N/A', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('V', 'Performed by prev. Operator', 'The document has been performed by the previous operator of the aircraft or component. Status: closed.', 'Closed', 'Post');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('W', 'See prev. Doc. Status', 'This status indicates that the status of the document has not been entered into AMOS. This status is highlighted orange in View/Edit Modifications.', '-', 'N/A');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('X', 'Externally Controlled', '-', 'N/A', '-');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('Y', 'Reopened', 'Status of the document is open. Setting the status to reopen is only possible if the document was closed before. When setting this status, a reason can be given.', 'Open', 'Pre');
  INSERT INTO схема.document_status (code, name, description, status_group, pre_post) VALUES ('Z', 'Superseded', 'This status is automatically set for documents which were cancelled or superseded - see Replacement Information section.' ||
                                                                                                         'It is also possible to assign this status manually to individual serial numbers. You must enter a reason for this status in the Initialise Documents window. This reason will also be printed on the Modification Status report.', 'N/A', 'Pre');

END;

/* ****************************************************************************
 * Шаг 3. После изменения статуса нижнего документа вышестоящим документам
   проставляется флаг need_recalculation
 * Входные параметры:
 * - p_docno_i - уникальный идентификатор документа
* Примечание: Принимает нижний документ у которого поменялся статус
 * - p_aircraftno_i - уникальный идентификатор воздушного судна

* Примечание:
Данный скрип ищет все документы у которых нет флага signoff-tree(в соответствующей ветке) и просталяется флаг пересчета
 * ************************************************************************* */

CREATE OR REPLACE PROCEDURE схема.update_status_tree_row_doc (
        p_docno_i NUMBER, p_aircraftno_i NUMBER
)
IS
    v_parrent_docno_i NUMBER(12);
    v_aircraftno_i NUMBER(12);

    CURSOR c_update_status_tree (
        cp_docno_i NUMBER,
        cp_aircraftno_i NUMBER
        ) IS
    SELECT DISTINCT
        sub.docno_i AS parrent_docno_i,
        applicability.ref_key
    FROM (
        SELECT
            LEVEL AS lev,
            item.id AS docno_i,
            item.pid,
            item.docno,
            item.revision,
            CONNECT_BY_ROOT item.docno_i AS root,
            item.write_off_logic,
            item.doc_type
        FROM (
            SELECT
                main.id,
                CASE
                    WHEN main.id = main.parrent_id THEN NULL
                    ELSE main.parrent_id
                END AS pid,
                header.docno_i,
                header.docno,
                header.revision,
                write_off.write_off_logic,
                header.doc_type
            FROM (
                SELECT DISTINCT
                    tree.docno_i AS id,
                    CASE
                        WHEN (
                            SELECT COUNT(doc_signoff_tree.ref_docno_i)
                            FROM doc_signoff_tree
                            WHERE tree.docno_i = doc_signoff_tree.ref_docno_i
                        ) > 0 THEN tree.docno_i
                        ELSE NULL
                    END AS parrent_id,
                    doc.docno
                FROM doc_signoff_tree tree
                JOIN doc_header doc ON tree.docno_i = doc.docno_i
                UNION
                SELECT
                    tree.ref_docno_i AS id,
                    tree.docno_i AS parrent_id,
                    doc.docno
                FROM doc_signoff_tree tree
                JOIN doc_header doc ON tree.ref_docno_i = doc.docno_i
                ) main
            JOIN doc_header header ON main.id = header.docno_i
            LEFT JOIN (
                SELECT
                    CASE
                        WHEN COUNT(docno_i) > 0 THEN 'Y'
                        ELSE 'N'
                    END AS write_off_logic,
                    docno_i
                FROM doc_signoff_tree
                WHERE docno_i = ref_docno_i
                GROUP BY docno_i
                ) write_off ON header.docno_i = write_off.docno_i
            ) item
        CONNECT BY NOCYCLE item.id = PRIOR item.pid
        START WITH item.id = item.docno_i
        ORDER SIBLINGS BY item.id, item.revision
    ) sub
    JOIN doc_header parrent ON sub.root = parrent.docno_i
    JOIN event_effectivity_link ON parrent.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
    JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
    JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                              AND applicability.ref_type = 'AC_INT'
    WHERE sub.root=cp_docno_i AND applicability.ref_key=cp_aircraftno_i
      AND parrent.docno_i<>sub.docno_i;
BEGIN
    OPEN c_update_status_tree (p_docno_i, p_aircraftno_i);
    LOOP
        FETCH c_update_status_tree  INTO
            v_parrent_docno_i,
            v_aircraftno_i;
        EXIT WHEN c_update_status_tree%NOTFOUND;
        UPDATE схема.event_effectivity_mapping_test
            SET needs_recalculation='Y'
            WHERE event_key_parent=v_parrent_docno_i AND aircraftno_i=v_aircraftno_i;
    END LOOP;
    COMMIT;
    CLOSE c_update_status_tree;
END;



/* ****************************************************************************
 * Шаг 4. Функции для расчета статуса
 * Входные параметры:
 * - p_docno_i - уникальный идентификатор документа
 * - p_aircraftno_i - уникальный идентификатор воздушного судна

**************************************************************************** */
/* ****************************************************************************
* Шаг 4.1. Функция расчета статуса всего древа
**************************************************************************** */


CREATE OR REPLACE FUNCTION схема.GET_EVENT_STATUS(
    p_docno_i IN NUMBER,
    p_aircraftno_i IN NUMBER
)
    RETURN VARCHAR2
    as event_status VARCHAR2(1);
        BEGIN
            SELECT
                CASE
                    WHEN status_event.p_applicable = 1 THEN
                        CASE
                            WHEN status_event.p_applicable_status = 1 THEN
                                CASE
                                    WHEN applicable=1 AND not_applicable=1 THEN
                                        CASE
                                            WHEN applicable_status = 1 AND not_applicable_status = 1 THEN
                                                CASE
                                                    WHEN status_event.open_group=1 THEN status_event.open_state
                                                    WHEN status_event.closing_group=1 THEN status_event.closing_state
                                                    WHEN status_event.not_assign_group = 1 THEN '-'
                                                    WHEN status_event.unknown_group = 1 THEN status_event.unknown_state
                                                    ELSE 'N'
                                                END
                                            WHEN applicable_status = 1 AND not_applicable_status = 0 THEN
                                                CASE
                                                    WHEN status_event.open_group=1 THEN status_event.open_state
                                                    WHEN status_event.closing_group=1 THEN status_event.closing_state
                                                    WHEN status_event.not_assign_group = 1 THEN '-'
                                                    WHEN status_event.unknown_group = 1 THEN status_event.unknown_state
                                                    ELSE 'N'
                                                END
                                            ELSE 'N'
                                        END
                                    WHEN applicable=1 AND not_applicable=0 THEN
                                        CASE
                                            WHEN status_event.open_group=1 THEN status_event.open_state
                                            WHEN status_event.closing_group=1  THEN status_event.closing_state
                                            WHEN status_event.unknown_group = 1 THEN status_event.unknown_state
                                            WHEN status_event.na_group=1 THEN status_event.na_state
                                            ELSE 'N'
                                        END
                                    ELSE 'N'
                                END
                        ELSE
                            CASE
                                WHEN status_event.applicable_status = 1 AND status_event.not_assign_group = 1 THEN '!'
                                ELSE 'N'
                            END

                        END
                    ELSE 'N'
                END  INTO event_status
            FROM
            (
                SELECT
                MAX(CASE
                    WHEN doc_sign_off.APPLICABLE ='Y' THEN 1
                    ELSE 0
                END) AS P_APPLICABLE,
                MAX(CASE WHEN doc_sign_off.applicable_status='Y' THEN 1 ELSE 0 end ) AS p_applicable_status,
                MAX(CASE
                    WHEN applicability.APPLICABLE ='Y' THEN 1
                    ELSE 0
                END) AS APPLICABLE,
                MAX(CASE
                    WHEN applicability.APPLICABLE <>'Y' THEN 1
                    ELSE 0
                END) AS NOT_APPLICABLE,
                MAX(CASE
                    WHEN mevt_effectivity.applicable_status ='Y' THEN 1
                    ELSE 0
                END) AS applicable_status,
                MAX(CASE
                    WHEN mevt_effectivity.applicable_status <>'Y'  OR mevt_effectivity.applicable_status is null THEN 1
                    ELSE 0
                END) AS not_applicable_status,
                MAX(CASE
                    WHEN document_status.status_group='Open'  THEN 1
                    ELSE 0
                END) AS open_group,
                MAX(CASE
                    WHEN document_status.status_group='-'  THEN 1
                    ELSE 0
                END) AS unknown_group,
                MAX(CASE
                    WHEN document_status.status_group = 'Closed' THEN 1
                    ELSE 0
                END) AS closing_group,
                MAX(CASE
                    WHEN mevt_effectivity.applicable_status = 'Y' AND wo_event_link.event_perfno_i IS NULL THEN 1
                    ELSE 0
                END) AS not_assign_group,
                MAX(CASE
                    WHEN document_status.status_group = 'N/A' THEN 1
                    ELSE null
                END) AS na_group,
                MAX(CASE
                    WHEN document_status.status_group = 'N/A' THEN document_status.code
                    ELSE null
                END) AS na_state,
                MAX(CASE
                    WHEN document_status.status_group = 'Open' THEN document_status.code
                    ELSE null
                END) AS open_state,
                MAX(CASE
                    WHEN document_status.status_group = 'Closed' THEN document_status.code
                    ELSE null
                END) AS closing_state,
                MAX(CASE
                    WHEN document_status.status_group = '-' THEN document_status.code
                    ELSE null
                END) AS unknown_state
            FROM (
                SELECT DISTINCT
                    parrent.docno_i,
                    sub.docno,
                    sub.docno_i AS d_docno_i,
                    sub.revision,
                    sub.doc_type,
                    sub.lev,
                    applicability.applicable,
                    mevt_effectivity.applicable_status,
                    applicability.ref_key
                FROM (
                    SELECT
                        LEVEL AS lev,
                        id AS docno_i,
                        pid,
                        item.docno,
                        item.revision,
                        CONNECT_BY_ROOT item.docno_i AS root,
                        item.write_off_logic,
                        doc_type
                    FROM (
                        SELECT
                            main.id,
                            CASE
                                WHEN main.id = main.parrent_id THEN NULL
                                ELSE main.parrent_id
                            END AS pid,
                            header.docno_i,
                            header.docno,
                            revision,
                            write_off.write_off_logic,
                            header.doc_type
                        FROM (
                            SELECT DISTINCT
                                tree.docno_i AS id,
                                CASE
                                    WHEN (
                                        SELECT COUNT(doc_signoff_tree.ref_docno_i) AS cnt
                                        FROM doc_signoff_tree
                                        WHERE tree.docno_i = doc_signoff_tree.ref_docno_i) > 0 THEN tree.docno_i
                                    ELSE NULL
                                END AS parrent_id,
                                docno
                            FROM doc_signoff_tree tree
                            JOIN doc_header doc ON tree.docno_i = doc.docno_i
                            UNION
                            SELECT
                                tree.ref_docno_i AS id,
                                tree.docno_i AS parrent_id,
                                doc.docno
                            FROM doc_signoff_tree tree
                            JOIN doc_header doc ON tree.ref_docno_i = doc.docno_i
                            ) main
                        JOIN doc_header header ON main.id = header.docno_i
                        /* Document write-off logic */
                        LEFT JOIN (
                            SELECT
                                CASE
                                    WHEN COUNT(docno_i) > 0 THEN 'Y'
                                    ELSE 'N'
                                END AS write_off_logic,
                                docno_i
                            FROM doc_signoff_tree
                            WHERE docno_i = ref_docno_i
                            GROUP BY docno_i
                            ) write_off ON header.docno_i = write_off.docno_i
                        ) item
                    CONNECT BY NOCYCLE PRIOR item.id = item.pid
                    ORDER SIBLINGS BY item.id, item.revision
                    ) sub
                JOIN doc_header parrent ON sub.root = parrent.docno_i
                JOIN event_effectivity_link ON parrent.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
                JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
                JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                LEFT JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                         AND mevt_header.ref_type='AC_INT'
                                         AND parrent.docno_i = mevt_header.mevt_key
                                         AND mevt_header.mevt_type = 'DO'
                LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                WHERE pid IS NOT NULL
                ) doc_sign_off
            LEFT JOIN doc_signoff_tree ON doc_sign_off.d_docno_i=doc_signoff_tree.docno_i AND doc_signoff_tree.docno_i = doc_signoff_tree.ref_docno_i
            JOIN event_effectivity_link ON doc_signoff_tree.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
            JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
            JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                                      AND doc_sign_off.ref_key=applicability.ref_key
                                      AND applicability.ref_type = 'AC_INT'
            JOIN aircraft ON applicability.ref_key = aircraft.aircraftno_i
            LEFT JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                         AND mevt_header.ref_type='AC_INT'
                                         AND doc_signoff_tree.docno_i = mevt_header.mevt_key
                                         AND mevt_header.mevt_type = 'DO'
            LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
            LEFT JOIN wo_event_link ON mevt_header.mevt_headerno_i = wo_event_link.mevt_headerno_i
                                           AND wo_event_link.pending_status = 0
            LEFT JOIN document_status ON wo_event_link.event_status=document_status.code
            WHERE doc_sign_off.docno_i = p_docno_i AND doc_sign_off.ref_key = p_aircraftno_i
            GROUP BY doc_sign_off.docno_i, aircraft.ac_registr) status_event;
        RETURN event_status;
    END;

/* ****************************************************************************
* Шаг 4.2. Функция для получения текущего event_perfno_i
**************************************************************************** */

CREATE OR REPLACE FUNCTION схема.GET_EVENT_PERFNO_I(
    p_docno_i IN NUMBER,
    p_aircraftno_i IN NUMBER
)
    RETURN NUMBER
    as event_perfno_i NUMBER(12);
        BEGIN
            SELECT
                CASE
                    WHEN get_event_perfno_i.unplanabel_status=1 AND get_event_perfno_i.planabel_status=0 THEN
                        CASE
                            WHEN get_event_perfno_i.closing_group=1 AND get_event_perfno_i.open_group=0
                                THEN get_event_perfno_i.max_unplanabel_event_perfno_i
                            ELSE get_event_perfno_i.min_unplanabel_event_perfno_i
                        END
                    WHEN get_event_perfno_i.unplanabel_status=1 AND get_event_perfno_i.planabel_status=1
                        THEN get_event_perfno_i.max_unplanabel_event_perfno_i
                    WHEN get_event_perfno_i.unplanabel_status=0 AND get_event_perfno_i.planabel_status=1
                        THEN get_event_perfno_i.min_planabel_event_perfno_i
                    ELSE 0
                END INTO event_perfno_i
            FROM(
                SELECT
                    MAX(
                        CASE
                            WHEN status_group = 'Open' THEN 1
                            ELSE 0
                        END
                    ) AS open_group,
                    MAX(
                        CASE
                            WHEN status_group = 'Closed' THEN 1
                            ELSE 0
                        END
                    ) AS closing_group,
                    MAX(unplanabel_event_perfno_i) AS max_unplanabel_event_perfno_i,
                    MIN(planabel_event_perfno_i) AS min_planabel_event_perfno_i,
                    MIN(unplanabel_event_perfno_i) AS min_unplanabel_event_perfno_i,
                    MAX(CASE WHEN PLANABLE_STATUS <= 0 THEN 1 ELSE 0 END) AS planabel_status,
                    MAX(CASE WHEN PLANABLE_STATUS > 0 THEN 1 ELSE 0 END)  AS unplanabel_status
                FROM (
                    SELECT
                        doc_sign_off.docno_i,
                        aircraft.aircraftno_i,
                        (
                            SELECT
                                wel.event_perfno_i
                            FROM wo_event_link wel
                            WHERE wel.mevt_headerno_i=wo_event_link.mevt_headerno_i
                              AND wel.pending_status = 0
                              AND wel.planable_status <= 0
                            ) AS planabel_event_perfno_i,
                        (
                            SELECT
                                wel.event_perfno_i
                            FROM wo_event_link wel
                            WHERE wel.mevt_headerno_i=wo_event_link.mevt_headerno_i
                              AND wel.pending_status = 0
                              AND wel.planable_status > 0
                            ) AS unplanabel_event_perfno_i,
                        wo_event_link.planable_status,
                        document_status.status_group
                    FROM (
                        SELECT
                            DISTINCT parrent.docno_i,
                            sub.docno,
                            sub.docno_i AS d_docno_i,
                            sub.revision,
                            sub.doc_type,
                            sub.lev,
                            applicability.applicable,
                            mevt_effectivity.applicable_status,
                            applicability.ref_key
                        FROM (
                            SELECT
                                LEVEL AS lev,
                                id    AS docno_i,
                                pid,
                                item.docno,
                                item.revision,
                                CONNECT_BY_ROOT item.docno_i AS root, item.write_off_logic,
                                doc_type
                            FROM (
                                SELECT
                                    main.id,
                                    CASE
                                        WHEN main.id = main.parrent_id THEN NULL
                                        ELSE main.parrent_id
                                    END AS pid,
                                    header.docno_i,
                                    header.docno,
                                    revision,
                                    write_off.write_off_logic,
                                    header.doc_type
                                FROM (
                                SELECT
                                    DISTINCT tree.docno_i AS id,
                                    CASE
                                        WHEN (
                                            SELECT
                                                COUNT(doc_signoff_tree.ref_docno_i) AS cnt
                                            FROM doc_signoff_tree
                                            WHERE tree.docno_i = doc_signoff_tree.ref_docno_i) > 0
                                            THEN tree.docno_i
                                       ELSE NULL
                                    END AS parrent_id,
                                    docno
                                FROM doc_signoff_tree tree
                                JOIN doc_header doc ON tree.docno_i = doc.docno_i
                                UNION
                                SELECT
                                    tree.ref_docno_i AS id,
                                    tree.docno_i     AS parrent_id,
                                    doc.docno
                                FROM doc_signoff_tree tree
                                JOIN doc_header doc ON tree.ref_docno_i = doc.docno_i) main
                                JOIN doc_header header ON main.id = header.docno_i
                                /* Document write-off logic */
                                LEFT JOIN (
                                    SELECT
                                        CASE
                                            WHEN COUNT(docno_i) > 0 THEN 'Y'
                                            ELSE 'N'
                                        END AS write_off_logic,
                                        docno_i
                                    FROM doc_signoff_tree
                                    WHERE docno_i = ref_docno_i
                                    GROUP BY docno_i
                                    ) write_off ON header.docno_i = write_off.docno_i
                                ) item CONNECT BY NOCYCLE PRIOR item.id = item.pid
                            ORDER SIBLINGS BY item.id, item.revision) sub
                        JOIN doc_header parrent ON sub.root = parrent.docno_i
                        JOIN event_effectivity_link ON parrent.docno_i = event_effectivity_link.event_key
                                                           AND event_effectivity_link.event_type = 'DO'
                        JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
                        JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                        LEFT JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                                     AND mevt_header.ref_type='AC_INT'
                                                     AND parrent.docno_i = mevt_header.mevt_key
                                                     AND mevt_header.mevt_type = 'DO'
                        LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                        WHERE pid IS NOT NULL
                        ) doc_sign_off
                    LEFT JOIN doc_signoff_tree ON doc_sign_off.d_docno_i = doc_signoff_tree.docno_i
                                                      AND doc_signoff_tree.docno_i = doc_signoff_tree.ref_docno_i
                    JOIN event_effectivity_link ON doc_signoff_tree.docno_i = event_effectivity_link.event_key
                                                       AND event_effectivity_link.event_type = 'DO'
                    JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
                    JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                                              AND doc_sign_off.ref_key = applicability.ref_key
                                              AND applicability.ref_type = 'AC_INT'
                    JOIN aircraft ON applicability.ref_key = aircraft.aircraftno_i
                    LEFT JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                                 AND mevt_header.ref_type='AC_INT'
                                                 AND doc_signoff_tree.docno_i = mevt_header.mevt_key
                                                 AND mevt_header.mevt_type = 'DO'
                    LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                    LEFT JOIN wo_event_link ON mevt_header.mevt_headerno_i = wo_event_link.mevt_headerno_i AND wo_event_link.pending_status = 0
                    LEFT JOIN document_status ON wo_event_link.event_status = document_status.code
                    WHERE doc_sign_off.docno_i = p_docno_i AND doc_sign_off.ref_key=p_aircraftno_i
                    ) now_event_perfno_i
                GROUP BY docno_i, aircraftno_i
            ) get_event_perfno_i;
        RETURN event_perfno_i;
    END;

/* ****************************************************************************
* Шаг 4.3. Функция для получения last event_perfno_i
**************************************************************************** */

CREATE OR REPLACE FUNCTION схема.GET_LAST_EVENT_PERFNO_I(
    p_docno_i IN NUMBER,
    p_aircraftno_i IN NUMBER
)
    RETURN NUMBER
    as last_event_perfno_i NUMBER(12);
        BEGIN
            SELECT
                CASE
                    WHEN get_last_event_perfno_i.unplanabel_status=1 AND get_last_event_perfno_i.planabel_status=0
                        THEN
                            CASE
                                WHEN get_last_event_perfno_i.unplanabel_event_close_group = 1 THEN get_last_event_perfno_i.max_unplanabel_event_perfno_i
                                ELSE 0
                            END
                    WHEN get_last_event_perfno_i.unplanabel_status=1 AND get_last_event_perfno_i.planabel_status=1
                        THEN
                            CASE
                                WHEN get_last_event_perfno_i.planabel_event_open_group=1 AND get_last_event_perfno_i.unplanabel_event_open_group=1
                                    THEN 0
                                ELSE get_last_event_perfno_i.max_unplanabel_event_perfno_i
                            END
                    WHEN get_last_event_perfno_i.unplanabel_status=0 AND get_last_event_perfno_i.planabel_status=1
                        THEN get_last_event_perfno_i.max_planabel_event_perfno_i
                    ELSE 0
                END INTO last_event_perfno_i
            FROM(
                SELECT
                    MAX(planabel_event_perfno_i) AS max_planabel_event_perfno_i,
                    MIN(planabel_event_perfno_i) AS min_planabel_event_perfno_i,
                    MAX(unplanabel_event_perfno_i) AS max_unplanabel_event_perfno_i,
                    MIN(unplanabel_event_perfno_i) AS min_unplanabel_event_perfno_i,
                    MAX(CASE WHEN planabel_status <= 0 THEN 1 ELSE 0 END) AS planabel_status,
                    MAX(CASE WHEN unplanabel_status > 0 THEN 1 ELSE 0 END)  AS unplanabel_status,
                    MAX(CASE WHEN planabel_event_status='Open' THEN 1 ELSE 0 END ) AS planabel_event_open_group,
                    MAX(CASE WHEN planabel_event_status='Closed' THEN 1 ELSE 0 END ) AS planabel_event_close_group,
                    MAX(CASE WHEN unplanabel_event_status='Open' THEN 1 ELSE 0 END) AS unplanabel_event_open_group,
                    MAX(CASE WHEN unplanabel_event_status='Closed' THEN 1 ELSE 0 END) AS unplanabel_event_close_group
                FROM (
                    SELECT
                        doc_sign_off.docno_i,
                        aircraft.aircraftno_i,
                        (
                            SELECT wel.event_perfno_i
                            FROM wo_event_link wel
                            WHERE wel.mevt_headerno_i = pend_status.mevt_headerno_i
                              AND wel.pending_status = pend_status.min_pending_status
                              AND wel.planable_status <= 0
                            ) AS planabel_event_perfno_i,
                        (
                            SELECT
                                document_status.status_group
                            FROM wo_event_link wel
                            LEFT JOIN document_status ON wel.event_status=document_status.code
                            WHERE wel.mevt_headerno_i=pend_status.mevt_headerno_i
                              AND wel.pending_status = pend_status.min_pending_status
                              AND wel.planable_status <= 0
                            ) AS planabel_event_status,
                        (
                            SELECT
                                wel.planable_status
                            FROM wo_event_link wel
                            WHERE wel.mevt_headerno_i = pend_status.mevt_headerno_i
                              AND wel.pending_status = pend_status.min_pending_status
                              AND wel.planable_status <= 0
                            ) AS planabel_status,
                        (
                            SELECT
                                wel.event_perfno_i
                            FROM wo_event_link wel
                            WHERE wel.mevt_headerno_i = pend_status.mevt_headerno_i
                              AND wel.pending_status = pend_status.min_pending_status
                              AND wel.planable_status > 0
                            )  AS unplanabel_event_perfno_i,
                        (
                            SELECT
                                wel.planable_status
                            FROM wo_event_link wel
                            WHERE wel.mevt_headerno_i = pend_status.mevt_headerno_i
                              AND wel.pending_status = pend_status.min_pending_status
                              AND wel.planable_status > 0
                            )  AS unplanabel_status,
                        (
                            SELECT
                                document_status.status_group
                            FROM wo_event_link wel
                            LEFT JOIN document_status ON wel.event_status=document_status.code
                            WHERE wel.mevt_headerno_i=pend_status.mevt_headerno_i
                              AND wel.pending_status = pend_status.min_pending_status
                              AND wel.planable_status > 0
                            ) AS unplanabel_event_status
                    FROM (
                        SELECT
                            DISTINCT parrent.docno_i,
                            sub.docno,
                            sub.docno_i AS d_docno_i,
                            sub.revision,
                            sub.doc_type,
                            sub.lev,
                            applicability.applicable,
                            mevt_effectivity.applicable_status,
                            applicability.ref_key
                        FROM (
                            SELECT
                                LEVEL AS lev,
                                id    AS docno_i,
                                pid,
                                item.docno,
                                item.revision,
                                CONNECT_BY_ROOT item.docno_i AS root, item.write_off_logic,
                                doc_type
                            FROM (
                                SELECT
                                    main.id,
                                    CASE
                                        WHEN main.id = main.parrent_id THEN NULL
                                        ELSE main.parrent_id
                                    END AS pid,
                                    header.docno_i,
                                    header.docno,
                                    revision,
                                    write_off.write_off_logic,
                                    header.doc_type
                                FROM (
                                SELECT
                                    DISTINCT tree.docno_i AS id,
                                    CASE
                                        WHEN (
                                            SELECT
                                                COUNT(doc_signoff_tree.ref_docno_i) AS cnt
                                            FROM doc_signoff_tree
                                            WHERE tree.docno_i = doc_signoff_tree.ref_docno_i) > 0
                                            THEN tree.docno_i
                                       ELSE NULL
                                    END AS parrent_id,
                                    docno
                                FROM doc_signoff_tree tree
                                JOIN doc_header doc ON tree.docno_i = doc.docno_i
                                UNION
                                SELECT
                                    tree.ref_docno_i AS id,
                                    tree.docno_i     AS parrent_id,
                                    doc.docno
                                FROM doc_signoff_tree tree
                                JOIN doc_header doc ON tree.ref_docno_i = doc.docno_i) main
                                JOIN doc_header header ON main.id = header.docno_i
                                /* Document write-off logic */
                                LEFT JOIN (
                                    SELECT
                                        CASE
                                            WHEN COUNT(docno_i) > 0 THEN 'Y'
                                            ELSE 'N'
                                        END AS write_off_logic,
                                        docno_i
                                    FROM doc_signoff_tree
                                    WHERE docno_i = ref_docno_i
                                    GROUP BY docno_i
                                    ) write_off ON header.docno_i = write_off.docno_i
                                ) item CONNECT BY NOCYCLE PRIOR item.id = item.pid
                            ORDER SIBLINGS BY item.id, item.revision) sub
                        JOIN doc_header parrent ON sub.root = parrent.docno_i
                        JOIN event_effectivity_link ON parrent.docno_i = event_effectivity_link.event_key
                                                           AND event_effectivity_link.event_type = 'DO'
                        JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
                        JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                        LEFT JOIN mevt_header ON mevt_header.ref_key='AC_INT'
                                                     AND applicability.ref_type = mevt_header.ref_type
                                                     AND parrent.docno_i = mevt_header.mevt_key
                                                     AND mevt_header.mevt_type = 'DO'
                        LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                        WHERE pid IS NOT NULL
                        ) doc_sign_off
                    LEFT JOIN doc_signoff_tree ON doc_sign_off.d_docno_i = doc_signoff_tree.docno_i
                                                      AND doc_signoff_tree.docno_i = doc_signoff_tree.ref_docno_i
                    JOIN event_effectivity_link ON doc_signoff_tree.docno_i = event_effectivity_link.event_key
                                                       AND event_effectivity_link.event_type = 'DO'
                    JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
                    JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                                              AND doc_sign_off.ref_key = applicability.ref_key
                                              AND applicability.ref_type = 'AC_INT'
                    JOIN aircraft ON applicability.ref_key = aircraft.aircraftno_i
                    LEFT JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                                 AND applicability.ref_type = mevt_header.ref_type
                                                 AND doc_signoff_tree.docno_i = mevt_header.mevt_key
                                                 AND mevt_header.mevt_type = 'DO'
                    LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                    LEFT JOIN (
                        SELECT
                            wo_event_link.mevt_headerno_i,
                            MIN(wo_event_link.pending_status) AS min_pending_status
                        FROM wo_event_link
                        WHERE wo_event_link.pending_status < 0
                        GROUP BY wo_event_link.mevt_headerno_i
                        ) pend_status ON mevt_header.mevt_headerno_i = pend_status.mevt_headerno_i
                    WHERE doc_sign_off.docno_i = p_docno_i AND doc_sign_off.ref_key=p_aircraftno_i
                    ) l_event_perfno_i
                GROUP BY docno_i, aircraftno_i
            ) get_last_event_perfno_i;
        RETURN last_event_perfno_i;
    END;

/* ****************************************************************************
* Шаг 4.3. Функция для получения текущего единого статуса
**************************************************************************** */

CREATE OR REPLACE FUNCTION схема.GET_EVENT_STATUS_SINGLE(
    p_docno_i IN NUMBER,
    p_aircraftno_i IN NUMBER
)
    RETURN VARCHAR2
    as event_status_single VARCHAR2(1);
BEGIN
    SELECT
        CASE
            WHEN single_status_event.applicable = 1 THEN
                CASE
                    WHEN single_status_event.applicable_status = 1 THEN
                        CASE
                            WHEN single_status_event.planable_status = 1 AND single_status_event.unplanable_status = 1 THEN
                                CASE
                                    WHEN single_status_event.open_group = 1 THEN single_status_event.open_state
                                    WHEN single_status_event.closing_group = 1 THEN single_status_event.closing_state
                                    ELSE 'N'
                                END
                            WHEN single_status_event.planable_status = 0 AND single_status_event.unplanable_status = 1 THEN
                                CASE
                                    WHEN single_status_event.closing_group = 1 THEN single_status_event.closing_state
                                    WHEN single_status_event.open_group = 1 THEN single_status_event.open_state
                                    ELSE  'N'
                                END
                            ELSE 'N'
                        END
                    ELSE 'N'
                END
            ELSE 'N'
        END INTO event_status_single
    FROM (
        SELECT
            MAX(
                CASE
                    WHEN doc_sign_off.APPLICABLE ='Y' THEN 1
                    ELSE 0
                END
            ) AS APPLICABLE,
            MAX(
                CASE
                    WHEN mevt_effectivity.applicable_status ='Y' THEN 1
                    ELSE 0
                END
            ) AS applicable_status,
            MAX(
                CASE
                    WHEN document_status.status_group='Open'  THEN 1
                    ELSE 0
                END
            ) AS open_group,
            MAX(
                CASE
                    WHEN document_status.status_group = 'Closed' THEN 1
                    ELSE 0
                END
            ) AS closing_group,
            MAX(
                CASE
                    WHEN document_status.status_group = 'Open' THEN document_status.code
                    ELSE null
                END
            ) AS open_state,
            MAX(
                CASE
                    WHEN document_status.status_group = 'Closed' THEN document_status.code
                    ELSE null
                END
            ) AS closing_state,
            MAX(
                CASE
                    WHEN wo_event_link.planable_status <= 0 THEN 1
                    ELSE 0
                END
            ) AS planable_status,
            MAX(
                CASE
                    WHEN wo_event_link.planable_status > 0 THEN 1
                    ELSE 0
                END
            ) AS unplanable_status
        FROM (
            SELECT DISTINCT
                parrent.docno_i,
                sub.docno,
                sub.docno_i AS d_docno_i,
                sub.revision,
                sub.doc_type,
                sub.lev,
                applicability.applicable,
                applicability.ref_key
            FROM (
                SELECT
                    LEVEL AS lev,
                    id AS docno_i,
                    pid,
                    item.docno,
                    item.revision,
                    CONNECT_BY_ROOT item.docno_i AS root,
                    item.write_off_logic,
                    doc_type
                FROM (
                    SELECT
                        main.id,
                        CASE
                            WHEN main.id = main.parrent_id THEN NULL
                            ELSE main.parrent_id
                        END AS pid,
                        header.docno_i,
                        header.docno,
                        revision,
                        write_off.write_off_logic,
                        header.doc_type
                    FROM (
                        SELECT DISTINCT
                            tree.docno_i AS id,
                            CASE
                                WHEN (
                                    SELECT COUNT(doc_signoff_tree.ref_docno_i) AS cnt
                                    FROM doc_signoff_tree
                                    WHERE tree.docno_i = doc_signoff_tree.ref_docno_i) > 0 THEN tree.docno_i
                                ELSE NULL
                            END AS parrent_id,
                            docno
                        FROM doc_signoff_tree tree
                        JOIN doc_header doc ON tree.docno_i = doc.docno_i
                        UNION
                        SELECT
                            tree.ref_docno_i AS id,
                            tree.docno_i AS parrent_id,
                            doc.docno
                        FROM doc_signoff_tree tree
                        JOIN doc_header doc ON tree.ref_docno_i = doc.docno_i
                        ) main
                    JOIN doc_header header ON main.id = header.docno_i
                        /* Document write-off logic */
                    LEFT JOIN (
                        SELECT
                            CASE
                                WHEN COUNT(docno_i) > 0 THEN 'Y'
                                ELSE 'N'
                            END AS write_off_logic,
                            docno_i
                        FROM doc_signoff_tree
                        WHERE docno_i = ref_docno_i
                        GROUP BY docno_i
                        ) write_off ON header.docno_i = write_off.docno_i
                    ) item
                CONNECT BY NOCYCLE PRIOR item.id = item.pid
                ORDER SIBLINGS BY item.id, item.revision
                ) sub
            JOIN doc_header parrent ON sub.root = parrent.docno_i
            JOIN event_effectivity_link ON parrent.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
            JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
            JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
            WHERE pid IS NOT NULL
            ) doc_sign_off
        LEFT JOIN doc_signoff_tree ON doc_sign_off.d_docno_i=doc_signoff_tree.docno_i AND doc_signoff_tree.docno_i = doc_signoff_tree.ref_docno_i
        JOIN event_effectivity_link ON doc_signoff_tree.docno_i = event_effectivity_link.event_key AND event_effectivity_link.event_type = 'DO'
        JOIN event_effectivity ON event_effectivity_link.effectivityno_i = event_effectivity.effectivityno_i
        JOIN applicability ON event_effectivity.effectivityno_i = applicability.effectivityno_i
                                  AND doc_sign_off.ref_key=applicability.ref_key
                                  AND applicability.ref_type = 'AC_INT'
        JOIN aircraft ON applicability.ref_key = aircraft.aircraftno_i
        LEFT JOIN mevt_header ON applicability.ref_key = mevt_header.ref_key
                                     AND applicability.ref_type = mevt_header.ref_type
                                     AND doc_signoff_tree.docno_i = mevt_header.mevt_key
                                     AND mevt_header.mevt_type = 'DO'
        LEFT JOIN mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
        LEFT JOIN wo_event_link ON mevt_header.mevt_headerno_i = wo_event_link.mevt_headerno_i
                                       AND wo_event_link.pending_status = 0
        LEFT JOIN document_status ON wo_event_link.event_status=document_status.code
        WHERE doc_sign_off.docno_i = p_docno_i AND doc_sign_off.ref_key = p_aircraftno_i
        GROUP BY doc_sign_off.docno_i, aircraft.aircraftno_i
        ) single_status_event;
    RETURN event_status_single;
END;

/* ****************************************************************************
 * Шаг 5. Процедура для заполнения таблицы event_effectivity_mapping_new
 * Входные параметры:
 * - p_docno_i - уникальный идентификатор документа
 * - p_aircraftno_i - уникальный идентификатор воздушного судна

**************************************************************************** */

CREATE OR REPLACE PROCEDURE update_event_mapping (
    p_docno_i        IN NUMBER,
    p_aircraftno_i   IN NUMBER
)
IS

    v_event_type              VARCHAR2(10);
    v_event_key_parent        NUMBER;
    v_affected                VARCHAR2(1);
    v_needs_recalculation     VARCHAR2(1);
    v_ac_registr              VARCHAR2(12);
    v_aircraftno_i            NUMBER;
    v_psn                     NUMBER;
    v_event_status            VARCHAR2(1);
    v_event_status_single     VARCHAR2(1);
    v_event_perfno_i          NUMBER;
    v_last_event_perfno_i     NUMBER;
    v_status                  NUMBER;
    v_effectivity_linkno_i    NUMBER;
BEGIN
    -- Получаем данные
    SELECT
        'EFL',
        dh.docno_i,
        CASE WHEN app.applicable IN ('X', 'N') THEN 'N' ELSE 'Y' END,
        COALESCE(NULL, 'N'),
        ac.ac_registr,
        ac.aircraftno_i,
        0,
        схема.GET_EVENT_STATUS(dh.docno_i, ac.aircraftno_i),
        схема.GET_EVENT_STATUS_SINGLE(dh.docno_i, ac.aircraftno_i),
        схема.get_event_perfno_I(dh.docno_i, ac.aircraftno_i),
        схема.GET_LAT_EVENT_PERFNO_I(dh.docno_i, ac.aircraftno_i),
        0,
        efl.effectivity_linkno_i
    INTO
        v_event_type,
        v_event_key_parent,
        v_affected,
        v_needs_recalculation,
        v_ac_registr,
        v_aircraftno_i,
        v_psn,
        v_event_status,
        v_event_status_single,
        v_event_perfno_i,
        v_last_event_perfno_i,
        v_status,
        v_effectivity_linkno_i
    FROM doc_header dh
    LEFT JOIN event_effectivity_link efl ON dh.docno_i = efl.event_key AND efl.event_type = 'DO'
    LEFT JOIN event_effectivity ef ON efl.effectivityno_i = ef.effectivityno_i
    LEFT JOIN applicability app ON ef.effectivityno_i = app.effectivityno_i AND app.ref_type = 'AC_INT'
    LEFT JOIN aircraft ac ON app.ref_key = ac.aircraftno_i
    WHERE dh.docno_i = p_docno_i
      AND ef.status = 0
      AND app.ref_key = p_aircraftno_i;

    -- Обновляем целевую таблицу
    UPDATE схема.event_effectivity_mapping_test eem
    SET
        eem.event_type = v_event_type,
        eem.affected = v_affected,
        eem.needs_recalculation = v_needs_recalculation,
        eem.ac_registr = v_ac_registr,
        eem.aircraftno_i=v_aircraftno_i,
        eem.psn = v_psn,
        eem.event_status = v_event_status,
        eem.event_status_single = v_event_status_single,
        eem.event_perfno_i = v_event_perfno_i,
        eem.last_event_perfno_i = v_last_event_perfno_i,
        eem.status = v_status,
        eem.effectivity_linkno_i = v_effectivity_linkno_i
    WHERE eem.event_key_parent = p_docno_i
      AND eem.aircraftno_i = p_aircraftno_i;

    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Ошибка: Запись для обновления не найдена');
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: Нет данных для обработки.');
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;

BEGIN
    схема.prepare_test_area(p_docno_i => :docno_i, p_aircraftno_i => :aircraftno_i);
    COMMIT;
END;


BEGIN
    схема.update_event_mapping(p_docno_i => :docno_i, p_aircraftno_i => :aircraftno_i);
    COMMIT;
END;



CREATE OR REPLACE VIEW схема.event_effectivity_mapping_test_check AS
    SELECT r.*
    FROM
        (
            SELECT
                'Test row' row_type,
                event_type,
                event_key_parent,
                affected,
                needs_recalculation,
                ac_registr,
                aircraftno_i,
                psn,
                event_status,
                last_event_perfno_i,
                event_perfno_i,
                effectivity_linkno_i
            FROM схема.event_effectivity_mapping_test r
            WHERE r.event_key_parent=:docno_i AND r.aircraftno_i=:aircraftno_i
            UNION
            SELECT
                'Original row' row_type,
                event_type,
                event_key_parent,
                affected,
                needs_recalculation,
                ac_registr,
                aircraftno_i,
                psn,
                event_status,
                last_event_perfno_i,
                event_perfno_i,
                effectivity_linkno_i
            FROM схема.event_effectivity_mapping r
            WHERE r.event_key_parent=:docno_i AND r.aircraftno_i=:aircraftno_i
            ) r
    ORDER BY r.event_key_parent, r.aircraftno_i, r.psn;


;

SELECT
    *
FROM event_effectivity_mapping_test_check
