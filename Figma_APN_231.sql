SELECT
    doc_header.docno,
    doc_header.doc_type,
    doc_header.revision,
    (
        SELECT compliance_type.code
        FROM compliance_type_link
        LEFT JOIN compliance_type ON compliance_type.compliance_typeno_i = compliance_type_link.compliance_typeno_i
        WHERE compliance_type_link.event_key = doc_header.docno_i
         AND compliance_type_link.event_type = 'DO'
        ) AS compliance,
    doc_header.ac_or_comp_doc,
    doc_header.ata_chapter,
    TRIM (TRAILING ' ' FROM doc_header.text) || ' ' || doc_header.text2,
    document_status.name AS status,
    CASE
        /* проверяю повторяющийся документ или нет */
        WHEN (
            SELECT
                  count(1)
            FROM схема..treq_time_requirement
            JOIN схема..treq_interval_group ON treq_time_requirement.timerequirementno_i=treq_interval_group.timerequirementno_i
            LEFT JOIN схема..treq_dimension_group ON treq_interval_group.interval_groupno_i=treq_dimension_group.interval_groupno_i
            LEFT JOIN схема..treq_interval ON treq_interval.dimension_groupno_i=treq_dimension_group.dimension_groupno_i
            LEFT JOIN схема..counter_definition ON counter_definition.counter_defno_i = treq_interval.counter_defno_i
            LEFT JOIN схема..treq_baseline ON treq_interval.intervalno_i = treq_baseline.intervalno_i
            LEFT JOIN схема..treq_baseline_threshold ON treq_baseline.baselineno_i = treq_baseline_threshold.baselineno_i
            WHERE treq_time_requirement.event_type = 'EFL'
              AND treq_time_requirement.status = 0
              AND treq_time_requirement.type = 'OP'
              AND схема..treq_interval.dimension_type='I'
              AND treq_time_requirement.event_key  = event_effectivity_mapping.effectivity_linkno_i
              AND (treq_interval_group.status  <> 2 OR treq_interval_group.status IS NULL)) > 0 THEN 'REP'
        ELSE null
    END AS REP,
    CASE
        WHEN COALESCE((
            SELECT count(1)
            FROM wo_transfer
            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.event_perfno_i
            GROUP BY wo_transfer.event_perfno_i
            ), 0) = 0
            THEN
                CASE
                    WHEN (
                        SELECT 1
                        FROM dual
                        WHERE EXISTS (
                            SELECT COUNT(1)
                            FROM wo_transfer
                            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                              AND wo_transfer.is_last_transfer='Y'
                              AND wo_transfer.transfer_type='R'
                            GROUP BY wo_transfer.event_perfno_i
                        )
                        )=1
                        THEN (
                            SELECT
                                TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+wo_transfer.absolute_due_date, 'dd.mm.yyyy')
                            FROM wo_transfer
                            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                              AND wo_transfer.is_last_transfer='Y'
                              AND wo_transfer.transfer_type='R'
                        )
                END
        WHEN event_effectivity_mapping.event_status='M'
            THEN (
                SELECT
                    TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+wo_transfer.absolute_due_date, 'dd.mm.yyyy')
                FROM msc_taskcard_version
                JOIN msc_operator_rev_req ON msc_taskcard_version.taskcard_verno_i = msc_operator_rev_req.taskcard_verno_i
                JOIN msc_requirement_tc_link ON msc_requirement_tc_link.req_linkno_i = msc_operator_rev_req.req_linkno_i
                JOIN msc_item ON msc_taskcard_version.taskcard_verno_i = msc_item.taskcard_verno_i
                JOIN msc_req_link_header ON msc_req_link_header.req_link_headerno_i = msc_requirement_tc_link.req_link_headerno_i
                JOIN msc_mp_link ON msc_requirement_tc_link.mpno_i_operator = msc_mp_link.mpno_i_operator
                JOIN msc_maintenance_program ON msc_mp_link.mpno_i_operator = msc_maintenance_program.mpno_i
                                                    AND msc_maintenance_program.program_type = 'OMP'
                JOIN msc_ac_configuration ON msc_maintenance_program.mpno_i = msc_ac_configuration.mpno_i
                                                 AND (
                                                     msc_ac_configuration.program_type = 'OMP'
                                                         AND
                                                     msc_ac_configuration.status = 0
                                                     )
                JOIN aircraft ON msc_ac_configuration.aircraftno_i = aircraft.aircraftno_i
                                                AND (
                                                    msc_maintenance_program.operated_by = aircraft.operator
                                                    )
                JOIN msc_operator_revision ON msc_operator_revision.op_revisionno_i=msc_operator_rev_req.op_revisionno_i
                JOIN схема..msc_rev_link ON msc_rev_link.revisionno_i_operator = msc_operator_revision.op_revisionno_i
                                                AND msc_rev_link.revisionno_i_requirement = msc_requirement_tc_link.revisionno_i_mrs
                                                AND msc_rev_link.type = 'MP'
                JOIN схема..mevt_header ON mevt_header.MEVT_TYPE = 'TCREQLIH'
                                                AND mevt_header.ref_key=aircraft.aircraftno_i
                                                AND mevt_header.MEVT_KEY = msc_req_link_header.req_link_headerno_i
                JOIN схема..mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                                AND mevt_effectivity.revision_type = 'TCREQLI'
                                                AND mevt_effectivity.revision_key = msc_requirement_tc_link.req_linkno_i
                JOIN схема..wo_event_link ON wo_event_link.mevt_headerno_i = mevt_header.mevt_headerno_i
                LEFT JOIN wo_transfer ON wo_event_link.event_perfno_i=wo_transfer.event_perfno_i
                WHERE msc_taskcard_version.taskcardno_i = (
                    SELECT
                        DISTINCT msc_item_header.taskcardno_i
                    FROM requirement_link
                    JOIN msc_req_link_header ON requirement_link.target = msc_req_link_header.req_link_headerno_i
                    JOIN msc_item_header ON msc_item_header.item_headerno_i = msc_req_link_header.item_headerno_i
                    JOIN msc_taskcard_version ON msc_item_header.taskcardno_i = msc_taskcard_version.taskcardno_i
                    WHERE requirement_link.source = event_effectivity_mapping.effectivity_linkno_i /*effectivity_linkno_i*/
                      AND requirement_link.source_mimetype = 'EFL'
                      AND requirement_link.target_mimetype = 'TCREQLIH'
                      AND requirement_link.code IN (
                                    'PD',
                                    'W'
                                   )
                        )
                  AND wo_transfer.is_last_transfer='Y'
                  AND wo_transfer.transfer_type='R'
                  AND EXISTS (
                    SELECT 1
                    FROM wo_event_link
                    JOIN event_effectivity_link ON wo_event_link.effectivity_linkno_i = event_effectivity_link.effectivity_linkno_i
                                                       AND (
                                                           wo_event_link.event_type = 'TCEFFLI'
                                                                AND
                                                           wo_event_link.pending_status = 0
                                                            )
                    JOIN wo_header ON wo_header.event_perfno_i = wo_event_link.event_perfno_i
                    WHERE wo_event_link.event_key_parent = msc_requirement_tc_link.req_linkno_i
                      AND wo_header.ac_registr = aircraft.ac_registr
                  )
                  AND aircraft.aircraftno_i = a.aircraftno_i
                  AND wo_event_link.pending_status = (
                  SELECT
                      MIN(wel.pending_status)
                  FROM wo_event_link wel
                  WHERE wel.mevt_headerno_i=wo_event_link.mevt_headerno_i
                  )
            )
        WHEN event_effectivity_mapping.event_status='R' THEN (
            SELECT
                TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+wo_transfer.absolute_due_date, 'dd.mm.yyyy')
            FROM wo_transfer
            WHERE wo_transfer.is_last_transfer='Y'
              AND wo_transfer.transfer_type='R'
              AND wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
            )
        ELSE null
    END AS perf_date,
    CASE
        WHEN COALESCE((
            SELECT COUNT(1)
            FROM wo_transfer
            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.event_perfno_i
            GROUP BY wo_transfer.event_perfno_i
            ), 0) = 0
            THEN
                CASE
                    WHEN (
                        SELECT 1
                        FROM dual
                        WHERE EXISTS (
                            SELECT 1
                            FROM wo_transfer
                            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                              AND wo_transfer.is_last_transfer='Y'
                              AND wo_transfer.transfer_type='R'
                        )
                        )=1
                        THEN (
                            SELECT
                                 TO_CHAR(TO_CHAR(TRUNC(TO_NUMBER(wo_transfer_dimension.due_at)/60),'FM999999999900')) || ':'
                                     ||TO_CHAR(TRUNC(MOD(TO_NUMBER(wo_transfer_dimension.due_at),60)),'FM00')
                            FROM wo_transfer
                            JOIN wo_transfer_dimension ON wo_transfer.event_transferno_i=wo_transfer_dimension.event_transferno_i
                            JOIN counter ON wo_transfer_dimension.counterno_i=counter.counterno_i
                            JOIN counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
                            JOIN counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
                            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                              AND wo_transfer.is_last_transfer='Y'
                              AND wo_transfer.transfer_type='R'
                              AND counter_definition.code='H'
                        )
                END
        WHEN event_effectivity_mapping.event_status='M'
            THEN (
                SELECT
                    TO_CHAR(TO_CHAR(TRUNC(TO_NUMBER(wo_transfer_dimension.due_at)/60),'FM999999999900')) || ':'
                                     ||TO_CHAR(TRUNC(MOD(TO_NUMBER(wo_transfer_dimension.due_at),60)),'FM00')
                FROM msc_taskcard_version
                JOIN msc_operator_rev_req ON msc_taskcard_version.taskcard_verno_i = msc_operator_rev_req.taskcard_verno_i
                JOIN msc_requirement_tc_link ON msc_requirement_tc_link.req_linkno_i = msc_operator_rev_req.req_linkno_i
                JOIN msc_item ON msc_taskcard_version.taskcard_verno_i = msc_item.taskcard_verno_i
                JOIN msc_req_link_header ON msc_req_link_header.req_link_headerno_i = msc_requirement_tc_link.req_link_headerno_i
                JOIN msc_mp_link ON msc_requirement_tc_link.mpno_i_operator = msc_mp_link.mpno_i_operator
                JOIN msc_maintenance_program ON msc_mp_link.mpno_i_operator = msc_maintenance_program.mpno_i
                                                    AND msc_maintenance_program.program_type = 'OMP'
                JOIN msc_ac_configuration ON msc_maintenance_program.mpno_i = msc_ac_configuration.mpno_i
                                                 AND (
                                                     msc_ac_configuration.program_type = 'OMP'
                                                         AND
                                                     msc_ac_configuration.status = 0
                                                     )
                JOIN aircraft ON msc_ac_configuration.aircraftno_i = aircraft.aircraftno_i
                                                AND (
                                                    msc_maintenance_program.operated_by = aircraft.operator
                                                    )
                JOIN msc_operator_revision ON msc_operator_revision.op_revisionno_i=msc_operator_rev_req.op_revisionno_i
                JOIN схема..msc_rev_link ON msc_rev_link.revisionno_i_operator = msc_operator_revision.op_revisionno_i
                                                AND msc_rev_link.revisionno_i_requirement = msc_requirement_tc_link.revisionno_i_mrs
                                                AND msc_rev_link.type = 'MP'
                JOIN схема..mevt_header ON mevt_header.MEVT_TYPE = 'TCREQLIH'
                                                AND mevt_header.ref_key=aircraft.aircraftno_i
                                                AND mevt_header.MEVT_KEY = msc_req_link_header.req_link_headerno_i
                JOIN схема..mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                                AND mevt_effectivity.revision_type = 'TCREQLI'
                                                AND mevt_effectivity.revision_key = msc_requirement_tc_link.req_linkno_i
                JOIN схема..wo_event_link ON wo_event_link.mevt_headerno_i = mevt_header.mevt_headerno_i
                JOIN wo_transfer ON wo_event_link.event_perfno_i=wo_transfer.event_perfno_i
                JOIN wo_transfer_dimension ON wo_transfer.event_transferno_i=wo_transfer_dimension.event_transferno_i
                JOIN counter ON wo_transfer_dimension.counterno_i=counter.counterno_i
                JOIN counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
                JOIN counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
                WHERE msc_taskcard_version.taskcardno_i = (
                    SELECT
                        DISTINCT msc_item_header.taskcardno_i
                    FROM requirement_link
                    JOIN msc_req_link_header ON requirement_link.target = msc_req_link_header.req_link_headerno_i
                    JOIN msc_item_header ON msc_item_header.item_headerno_i = msc_req_link_header.item_headerno_i
                    JOIN msc_taskcard_version ON msc_item_header.taskcardno_i = msc_taskcard_version.taskcardno_i
                    WHERE requirement_link.source = event_effectivity_mapping.effectivity_linkno_i /*effectivity_linkno_i*/
                      AND requirement_link.source_mimetype = 'EFL'
                      AND requirement_link.target_mimetype = 'TCREQLIH'
                      AND requirement_link.code IN (
                                    'PD',
                                    'W'
                                   )
                        )
                  AND wo_transfer.is_last_transfer='Y'
                  AND wo_transfer.transfer_type='R'
                  AND counter_definition.code='H'
                  AND EXISTS (
                    SELECT 1
                    FROM wo_event_link
                    JOIN event_effectivity_link ON wo_event_link.effectivity_linkno_i = event_effectivity_link.effectivity_linkno_i
                                                       AND (
                                                           wo_event_link.event_type = 'TCEFFLI'
                                                                AND
                                                           wo_event_link.pending_status = 0
                                                            )
                    JOIN wo_header ON wo_header.event_perfno_i = wo_event_link.event_perfno_i
                    WHERE wo_event_link.event_key_parent = msc_requirement_tc_link.req_linkno_i
                      AND wo_header.ac_registr = aircraft.ac_registr
                  )
                  AND aircraft.aircraftno_i = a.aircraftno_i
                  AND wo_event_link.pending_status = (
                    SELECT
                      MIN(wel.pending_status)
                    FROM wo_event_link wel
                    WHERE wel.mevt_headerno_i=wo_event_link.mevt_headerno_i
                  )
            )
        WHEN event_effectivity_mapping.event_status='R' THEN (
            SELECT
                TO_CHAR(TO_CHAR(TRUNC(TO_NUMBER(wo_transfer_dimension.due_at)/60),'FM999999999900')) || ':'
                                     ||TO_CHAR(TRUNC(MOD(TO_NUMBER(wo_transfer_dimension.due_at),60)),'FM00')
            FROM wo_transfer
            JOIN wo_transfer_dimension ON wo_transfer.event_transferno_i=wo_transfer_dimension.event_transferno_i
            JOIN counter ON wo_transfer_dimension.counterno_i=counter.counterno_i
            JOIN counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
            JOIN counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
            WHERE wo_transfer.is_last_transfer='Y'
              AND wo_transfer.transfer_type='R'
              AND wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
              AND counter_definition.code='H'
            )
        ELSE null
    END AS TAH_TSN,
    CASE
        WHEN COALESCE((
            SELECT COUNT(1)
            FROM wo_transfer
            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.event_perfno_i
            ), 0) = 0
            THEN
                CASE
                    WHEN (
                        SELECT 1
                        FROM dual
                        WHERE EXISTS (
                            SELECT 1
                            FROM wo_transfer
                            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                              AND wo_transfer.is_last_transfer='Y'
                              AND wo_transfer.transfer_type='R'
                        )
                        )=1
                        THEN (
                            SELECT
                                 wo_transfer_dimension.due_at
                            FROM wo_transfer
                            JOIN wo_transfer_dimension ON wo_transfer.event_transferno_i=wo_transfer_dimension.event_transferno_i
                            JOIN counter ON wo_transfer_dimension.counterno_i=counter.counterno_i
                            JOIN counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
                            JOIN counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
                            WHERE wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                              AND wo_transfer.is_last_transfer='Y'
                              AND wo_transfer.transfer_type='R'
                              AND counter_definition.code='C'
                        )
                END
        WHEN event_effectivity_mapping.event_status='M'
            THEN (
                SELECT
                    wo_transfer_dimension.due_at
                FROM msc_taskcard_version
                JOIN msc_operator_rev_req ON msc_taskcard_version.taskcard_verno_i = msc_operator_rev_req.taskcard_verno_i
                JOIN msc_requirement_tc_link ON msc_requirement_tc_link.req_linkno_i = msc_operator_rev_req.req_linkno_i
                JOIN msc_item ON msc_taskcard_version.taskcard_verno_i = msc_item.taskcard_verno_i
                JOIN msc_req_link_header ON msc_req_link_header.req_link_headerno_i = msc_requirement_tc_link.req_link_headerno_i
                JOIN msc_mp_link ON msc_requirement_tc_link.mpno_i_operator = msc_mp_link.mpno_i_operator
                JOIN msc_maintenance_program ON msc_mp_link.mpno_i_operator = msc_maintenance_program.mpno_i
                                                    AND msc_maintenance_program.program_type = 'OMP'
                JOIN msc_ac_configuration ON msc_maintenance_program.mpno_i = msc_ac_configuration.mpno_i
                                                 AND (
                                                     msc_ac_configuration.program_type = 'OMP'
                                                         AND
                                                     msc_ac_configuration.status = 0
                                                     )
                JOIN aircraft ON msc_ac_configuration.aircraftno_i = aircraft.aircraftno_i
                                                AND (
                                                    msc_maintenance_program.operated_by = aircraft.operator
                                                    )
                JOIN msc_operator_revision ON msc_operator_revision.op_revisionno_i=msc_operator_rev_req.op_revisionno_i
                JOIN схема..msc_rev_link ON msc_rev_link.revisionno_i_operator = msc_operator_revision.op_revisionno_i
                                                AND msc_rev_link.revisionno_i_requirement = msc_requirement_tc_link.revisionno_i_mrs
                                                AND msc_rev_link.type = 'MP'
                JOIN схема..mevt_header ON mevt_header.MEVT_TYPE = 'TCREQLIH'
                                                AND mevt_header.ref_key=aircraft.aircraftno_i
                                                AND mevt_header.MEVT_KEY = msc_req_link_header.req_link_headerno_i
                JOIN схема..mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                                AND mevt_effectivity.revision_type = 'TCREQLI'
                                                AND mevt_effectivity.revision_key = msc_requirement_tc_link.req_linkno_i
                JOIN схема..wo_event_link ON wo_event_link.mevt_headerno_i = mevt_header.mevt_headerno_i
                JOIN wo_transfer ON wo_event_link.event_perfno_i=wo_transfer.event_perfno_i
                JOIN wo_transfer_dimension ON wo_transfer.event_transferno_i=wo_transfer_dimension.event_transferno_i
                JOIN counter ON wo_transfer_dimension.counterno_i=counter.counterno_i
                JOIN counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
                JOIN counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
                WHERE msc_taskcard_version.taskcardno_i = (
                    SELECT
                        DISTINCT msc_item_header.taskcardno_i
                    FROM requirement_link
                    JOIN msc_req_link_header ON requirement_link.target = msc_req_link_header.req_link_headerno_i
                    JOIN msc_item_header ON msc_item_header.item_headerno_i = msc_req_link_header.item_headerno_i
                    JOIN msc_taskcard_version ON msc_item_header.taskcardno_i = msc_taskcard_version.taskcardno_i
                    WHERE requirement_link.source = event_effectivity_mapping.effectivity_linkno_i /*effectivity_linkno_i*/
                      AND requirement_link.source_mimetype = 'EFL'
                      AND requirement_link.target_mimetype = 'TCREQLIH'
                      AND requirement_link.code IN (
                                    'PD',
                                    'W'
                                   )
                        )
                  AND wo_transfer.is_last_transfer='Y'
                  AND wo_transfer.transfer_type='R'
                  AND counter_definition.code='C'
                  AND EXISTS (
                    SELECT 1
                    FROM wo_event_link
                    JOIN event_effectivity_link ON wo_event_link.effectivity_linkno_i = event_effectivity_link.effectivity_linkno_i
                                                       AND (
                                                           wo_event_link.event_type = 'TCEFFLI'
                                                                AND
                                                           wo_event_link.pending_status = 0
                                                            )
                    JOIN wo_header ON wo_header.event_perfno_i = wo_event_link.event_perfno_i
                    WHERE wo_event_link.event_key_parent = msc_requirement_tc_link.req_linkno_i
                      AND wo_header.ac_registr = aircraft.ac_registr
                  )
                  AND aircraft.aircraftno_i = a.aircraftno_i
                  AND wo_event_link.pending_status = (
                    SELECT
                      MIN(wel.pending_status)
                    FROM wo_event_link wel
                    WHERE wel.mevt_headerno_i=wo_event_link.mevt_headerno_i
                  )
            )
        WHEN event_effectivity_mapping.event_status='R' THEN (
            SELECT wo_transfer_dimension.due_at
            FROM wo_transfer
            JOIN wo_transfer_dimension ON wo_transfer.event_transferno_i=wo_transfer_dimension.event_transferno_i
            JOIN counter ON wo_transfer_dimension.counterno_i=counter.counterno_i
            JOIN counter_template ON counter.counter_templateno_i=counter_template.counter_templateno_i
            JOIN counter_definition ON counter_template.counter_defno_i=counter_definition.counter_defno_i
            WHERE wo_transfer.is_last_transfer='Y'
              AND wo_transfer.transfer_type='R'
              AND wo_transfer.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
              AND counter_definition.code='C'
            )
        ELSE null
    END AS CSN,
    CASE
        WHEN event_effectivity_mapping.event_status='C' AND COALESCE((
                SELECT
                    wo_header.workorderno_display
                FROM wo_header
                WHERE wo_header.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                ), 0) > 0
            THEN (
                SELECT
                    wo_header.workorderno_display
                FROM wo_header
                WHERE wo_header.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                )
        WHEN event_effectivity_mapping.event_status='C' AND (
                SELECT
                    wo_header.type
                FROM wo_header
                WHERE wo_header.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                ) <>'PD'
            THEN (
                SELECT
                    wo_header.workorderno_display
                FROM wo_header
                WHERE wo_header.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
                )
        WHEN event_effectivity_mapping.event_status='M'
            THEN (
                SELECT
                   wo_header.workorderno_display
                FROM msc_taskcard_version
                JOIN msc_operator_rev_req ON msc_taskcard_version.taskcard_verno_i = msc_operator_rev_req.taskcard_verno_i
                JOIN msc_requirement_tc_link ON msc_requirement_tc_link.req_linkno_i = msc_operator_rev_req.req_linkno_i
                JOIN msc_item ON msc_taskcard_version.taskcard_verno_i = msc_item.taskcard_verno_i
                JOIN msc_req_link_header ON msc_req_link_header.req_link_headerno_i = msc_requirement_tc_link.req_link_headerno_i
                JOIN msc_mp_link ON msc_requirement_tc_link.mpno_i_operator = msc_mp_link.mpno_i_operator
                JOIN msc_maintenance_program ON msc_mp_link.mpno_i_operator = msc_maintenance_program.mpno_i
                                                    AND msc_maintenance_program.program_type = 'OMP'
                JOIN msc_ac_configuration ON msc_maintenance_program.mpno_i = msc_ac_configuration.mpno_i
                                                 AND (
                                                     msc_ac_configuration.program_type = 'OMP'
                                                         AND
                                                     msc_ac_configuration.status = 0
                                                     )
                JOIN aircraft ON msc_ac_configuration.aircraftno_i = aircraft.aircraftno_i
                                                AND (
                                                    msc_maintenance_program.operated_by = aircraft.operator
                                                    )
                JOIN msc_operator_revision ON msc_operator_revision.op_revisionno_i=msc_operator_rev_req.op_revisionno_i
                JOIN схема..msc_rev_link ON msc_rev_link.revisionno_i_operator = msc_operator_revision.op_revisionno_i
                                                AND msc_rev_link.revisionno_i_requirement = msc_requirement_tc_link.revisionno_i_mrs
                                                AND msc_rev_link.type = 'MP'
                JOIN схема..mevt_header ON mevt_header.MEVT_TYPE = 'TCREQLIH'
                                                AND mevt_header.ref_key=aircraft.aircraftno_i
                                                AND mevt_header.MEVT_KEY = msc_req_link_header.req_link_headerno_i
                JOIN схема..mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                                AND mevt_effectivity.revision_type = 'TCREQLI'
                                                AND mevt_effectivity.revision_key = msc_requirement_tc_link.req_linkno_i
                JOIN схема..wo_event_link ON wo_event_link.mevt_headerno_i = mevt_header.mevt_headerno_i
                LEFT JOIN wo_header ON wo_event_link.event_perfno_i=wo_header.event_perfno_i
                WHERE msc_taskcard_version.taskcardno_i = (
                    SELECT
                        DISTINCT msc_item_header.taskcardno_i
                    FROM requirement_link
                    JOIN msc_req_link_header ON requirement_link.target = msc_req_link_header.req_link_headerno_i
                    JOIN msc_item_header ON msc_item_header.item_headerno_i = msc_req_link_header.item_headerno_i
                    JOIN msc_taskcard_version ON msc_item_header.taskcardno_i = msc_taskcard_version.taskcardno_i
                    WHERE requirement_link.source = event_effectivity_mapping.effectivity_linkno_i /*effectivity_linkno_i*/
                      AND requirement_link.source_mimetype = 'EFL'
                      AND requirement_link.target_mimetype = 'TCREQLIH'
                      AND requirement_link.code IN (
                                    'PD',
                                    'W'
                                   )
                        )
                  AND EXISTS (
                    SELECT 1
                    FROM wo_event_link
                    JOIN event_effectivity_link ON wo_event_link.effectivity_linkno_i = event_effectivity_link.effectivity_linkno_i
                                                       AND (
                                                           wo_event_link.event_type = 'TCEFFLI'
                                                                AND
                                                           wo_event_link.pending_status = 0
                                                            )
                    JOIN wo_header ON wo_header.event_perfno_i = wo_event_link.event_perfno_i
                    WHERE wo_event_link.event_key_parent = msc_requirement_tc_link.req_linkno_i
                      AND wo_header.ac_registr = aircraft.ac_registr
                  )
                  AND aircraft.aircraftno_i = a.aircraftno_i
                  AND wo_event_link.pending_status = (
                  SELECT
                      MIN(wel.pending_status)
                  FROM wo_event_link wel
                  WHERE wel.mevt_headerno_i=wo_event_link.mevt_headerno_i
                  )
            )
        WHEN event_effectivity_mapping.event_status='R' AND (
                SELECT
                    type
                FROM wo_header
                WHERE wo_header.event_perfno_i=event_effectivity_mapping.last_event_perfno_i
            ) <> 'PD'
            THEN event_effectivity_mapping.event_perfno_i
        ELSE null
    END AS WO,
    a.ac_registr,
    CASE
        WHEN event_effectivity_mapping.event_status='M'
            THEN (
                SELECT
                   TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+wo_transfer.absolute_due_date, 'dd.mm.yyyy')
                FROM msc_taskcard_version
                JOIN msc_operator_rev_req ON msc_taskcard_version.taskcard_verno_i = msc_operator_rev_req.taskcard_verno_i
                JOIN msc_requirement_tc_link ON msc_requirement_tc_link.req_linkno_i = msc_operator_rev_req.req_linkno_i
                JOIN msc_item ON msc_taskcard_version.taskcard_verno_i = msc_item.taskcard_verno_i
                JOIN msc_req_link_header ON msc_req_link_header.req_link_headerno_i = msc_requirement_tc_link.req_link_headerno_i
                JOIN msc_mp_link ON msc_requirement_tc_link.mpno_i_operator = msc_mp_link.mpno_i_operator
                JOIN msc_maintenance_program ON msc_mp_link.mpno_i_operator = msc_maintenance_program.mpno_i
                                                    AND msc_maintenance_program.program_type = 'OMP'
                JOIN msc_ac_configuration ON msc_maintenance_program.mpno_i = msc_ac_configuration.mpno_i
                                                 AND (
                                                     msc_ac_configuration.program_type = 'OMP'
                                                         AND
                                                     msc_ac_configuration.status = 0
                                                     )
                JOIN aircraft ON msc_ac_configuration.aircraftno_i = aircraft.aircraftno_i
                                                AND (
                                                    msc_maintenance_program.operated_by = aircraft.operator
                                                    )
                JOIN msc_operator_revision ON msc_operator_revision.op_revisionno_i=msc_operator_rev_req.op_revisionno_i
                JOIN схема..msc_rev_link ON msc_rev_link.revisionno_i_operator = msc_operator_revision.op_revisionno_i
                                                AND msc_rev_link.revisionno_i_requirement = msc_requirement_tc_link.revisionno_i_mrs
                                                AND msc_rev_link.type = 'MP'
                JOIN схема..mevt_header ON mevt_header.MEVT_TYPE = 'TCREQLIH'
                                                AND mevt_header.ref_key=aircraft.aircraftno_i
                                                AND mevt_header.MEVT_KEY = msc_req_link_header.req_link_headerno_i
                JOIN схема..mevt_effectivity ON mevt_header.mevt_headerno_i = mevt_effectivity.mevt_headerno_i
                                                AND mevt_effectivity.revision_type = 'TCREQLI'
                                                AND mevt_effectivity.revision_key = msc_requirement_tc_link.req_linkno_i
                JOIN схема..wo_event_link ON wo_event_link.mevt_headerno_i = mevt_header.mevt_headerno_i
                LEFT JOIN wo_transfer ON wo_event_link.event_perfno_i=wo_transfer.event_perfno_i
                LEFT JOIN wo_header ON wo_event_link.event_perfno_i=wo_header.event_perfno_i
                WHERE msc_taskcard_version.taskcardno_i = (
                    SELECT
                        DISTINCT msc_item_header.taskcardno_i
                    FROM requirement_link
                    JOIN msc_req_link_header ON requirement_link.target = msc_req_link_header.req_link_headerno_i
                    JOIN msc_item_header ON msc_item_header.item_headerno_i = msc_req_link_header.item_headerno_i
                    JOIN msc_taskcard_version ON msc_item_header.taskcardno_i = msc_taskcard_version.taskcardno_i
                    WHERE requirement_link.source = event_effectivity_mapping.effectivity_linkno_i /*effectivity_linkno_i*/
                      AND requirement_link.source_mimetype = 'EFL'
                      AND requirement_link.target_mimetype = 'TCREQLIH'
                      AND requirement_link.code IN (
                                    'PD',
                                    'W'
                                   )
                        )
                  AND wo_transfer.is_last_transfer='Y'
                  AND EXISTS (
                    SELECT 1
                    FROM wo_event_link
                    JOIN event_effectivity_link ON wo_event_link.effectivity_linkno_i = event_effectivity_link.effectivity_linkno_i
                                                       AND (
                                                           wo_event_link.event_type = 'TCEFFLI'
                                                                AND
                                                           wo_event_link.pending_status = 0
                                                            )
                    JOIN wo_header ON wo_header.event_perfno_i = wo_event_link.event_perfno_i
                    WHERE wo_event_link.event_key_parent = msc_requirement_tc_link.req_linkno_i
                      AND wo_header.ac_registr = aircraft.ac_registr
                  )
                  AND aircraft.aircraftno_i = a.aircraftno_i
                  AND wo_event_link.pending_status = 0
            )
        WHEN event_effectivity_mapping.event_status='R'
            THEN (
                SELECT
                    TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+forecast.expected_date, 'dd.mm.yyyy')
                FROM forecast
                WHERE forecast.event_perfno_i=event_effectivity_mapping.event_perfno_i
            )
        ELSE
            CASE
                WHEN (
                    SELECT
                        expected_date
                    FROM forecast
                    WHERE forecast.event_perfno_i=event_effectivity_mapping.event_perfno_i
                    ) <> 1491308 OR
                     COALESCE((
                        SELECT
                            expected_date
                        FROM forecast
                        WHERE forecast.event_perfno_i=event_effectivity_mapping.event_perfno_i
                        ), 0)=0
                    THEN (
                        SELECT
                            TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+forecast.expected_date, 'dd.mm.yyyy')
                        FROM forecast
                        WHERE forecast.event_perfno_i=event_effectivity_mapping.event_perfno_i
                        )
                ELSE 'Undefined'
            END
    END AS due_date,
    TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+doc_header.issue_date, 'dd.mm.yyyy') AS issue_date,
    CASE
        WHEN doc_header.rev_date <> 0 THEN TO_CHAR(TO_DATE('31.12.1971', 'dd.mm.yyyy')+doc_header.rev_date, 'dd.mm.yyyy')
        ELSE null
    END AS rev_date,
    LAST_EVENT_PERFNO_I,
    EVENT_PERFNO_I,
    mevt_header.mevt_headerno_i
FROM aircraft a
JOIN event_effectivity_mapping ON a.aircraftno_i=event_effectivity_mapping.aircraftno_i
JOIN doc_header ON event_effectivity_mapping.event_key_parent=doc_header.docno_i
LEFT JOIN mevt_header ON a.aircraftno_i = mevt_header.ref_key
                                                 AND mevt_header.ref_type='AC_INT'
                                                 AND doc_header.docno_i = mevt_header.mevt_key
                                                 AND mevt_header.mevt_type = 'DO'
JOIN document_status ON event_effectivity_mapping.event_status=document_status.code
WHERE a.ac_registr='73094'
  AND doc_header.ac_or_comp_doc='A'
  AND doc_header.release_state NOT IN (
         'U',
         'I',
         'P'
      )
;

SELECT
    doc_header.docno,
    doc_header.doc_type,
    doc_header.REVISION,
    a.ac_registr,
    rotables.PARTNO,
    rotables.serialno,
    event_effectivity_mapping.EVENT_STATUS
FROM aircraft a
JOIN rotables ON a.ac_registr=rotables.ac_registr
LEFT JOIN event_effectivity_mapping ON a.aircraftno_i=event_effectivity_mapping.aircraftno_i
                                      AND rotables.psn=event_effectivity_mapping.psn
JOIN doc_header ON event_effectivity_mapping.event_key_parent=doc_header.docno_i
JOIN document_status ON event_effectivity_mapping.event_status=document_status.code
WHERE a.ac_registr='73094'
  AND doc_header.release_state NOT IN (
         'U',
         'I',
         'P'
      )
;

SELECT
    *
FROM doc_header
JOIN event_effectivity_link ON doc_header.docno_i=event_effectivity_link.event_key
                                   AND event_effectivity_link.event_type='DO'
JOIN event_effectivity ON event_effectivity_link.effectivityno_i=event_effectivity.effectivityno_i
JOIN event_effectivity_parts ON event_effectivity.effectivityno_i=event_effectivity_parts.effectivityno_i
JOIN event_effectivity_sns ON event_effectivity_parts.effectivityno_i=event_effectivity_sns.EFFECTIVITYNO_I
JOIN event_effectivity_rules ON event_effectivity_sns.EFFECTIVITYNO_I=event_effectivity_rules.effectivityno_i
-- JOIN applicability ON event_effectivity.EFFECTIVITYNO_I=event_effectivity.EFFECTIVITYNO_I
WHERE doc_header.docno_i=29495

;

