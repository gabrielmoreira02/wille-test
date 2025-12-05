-- Agrarmonitor Data Replication - 90 Days
-- Paste this AFTER importing base data
-- This replicates your base data across 90 days using database-native operations

BEGIN;

DO $$
DECLARE
    days_to_generate INT := 90;
    end_date DATE := CURRENT_DATE;
    start_date DATE := end_date - days_to_generate;
    day_offset INT;
    current_date DATE;
    
    tankbuch_count INT := 0;
    betriebsstoffe_count INT := 0;
    arbeitszeit_count INT := 0;
    rechnungen_count INT := 0;
    base_tankbuch INT;
    base_betriebsstoffe INT;
    base_arbeitszeit INT;
    base_rechnungen INT;
BEGIN
    RAISE NOTICE '🔄 Replicating Agrarmonitor data for 90 days...';
    RAISE NOTICE '📅 Date range: % → %', start_date, end_date;
    
    -- Get base counts
    SELECT COUNT(*) INTO base_tankbuch FROM tankbuch WHERE date::date = (SELECT MAX(date::date) FROM tankbuch);
    SELECT COUNT(*) INTO base_betriebsstoffe FROM betriebsstoffe WHERE datum = (SELECT MAX(datum) FROM betriebsstoffe);
    SELECT COUNT(*) INTO base_arbeitszeit FROM maschinenkosten_arbeitszeit WHERE datum = (SELECT MAX(datum) FROM maschinenkosten_arbeitszeit);
    SELECT COUNT(*) INTO base_rechnungen FROM maschinenkosten_rechnungen WHERE datum = (SELECT MAX(datum) FROM maschinenkosten_rechnungen);
    
    RAISE NOTICE '';
    RAISE NOTICE 'Base data per day:';
    RAISE NOTICE '  tankbuch: %', base_tankbuch;
    RAISE NOTICE '  betriebsstoffe: %', base_betriebsstoffe;
    RAISE NOTICE '  maschinenkosten_arbeitszeit: %', base_arbeitszeit;
    RAISE NOTICE '  maschinenkosten_rechnungen: %', base_rechnungen;
    RAISE NOTICE '';
    
    -- Create temp table for dates
    CREATE TEMP TABLE date_range (target_date DATE);
    FOR day_offset IN 0..days_to_generate-1 LOOP
        INSERT INTO date_range VALUES (end_date - day_offset);
    END LOOP;
    
    RAISE NOTICE '📊 Replicating...';
    
    -- TANKBUCH
    INSERT INTO tankbuch (
        external_id, date, date_formatted, machine, machine_name, machine_kennzeichen,
        equipment_id, tankstelle, mitarbeiter, zaehlerstand, menge, durchschnitt,
        created_at, updated_at
    )
    SELECT 
        t.external_id || '_' || dr.target_date::text,
        dr.target_date::timestamp,
        TO_CHAR(dr.target_date, 'DD.MM.YYYY'),
        t.machine, t.machine_name, t.machine_kennzeichen, t.equipment_id,
        t.tankstelle, t.mitarbeiter, t.zaehlerstand, t.menge, t.durchschnitt,
        dr.target_date::timestamp,
        dr.target_date::timestamp
    FROM tankbuch t
    CROSS JOIN date_range dr
    WHERE t.date::date = (SELECT MAX(date::date) FROM tankbuch)
    AND NOT EXISTS (
        SELECT 1 FROM tankbuch t2 
        WHERE t2.external_id = t.external_id || '_' || dr.target_date::text
    );
    
    GET DIAGNOSTICS tankbuch_count = ROW_COUNT;
    RAISE NOTICE '  ✓ tankbuch: % records', tankbuch_count;
    
    -- BETRIEBSSTOFFE
    INSERT INTO betriebsstoffe (
        external_id, datum, lieferant, artikel, beschreibung, menge, einheit,
        einzelpreis, gesamtpreis, rechnung_nummer, lieferschein_nummer,
        created_at, updated_at
    )
    SELECT 
        b.external_id || '_' || dr.target_date::text,
        dr.target_date,
        b.lieferant, b.artikel, b.beschreibung, b.menge, b.einheit,
        b.einzelpreis, b.gesamtpreis, b.rechnung_nummer, b.lieferschein_nummer,
        dr.target_date::timestamp,
        dr.target_date::timestamp
    FROM betriebsstoffe b
    CROSS JOIN date_range dr
    WHERE b.datum = (SELECT MAX(datum) FROM betriebsstoffe)
    AND NOT EXISTS (
        SELECT 1 FROM betriebsstoffe b2 
        WHERE b2.external_id = b.external_id || '_' || dr.target_date::text
    );
    
    GET DIAGNOSTICS betriebsstoffe_count = ROW_COUNT;
    RAISE NOTICE '  ✓ betriebsstoffe: % records', betriebsstoffe_count;
    
    -- MASCHINENKOSTEN_ARBEITSZEIT
    INSERT INTO maschinenkosten_arbeitszeit (
        external_id, equipment_id, datum, mitarbeiter, taetigkeit, notiz,
        zaehlerstand, menge, stueckpreis, kosten, created_at, updated_at
    )
    SELECT 
        ma.external_id || '_' || dr.target_date::text,
        ma.equipment_id, dr.target_date, ma.mitarbeiter, ma.taetigkeit, ma.notiz,
        ma.zaehlerstand, ma.menge, ma.stueckpreis, ma.kosten,
        dr.target_date::timestamp,
        dr.target_date::timestamp
    FROM maschinenkosten_arbeitszeit ma
    CROSS JOIN date_range dr
    WHERE ma.datum = (SELECT MAX(datum) FROM maschinenkosten_arbeitszeit)
    AND NOT EXISTS (
        SELECT 1 FROM maschinenkosten_arbeitszeit ma2 
        WHERE ma2.external_id = ma.external_id || '_' || dr.target_date::text
    );
    
    GET DIAGNOSTICS arbeitszeit_count = ROW_COUNT;
    RAISE NOTICE '  ✓ maschinenkosten_arbeitszeit: % records', arbeitszeit_count;
    
    -- MASCHINENKOSTEN_RECHNUNGEN
    INSERT INTO maschinenkosten_rechnungen (
        external_id, equipment_id, datum, mitarbeiter_lieferant, rechnung, lieferschein,
        artikel_taetigkeit, notiz, zaehlerstand, menge, stueckpreis, kosten,
        created_at, updated_at
    )
    SELECT 
        mr.external_id || '_' || dr.target_date::text,
        mr.equipment_id, dr.target_date, mr.mitarbeiter_lieferant, mr.rechnung, mr.lieferschein,
        mr.artikel_taetigkeit, mr.notiz, mr.zaehlerstand, mr.menge, mr.stueckpreis, mr.kosten,
        dr.target_date::timestamp,
        dr.target_date::timestamp
    FROM maschinenkosten_rechnungen mr
    CROSS JOIN date_range dr
    WHERE mr.datum = (SELECT MAX(datum) FROM maschinenkosten_rechnungen)
    AND NOT EXISTS (
        SELECT 1 FROM maschinenkosten_rechnungen mr2 
        WHERE mr2.external_id = mr.external_id || '_' || dr.target_date::text
    );
    
    GET DIAGNOSTICS rechnungen_count = ROW_COUNT;
    RAISE NOTICE '  ✓ maschinenkosten_rechnungen: % records', rechnungen_count;
    
    DROP TABLE date_range;
    
    RAISE NOTICE '';
    RAISE NOTICE '✅ Replication complete!';
    RAISE NOTICE 'Total records created: %', 
        tankbuch_count + betriebsstoffe_count + arbeitszeit_count + rechnungen_count;
END $$;

COMMIT;

-- Verification
\echo ''
\echo '📊 Final counts:'
SELECT 'equipment_groups' as table_name, COUNT(*) FROM equipment_groups
UNION ALL SELECT 'equipment', COUNT(*) FROM equipment
UNION ALL SELECT 'tankbuch', COUNT(*) FROM tankbuch
UNION ALL SELECT 'betriebsstoffe', COUNT(*) FROM betriebsstoffe
UNION ALL SELECT 'maschinenkosten_arbeitszeit', COUNT(*) FROM maschinenkosten_arbeitszeit
UNION ALL SELECT 'maschinenkosten_rechnungen', COUNT(*) FROM maschinenkosten_rechnungen;

\echo ''
\echo '📅 Date distribution (tankbuch):'
SELECT date::date, COUNT(*) as records FROM tankbuch GROUP BY date::date ORDER BY date DESC LIMIT 10;
