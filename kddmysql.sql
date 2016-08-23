USE kddcupdb;

CREATE OR REPLACE VIEW v_exciting_projects AS 
	SELECT projectid, school_state, poverty_level, underprivileged_students_reached
		FROM predictions
		WHERE is_exciting = 1;

SELECT
    projectid,
    school_state,
    poverty_level,
	project_score,
	rank
FROM
	(SELECT
		projectid,
		school_state,
		poverty_level,
		project_score,
		@CURRANK := IF(@PREVSTATE = school_state, @CURRANK  + 1, 1) rank,
		@PREVSTATE := school_state
	FROM 
		(SELECT 
			projectid, 
			x.school_state, 
			poverty_level, 
			underprivileged_students_reached, 
			underprivileged_students_reached/upriv_students_reached_statewide AS project_score
		FROM v_exciting_projects AS x
		JOIN (SELECT school_state, SUM(underprivileged_students_reached) AS upriv_students_reached_statewide 
				FROM v_exciting_projects 
				GROUP BY school_state) AS y
		ON x.school_state = y.school_state) AS t1,
		(SELECT @PREVSTATE := NULL, @CURRANK := 0) AS vars
	ORDER BY school_state, project_score DESC
	) AS t2
WHERE rank <= 5;
