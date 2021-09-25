SELECT P.name
FROM Pokemon AS P
WHERE P.type LIKE 'Water'
	AND P.id NOT IN (
      SELECT PP.id
      FROM Pokemon AS PP, Evolution AS EE
      WHERE PP.id = EE.before_id )
ORDER BY P.name
;