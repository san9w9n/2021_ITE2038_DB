SELECT P.name
FROM Pokemon AS P, Evolution AS E
WHERE P.id = E.after_id
	AND E.before_id IN (
      SELECT EE.after_id
      FROM Pokemon AS PP, Evolution AS EE
      WHERE PP.id = EE.before_id AND PP.name = 'Charmander'
      )
;