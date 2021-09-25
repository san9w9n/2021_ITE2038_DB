SELECT P.name
FROM Pokemon AS P, Evolution AS E
WHERE 
	(P.id = E.after_id AND
     P.id IN (
       SELECT PP.id
       FROM Pokemon AS PP, Evolution AS EE
       WHERE PP.id = EE.before_id
       )
     ) OR (
     P.id = E.after_id AND
     E.before_id NOT IN (
       SELECT PP.id
       FROM Pokemon AS PP, Evolution AS EE
       WHERE PP.id = EE.after_id
       ))
ORDER BY P.name
;