SELECT P.name
FROM Pokemon AS P, Evolution AS E
WHERE P.id = E.before_id AND E.after_id < E.before_id
ORDER BY P.name
;