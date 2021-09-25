SELECT T.name
FROM Trainer AS T
WHERE T.hometown = 'Brown City' OR T.hometown = 'Rainbow city'
ORDER BY T.name
;