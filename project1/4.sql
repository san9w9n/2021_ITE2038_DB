SELECT T.name
FROM Trainer T, City C
WHERE T.hometown LIKE 'Blue City' AND T.hometown = C.name
ORDER BY T.name
;