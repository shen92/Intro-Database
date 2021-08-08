SELECT Count(*)
FROM (SELECT BelongTo.ItemID
FROM BelongTo
GROUP BY BelongTo.ItemID
HAVING Count(BelongTo.Name) = 4);