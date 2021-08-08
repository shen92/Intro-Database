SELECT COUNT(A.Name)
FROM(SELECT BelongTo.Name
FROM BelongTo, Item, Bid
WHERE BelongTo.ItemID = Item.ItemID AND Item.ItemID = Bid.ItemID AND Bid.Amount > 100
GROUP BY BelongTo.Name
HAVING Count(Item.ItemID) >= 1) AS A;