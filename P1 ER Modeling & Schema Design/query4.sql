SELECT ItemID
FROM Item
WHERE Item.Currently = (SELECT Item.Currently FROM Item ORDER BY Item.Currently DESC LIMIT 1);