SELECT Count(*)
FROM (SELECT DISTINCT User.UserID
FROM User, Bid, Item
WHERE User.UserID = Bid.UserID
AND User.UserID = Item.SellerID);