rm -rf *.dat
python parser.py ebay_data/items-*.json
sort -o Items_buffer.dat Items_buffer.dat
sort -o User_buffer.dat User_buffer.dat
sort -o Category_buffer.dat Category_buffer.dat
sort -o Bid_buffer.dat Bid_buffer.dat
sort -o BelongTo_buffer.dat BelongTo_buffer.dat
uniq Items_buffer.dat Items.dat
uniq User_buffer.dat User.dat
uniq Category_buffer.dat Category.dat
uniq Bid_buffer.dat Bid.dat
uniq BelongTo_buffer.dat BelongTo.dat
rm -rf *_buffer.dat

