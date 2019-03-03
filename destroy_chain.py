import leveldb
a = leveldb.LevelDB('./data/node1/chain.db')
for i,j in a.RangeIter():
    a.Delete(i)

a = leveldb.LevelDB('./data/node1/book.db')
for i,j in a.RangeIter():
    a.Delete(i)

a = leveldb.LevelDB('./data/node1/transaction_pool.db')
for i,j in a.RangeIter():
    a.Delete(i)


a = leveldb.LevelDB('./data/node2/chain_test.db')
for i,j in a.RangeIter():
    a.Delete(i)

a = leveldb.LevelDB('./data/node2/book_test.db')
for i,j in a.RangeIter():
    a.Delete(i)

a = leveldb.LevelDB('./data/node2/transaction_pool_test.db')
for i,j in a.RangeIter():
    a.Delete(i)

