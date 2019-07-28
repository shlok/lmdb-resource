--------------------------------------------------------------------------------

{-# LANGUAGE TypeApplications #-}

--------------------------------------------------------------------------------

module Database.LMDB.Resource.Tests (tests) where

--------------------------------------------------------------------------------

import Control.Concurrent.Async (asyncBound, wait)
import Control.Monad (forM_)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Monad.Writer.Strict (execWriterT, tell)
import Data.ByteString (ByteString, pack)
import Data.List (foldl', sort)
import Database.LMDB.Raw (MDB_dbi', MDB_env, mdb_clear', mdb_put', mdb_txn_begin, mdb_txn_commit)
import Database.LMDB.Resource (readLMDB, writeLMDB)
import Database.LMDB.Resource.Internal (marshalOut, noWriteFlags)
import Test.QuickCheck.Monadic (PropertyM, monadicIO, pick, run)
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck (arbitrary, testProperty)

--------------------------------------------------------------------------------

tests :: IO (MDB_env, MDB_dbi') -> [TestTree]
tests res = [ testReadLMDB res, testWriteLMDB res ]

--------------------------------------------------------------------------------

-- | Clear the database, write key-value pairs to it in a normal manner, read
-- them back using our library, and make sure the result is what we wrote.
testReadLMDB :: IO (MDB_env, MDB_dbi') -> TestTree
testReadLMDB res = testProperty "readLMDB" . monadicIO $ do
    (env, dbi) <- run res
    keyValuePairs <- arbitraryKeyValuePairs
    run $ (asyncBound $ do
        txn <- mdb_txn_begin env Nothing False
        mdb_clear' txn dbi
        forM_ keyValuePairs $ \(k, v) -> marshalOut k $ \k' ->
                                         marshalOut v $ \v' -> mdb_put' noWriteFlags txn dbi k' v' >> return ()
        mdb_txn_commit txn) >>= wait
    readPairs <- run . runResourceT . execWriterT $ readLMDB env dbi (\kvp -> tell [kvp] >> return ())
    return $ readPairs == (sort . removeDuplicateKeys $ keyValuePairs)

-- | Clear the database, write key-value pairs to it using our library,
-- read them back using our library (already covered by 'testReadLMDB'),
-- and make sure the result is what we wrote.
testWriteLMDB :: IO (MDB_env, MDB_dbi') -> TestTree
testWriteLMDB res = testProperty "writeLMDB" . monadicIO $ do
    (env, dbi) <- run res
    run $ (asyncBound $ do
        txn <- mdb_txn_begin env Nothing False
        mdb_clear' txn dbi
        mdb_txn_commit txn) >>= wait
    keyValuePairs <- arbitraryKeyValuePairs
    retValue <- pick (arbitrary @String)
    r <- run . runResourceT $ writeLMDB env dbi (\write -> forM_ keyValuePairs write >> return retValue)
    readPairs <- run . runResourceT . execWriterT $ readLMDB env dbi (\kvp -> tell [kvp] >> return ())
    return $ r == retValue && readPairs == (sort . removeDuplicateKeys $ keyValuePairs)

arbitraryKeyValuePairs :: PropertyM IO [(ByteString, ByteString)]
arbitraryKeyValuePairs =
    map (\(ws1, ws2) -> (pack ws1, pack ws2))
    . filter (\(ws1, _) -> length ws1 > 0) -- LMDB does not allow empty keys.
   <$> pick arbitrary

-- | Note that this function retains the last value for each key.
removeDuplicateKeys :: (Eq a) => [(a, b)] -> [(a, b)]
removeDuplicateKeys = foldl' (\acc (a, b) -> if any ((== a) . fst) acc then acc else (a, b) : acc) [] . reverse

--------------------------------------------------------------------------------
