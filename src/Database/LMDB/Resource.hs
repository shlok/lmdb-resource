--------------------------------------------------------------------------------

module Database.LMDB.Resource (marshalOut, readLMDB, readAllLMDB, writeLMDB) where

--------------------------------------------------------------------------------

import Control.Concurrent (isCurrentThreadBound)
import Control.Monad (unless, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans (MonadTrans, lift)
import Control.Monad.Trans.Resource (MonadResource, allocate, release, unprotect)
import Data.ByteString (ByteString, packCStringLen)
import qualified Data.ByteString as B (take)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Database.LMDB.Raw (MDB_cursor', MDB_cursor_op (MDB_FIRST, MDB_NEXT), MDB_dbi',
                          MDB_env, MDB_val (MDB_val), MDB_WriteFlags, mdb_cursor_close', mdb_cursor_get',
                          mdb_cursor_open', mdb_get', mdb_put', mdb_txn_abort, mdb_txn_begin, mdb_txn_commit)
import Foreign (Ptr, castPtr, free, malloc, peek)
import UnliftIO.Exception (throwString)

--------------------------------------------------------------------------------

--  Write streaming version.
-- readLMDB' env dbi s =
--     readLMDB env dbi $ \mp -> S.mapM (liftIO . mp) s

-- -- Write conduit version.
-- readLMDB'' env dbi =
--     readLMDB env dbi $ \mp -> awaitForever $ \k -> do
--         v <- liftIO $ mp k
--         C.yield v

-- | Creates a read transaction, within which we can query for keys in the database.
readLMDB :: (MonadResource m, MonadTrans t, MonadIO (t m))
         => MDB_env -> MDB_dbi' -> ((ByteString -> IO (Maybe ByteString)) -> t m r) -> t m r
readLMDB env dbi mp = do
    (txnKey, txn) <- lift $ allocate (mdb_txn_begin env Nothing True) mdb_txn_abort
    r <- mp $ \k -> marshalOut k $ \kv -> do
        mvv <- mdb_get' txn dbi kv
        case mvv of
            Nothing -> return Nothing
            Just vv -> Just <$> marshalIn vv
    _ <- unprotect txnKey
    liftIO $ mdb_txn_commit txn
    return r

--------------------------------------------------------------------------------

-- | Creates a read transaction, within which we can read all key-value pairs from the database.
readAllLMDB :: (MonadResource m, MonadTrans t, MonadIO (t m))
            => MDB_env -> MDB_dbi' -> ((ByteString, ByteString) -> t m ()) -> t m ()
readAllLMDB env dbi yield = do
    (txnKey, txn) <- lift $ allocate (mdb_txn_begin env Nothing True) mdb_txn_abort
    (cursKey, curs) <- lift $ allocate (mdb_cursor_open' txn dbi) mdb_cursor_close'
    (ptrKey, (kp, vp)) <- lift $ allocate ((,) <$> malloc <*> malloc) (\(kp, vp) -> free kp >> free vp)
    yieldAll curs kp vp True yield
    _ <- release ptrKey >> release cursKey >> unprotect txnKey
    liftIO $ mdb_txn_commit txn

yieldAll :: (MonadIO m)
         => MDB_cursor' -> Ptr MDB_val -> Ptr MDB_val -> Bool -> ((ByteString, ByteString) -> m ()) -> m ()
yieldAll curs kp vp first yield = do
    found <- liftIO $ mdb_cursor_get' (if first then MDB_FIRST else MDB_NEXT) curs kp vp
    when found $ do
        k <- liftIO (peek kp >>= marshalIn)
        v <- liftIO (peek vp >>= marshalIn)
        yield (k, v)
        yieldAll curs kp vp False yield

--------------------------------------------------------------------------------

-- | Creates a write transaction, within which we can write key-value pairs to the database.
-- Important: The final IO action (the one obtained with 'runResourceT') must be run on a bound thread.
writeLMDB :: (MonadIO n, MonadResource m)
          => MDB_env -> MDB_dbi' -> MDB_WriteFlags -> (((ByteString, ByteString) -> n ()) -> m r) -> m r
writeLMDB env dbi wf write = do
    isBound <- liftIO isCurrentThreadBound
    unless isBound (throwString "bound thread expected") -- todo: remove? already checked by lmdb library?
    (txnKey, txn) <- allocate (mdb_txn_begin env Nothing False) mdb_txn_abort
    r <- write $ \(k, v) -> liftIO . marshalOut k $ \k' -> marshalOut v $ \v' -> do
        res <- mdb_put' wf txn dbi k' v'
        unless res $ throwString $ "MDB_KEYEXIST: " ++ show (B.take 50 k)
    _ <- unprotect txnKey
    liftIO $ mdb_txn_commit txn
    return r

--------------------------------------------------------------------------------

marshalIn :: MDB_val -> IO ByteString
marshalIn (MDB_val len ptr) = packCStringLen (castPtr ptr, fromIntegral len)

marshalOut :: ByteString -> (MDB_val -> IO a) -> IO a
marshalOut bs f = unsafeUseAsCStringLen bs $ \(ptr, len) -> f $ MDB_val (fromIntegral len) (castPtr ptr)

--------------------------------------------------------------------------------
