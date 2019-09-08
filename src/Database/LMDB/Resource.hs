--------------------------------------------------------------------------------

module Database.LMDB.Resource (readLMDB, writeLMDB) where

--------------------------------------------------------------------------------

import Control.Concurrent.Async (asyncBound)
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent.MVar (MVar, isEmptyMVar, newEmptyMVar, putMVar, takeMVar)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans (MonadTrans, lift)
import Control.Monad.Trans.Resource (MonadResource, allocate, register, release, unprotect)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B (take)
import Database.LMDB.Raw (MDB_cursor', MDB_cursor_op (MDB_FIRST, MDB_NEXT), MDB_dbi',
                          MDB_env, MDB_val, mdb_cursor_close', mdb_cursor_get', mdb_cursor_open',
                          mdb_put', mdb_txn_abort, mdb_txn_begin, mdb_txn_commit)
import Database.LMDB.Resource.Utility (emptyWriteFlags, marshalIn, marshalOut, noOverwriteWriteFlag)
import Foreign (Ptr, free, malloc, peek)
import UnliftIO.Exception (throwIO, throwString, tryAny)

--------------------------------------------------------------------------------

-- | Creates a read transaction, within which we can read all key-value pairs from the database.
readLMDB :: (MonadResource m, MonadTrans t, MonadIO (t m))
         => MDB_env -> MDB_dbi' -> ((ByteString, ByteString) -> t m ()) -> t m ()
readLMDB env dbi yield = do
    (txnKey, txn) <- lift $ allocate (mdb_txn_begin env Nothing True) mdb_txn_abort
    (cursKey, curs) <- lift $ allocate (mdb_cursor_open' txn dbi) mdb_cursor_close'
    (ptrKey, (kp, vp)) <- lift $ allocate ((,) <$> malloc <*> malloc) (\(kp, vp) -> free kp >> free vp)
    yieldAll curs kp vp True yield
    _ <- release ptrKey >> release cursKey >> unprotect txnKey
    liftIO $ mdb_txn_commit txn

yieldAll :: (Monad m, MonadTrans t, MonadIO (t m)) => MDB_cursor' -> Ptr MDB_val -> Ptr MDB_val
         -> Bool -> ((ByteString, ByteString) -> t m ()) -> t m ()
yieldAll curs kp vp first yield = do
    found <- liftIO $ mdb_cursor_get' (if first then MDB_FIRST else MDB_NEXT) curs kp vp
    when found $ do
        k <- liftIO (peek kp >>= marshalIn)
        v <- liftIO (peek vp >>= marshalIn)
        yield (k, v)
        yieldAll curs kp vp False yield

--------------------------------------------------------------------------------

-- | Creates a write transaction, within which we can write key-value pairs to the database.
writeLMDB :: (MonadResource m)
          => MDB_env
          -> MDB_dbi'
          -> Bool     -- ^ If 'True', an exception will be thrown when attempting to re-insert a key.
          -> (((ByteString, ByteString) -> m ()) -> m r)
          -> m r
writeLMDB env dbi noOverwrite write = do
    channel <- liftIO createChannel
    _ <- liftIO . asyncBound $ startChannel channel
    finishKey <- register $ endChannel channel
    (txnKey, txn) <- allocate (runOnChannel channel $ mdb_txn_begin env Nothing False)
                              (\txn -> runOnChannel channel $ mdb_txn_abort txn)
    r <- write $ \(k, v) -> liftIO . runOnChannel channel . marshalOut k $ \k' -> marshalOut v $ \v' -> do
        res <- mdb_put' (if noOverwrite then noOverwriteWriteFlag else emptyWriteFlags) txn dbi k' v'
        if not res && noOverwrite
            then throwString $ "LMDB key already exists: " ++ show (B.take 100 k)
            else return ()
    _ <- unprotect txnKey
    liftIO . runOnChannel channel $ mdb_txn_commit txn
    release finishKey
    return r

--------------------------------------------------------------------------------

-- LMDB requires write transactions to happen on a bound thread.
-- The following machinery helps us with that.

data Channel =
     Channel { chan       :: !(Chan (IO ()))
             , finishMVar :: !(MVar ()) }

createChannel :: IO Channel
createChannel = Channel <$> newChan <*> newEmptyMVar

-- | We will call 'startChannel' with 'asyncBound', which will cause
-- IO actions that we 'runOnChannel' to run on a bound thread.
startChannel :: Channel -> IO ()
startChannel channel@(Channel { chan = chan', finishMVar = finishMVar' }) = do
    io <- readChan chan'
    io
    isEmptyMVar finishMVar' >>= (flip when) (startChannel channel)

runOnChannel :: Channel -> IO a -> IO a
runOnChannel (Channel { chan = chan' }) io = do
    mVar <- newEmptyMVar
    writeChan chan' $
        -- Catch synchronous exception and keep the channel running.
        tryAny io >>= either (putMVar mVar . Left) (putMVar mVar . Right)
    -- Rethrow synchronous exception.
    takeMVar mVar >>= either throwIO return

endChannel :: Channel -> IO ()
endChannel (Channel { chan = chan', finishMVar = finishMVar' }) = do
    putMVar finishMVar' ()
    writeChan chan' (return ())

--------------------------------------------------------------------------------
