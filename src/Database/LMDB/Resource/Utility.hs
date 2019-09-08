--------------------------------------------------------------------------------

module Database.LMDB.Resource.Utility
    ( marshalIn
    , marshalOut
    , emptyWriteFlags
    , noOverwriteWriteFlag) where

--------------------------------------------------------------------------------

import Data.ByteString (ByteString, packCStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Database.LMDB.Raw (MDB_val (MDB_val), MDB_WriteFlag (MDB_NOOVERWRITE), MDB_WriteFlags, compileWriteFlags)
import Foreign (castPtr)

--------------------------------------------------------------------------------

marshalIn :: MDB_val -> IO ByteString
marshalIn (MDB_val len ptr) = packCStringLen (castPtr ptr, fromIntegral len)

marshalOut :: ByteString -> (MDB_val -> IO ()) -> IO ()
marshalOut bs f = unsafeUseAsCStringLen bs $ \(ptr, len) -> f $ MDB_val (fromIntegral len) (castPtr ptr)

emptyWriteFlags :: MDB_WriteFlags
emptyWriteFlags = compileWriteFlags []

noOverwriteWriteFlag :: MDB_WriteFlags
noOverwriteWriteFlag = compileWriteFlags [MDB_NOOVERWRITE]

--------------------------------------------------------------------------------
