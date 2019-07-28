--------------------------------------------------------------------------------

module Main where

--------------------------------------------------------------------------------

import Database.LMDB.Raw (MDB_dbi', MDB_env, mdb_dbi_open', mdb_env_create, mdb_env_open,
                          mdb_env_set_mapsize, mdb_txn_begin, mdb_txn_commit)
import qualified Database.LMDB.Resource.Tests (tests)
import System.Directory (removeDirectoryRecursive)
import System.Environment (setEnv)
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)
import Test.Tasty (TestTree, defaultMain, testGroup, withResource)

--------------------------------------------------------------------------------

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" -- Multiple tests use the same LMDB database.
    defaultMain $ withResource
        (do tmpParent <- getCanonicalTemporaryDirectory
            tmpDir <- createTempDirectory tmpParent "lmdb-resource-tests"
            env <- mdb_env_create
            mdb_env_set_mapsize env (1024 * 1024 * 1024 * 1024)
            mdb_env_open env tmpDir []
            txn <- mdb_txn_begin env Nothing True
            dbi <- mdb_dbi_open' txn Nothing []
            mdb_txn_commit txn
            return (tmpDir, env, dbi))
        (\(tmpDir,_,_) -> removeDirectoryRecursive tmpDir) (\io -> tests $ (\(_,y,z) -> (y,z)) <$> io)

tests :: IO (MDB_env, MDB_dbi') -> TestTree
tests res =
    testGroup "Tests"
        [ testGroup "Database.LMDB.Resource.Tests" $
                     Database.LMDB.Resource.Tests.tests res ]

--------------------------------------------------------------------------------
