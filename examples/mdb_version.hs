
import Database.LMDB
import Text.Show.Pretty

main :: IO ()
main = do
  putStr "Static "
  pPrint lmdb_version
  putStrLn ""
  putStr "Dynamic "
  lmdb_dyn_version >>= pPrint
