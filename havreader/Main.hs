{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Aeson
import Data.Binary.Get
import Codec.Compression.Zlib.Raw
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import qualified Data.Map as Map
import qualified Data.Vector as V

import Data.Avro.Container
import Data.Avro.Generic
import Data.Avro.Schema
import Data.Avro.Class

getCodec :: BS.ByteString -> Maybe (LB.ByteString -> LB.ByteString)
getCodec "deflate" = Just decompress
getCodec _ = Nothing

main :: IO ()
main = do
  l <- LB.readFile "/home/data/capture/suntime/AUTHOR_CORE.avc"
  let Right (bs, _ , FileHeader m sync) = runGetOrFail (fromBinary (avroSchema (undefined :: FileHeader))) l
      Right s = maybe (Left "missing") Right (Map.lookup "avro.schema" m) >>= eitherDecode . LB.fromStrict
      Block n b sync1 = runGet (fromBinary (avroSchema (undefined :: Block))) bs
      t :: V.Vector AvroType
      t = runGet (V.replicateM n (fromBinary s)) (decompress b)
  print s
  V.mapM_ print t
  -- let v = runGet ((,) <$> getBinary fileHeaderSchema <*> getDataBlock decompress (PrimitiveSchema IntSchema)) l
