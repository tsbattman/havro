{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}

module Data.Avro.Container (
    Magic(..)
  , Sync(..)
  , FileHeader(..)
  , Block(..)
  ) where

import Data.Binary
import qualified Data.Map as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T

import Data.Avro.Schema
import Data.Avro.Generic
import Data.Avro.Encoding
import Data.Avro.Class

newtype Magic = Magic { magicBytes :: BS.ByteString }
  deriving (Eq, Ord, Show, Read)

magic :: Magic
magic = Magic "Obj\x1"

instance ToAvro Magic where
  avroSchema _ = plainSchema . ComplexSchema $ fixedSchema "Magic" Nothing [] 4
  toAvro = toAvro . AvroFixed . magicBytes

instance FromAvro Magic where
  fromAvro s = fmap Magic . fromAvro s

newtype Sync = Sync { syncBytes :: BS.ByteString }
  deriving (Eq, Ord, Show, Read)

instance ToAvro Sync where
  avroSchema _ = plainSchema . ComplexSchema $ fixedSchema "Sync" Nothing [] 16
  toAvro = toAvro . AvroFixed . syncBytes

instance FromAvro Sync where
  fromAvro s = fmap Sync . fromAvro s

data FileHeader = FileHeader (Map.Map T.Text BS.ByteString) Sync
  deriving (Eq, Show, Read)

instance ToAvro FileHeader where
  avroSchema _ = plainSchema . ComplexSchema $ recordSchema "Header" (Just "org.apache.avro.file") [] [
      recordField "magic" (toTypeSchema $ avroSchema (undefined :: Magic))
    , recordField "meta" (ComplexSchema $ MapSchema (PrimitiveSchema BytesSchema))
    , recordField "sync" (toTypeSchema $ avroSchema (undefined :: Sync))
    ]
  toAvro (FileHeader m s) = toAvro . AvroRecord $ [toAvro magic, toAvro m, toAvro s]

instance FromAvro FileHeader where
  fromAvro s@(toTypeSchema -> ComplexSchema (NamedSchema _ _ _ (RecordSchema rs))) v = do
    AvroRecord r <- fromAvro s v
    m <- fromAvro (plainSchema . fieldType $ head rs) (head r)
    if m /= magic
      then fail "bad magic"
      else FileHeader <$>
            fromAvro (plainSchema . fieldType $ rs !! 1) (r !! 1)
        <*> fromAvro (plainSchema . fieldType $ rs !! 2) (r !! 2)

data Block = Block Int LB.ByteString Sync
  deriving (Eq, Show, Read)

instance ToAvro Block where
  avroSchema _ = plainSchema . ComplexSchema $ recordSchema "Block" (Just "org.apache.avro.file") [] [
      recordField "count" (PrimitiveSchema LongSchema)
    , recordField "data" (PrimitiveSchema BytesSchema)
    , recordField "sync" (toTypeSchema $ avroSchema (undefined :: Sync))
    ]
  toAvro (Block n bs sync) = toAvro . AvroRecord $ [toAvro n, toAvro bs, toAvro sync]

instance FromAvro Block where
  fromAvro s@(toTypeSchema -> ComplexSchema (NamedSchema _ _ _ (RecordSchema rs))) v = do
    AvroRecord r <- fromAvro s v
    Block <$>
          fromAvro (plainSchema . fieldType $ rs !! 0) (r !! 0)
      <*> fromAvro (plainSchema . fieldType $ rs !! 1) (r !! 1)
      <*> fromAvro (plainSchema . fieldType $ rs !! 2) (r !! 2)
