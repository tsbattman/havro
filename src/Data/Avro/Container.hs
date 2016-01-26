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
  toAvro = toAvro . AvroNamed "Magic" Nothing . AvroFixed . magicBytes

instance FromAvro Magic where
  fromAvro s = fmap Magic . fromAvro s

newtype Sync = Sync { syncBytes :: BS.ByteString }
  deriving (Eq, Ord, Show, Read)

instance ToAvro Sync where
  avroSchema _ = plainSchema . ComplexSchema $ fixedSchema "Sync" Nothing [] 16
  toAvro = toAvro . AvroNamed "Sync" Nothing . AvroFixed . syncBytes

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
  toAvro (FileHeader m s) = toAvro . AvroNamed "Header" (Just "org.apache.avro.file") . AvroRecord $ [
      ("magic", toAvro magic)
    , ("meta", toAvro m)
    , ("sync", toAvro s)
    ]

instance FromAvro FileHeader where
  fromAvro s@(toTypeSchema -> ComplexSchema (NamedSchema _ _ _ (RecordSchema rs))) v = do
    AvroNamed _ _ (AvroRecord r) <- fromAvro s v
    m <- fromAvro (plainSchema . fieldType $ head rs) (snd . head $ r)
    if m /= magic
      then fail "bad magic"
      else FileHeader <$>
            fromAvro (plainSchema . fieldType $ rs !! 1) (snd $ r !! 1)
        <*> fromAvro (plainSchema . fieldType $ rs !! 2) (snd $ r !! 2)

data Block = Block Int LB.ByteString Sync
  deriving (Eq, Show, Read)

instance ToAvro Block where
  avroSchema _ = plainSchema . ComplexSchema $ recordSchema "Block" (Just "org.apache.avro.file") [] [
      recordField "count" (PrimitiveSchema LongSchema)
    , recordField "data" (PrimitiveSchema BytesSchema)
    , recordField "sync" (toTypeSchema $ avroSchema (undefined :: Sync))
    ]
  toAvro (Block n bs sync) = toAvro . AvroNamed "Block" (Just "org.apache.avro.file") . AvroRecord $ [
      ("count", toAvro n)
    , ("data", toAvro bs)
    , ("sync", toAvro sync)
    ]

instance FromAvro Block where
  fromAvro s@(toTypeSchema -> ComplexSchema (NamedSchema _ _ _ (RecordSchema rs))) v = do
    AvroNamed _ _ (AvroRecord r) <- fromAvro s v
    Block <$>
          fromAvro (plainSchema . fieldType $ rs !! 0) (snd $ r !! 0)
      <*> fromAvro (plainSchema . fieldType $ rs !! 1) (snd $ r !! 1)
      <*> fromAvro (plainSchema . fieldType $ rs !! 2) (snd $ r !! 2)
