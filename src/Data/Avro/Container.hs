{-# LANGUAGE OverloadedStrings #-}

module Data.Avro.Container (
    Magic(..)
  , Sync(..)
  , FileHeader(..)
  , dataSchema
  , Block(..)
  , Container(..)
  , container
  , parseBlocks
  , parseContainer
  , readAvroContainer
  ) where

import Control.Monad ((<=<))
import Control.Applicative

import Data.Aeson as A
import Data.Binary
import Data.Binary.Get
import qualified Data.Map as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T

import Data.Avro.Schema
import Data.Avro.Generic
import Data.Avro.Class

newtype Magic = Magic { magicBytes :: BS.ByteString }
  deriving (Eq, Ord, Show, Read)

instance ToAvro Magic where
  avroSchema _ = plainSchema . ComplexSchema $ fixedSchema "Magic" Nothing [] 4
  toAvro = toAvro . AvroNamed "Magic" Nothing . AvroFixed . magicBytes

instance FromAvro Magic where
  fromAvro = fmap Magic . fromAvro

newtype Sync = Sync { syncBytes :: BS.ByteString }
  deriving (Eq, Ord, Show, Read)

instance ToAvro Sync where
  avroSchema _ = plainSchema . ComplexSchema $ fixedSchema "Sync" Nothing [] 16
  toAvro = toAvro . AvroNamed "Sync" Nothing . AvroFixed . syncBytes

instance FromAvro Sync where
  fromAvro = fmap Sync . fromAvro

data FileHeader = FileHeader {
    headerMeta :: Map.Map T.Text BS.ByteString
  , headerSync :: Sync
  } deriving (Eq, Show, Read)

headerMagic :: Magic
headerMagic = Magic "Obj\x1"

instance ToAvro FileHeader where
  avroSchema _ = plainSchema . ComplexSchema $ recordSchema "Header" (Just "org.apache.avro.file") [] [
      recordField "magic" (avroSchema (undefined :: Magic))
    , recordField "meta" (plainSchema . ComplexSchema . MapSchema . plainSchema $ PrimitiveSchema BytesSchema)
    , recordField "sync" (avroSchema (undefined :: Sync))
    ]
  toAvro (FileHeader m s) = record  "Header" (Just "org.apache.avro.file") [
      ("magic", toAvro headerMagic)
    , ("meta", toAvro m)
    , ("sync", toAvro s)
    ]

instance FromAvro FileHeader where
  fromAvro = withRecord $ \r -> do
    m <- flookup "magic" r
    if m /= headerMagic
      then fail "bad magic"
      else FileHeader <$> flookup "meta" r <*> flookup "sync" r

dataSchema :: FileHeader -> Maybe Schema
dataSchema = A.decode . LB.fromStrict <=< Map.lookup "avro.schema" . headerMeta

data Block = Block {
    blockCount :: !Int
  , blockData :: LB.ByteString
  , blockSync :: !Sync
  } deriving (Eq, Show, Read)

instance ToAvro Block where
  avroSchema _ = plainSchema . ComplexSchema $ recordSchema "Block" (Just "org.apache.avro.file") [] [
      recordField "count" (plainSchema $ PrimitiveSchema LongSchema)
    , recordField "data" (plainSchema $ PrimitiveSchema BytesSchema)
    , recordField "sync" (avroSchema (undefined :: Sync))
    ]
  toAvro (Block n bs sync) = record "Block" (Just "org.apache.avro.file") [
      ("count", toAvro n)
    , ("data", toAvro bs)
    , ("sync", toAvro sync)
    ]

instance FromAvro Block where
  fromAvro = withRecord $ \r ->
    Block <$> flookup "count" r <*> flookup "data" r <*> flookup "sync" r

data Container = Container {
    containerHeader :: FileHeader
  , containerBlocks :: [Block]
  } deriving (Eq, Read, Show)

container :: Get Container
container = Container <$>
      fromBinary (avroSchema (undefined :: FileHeader))
  <*> some (fromBinary (avroSchema (undefined :: Block)))

parseBlocks :: LB.ByteString -> [Block]
parseBlocks lb
  | LB.null lb = []
  | otherwise =
    case runGetOrFail (fromBinary (avroSchema (undefined :: Block))) lb of
      Left (_, _, e) -> error e
      Right (r, _, v) -> v:parseBlocks r

parseContainer :: LB.ByteString -> Container
parseContainer lb =
  case runGetOrFail (fromBinary (avroSchema (undefined :: FileHeader))) lb of
    Left (_, _, e) -> error e
    Right (r, _, h) -> Container h (parseBlocks r)

readAvroContainer :: FilePath -> IO Container
readAvroContainer = fmap parseContainer . LB.readFile
